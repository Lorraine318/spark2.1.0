/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rpc.netty

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 *
 * @param numUsableCores Number of CPU cores allocated to the process, for sizing the thread pool.
 *                       If 0, will consider the available CPUs on the host.
  *
  *                       执行流程:
  *                       1.调用Inbox的post方法将消息放入messages列表中
  *                       2.将有消息的Inbo相关联的EndpointData放入receivers
  *                       3.messageLoop每次循环首先从receivers中获取EndpointData
  *                       4.执行EndpointData中的Inbox的process方法对消息进行具体处理
  *
 */
//可以将RPC消息路由到要对消息处理的RpcEndpoint端点上
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {

  //inbox与 RpcEndpoint,NettyRpcEndpointRef通过endpointData进行关联  Rpc端点数据，他包括了RpcEndpoint,nettyRpcEndpointRef及Inbox等属于同一个端点的实例，
  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint, //RPC端点
      val ref: NettyRpcEndpointRef) {  //RPC端点引用  “spark://host:port/name”所谓引用也就是这种格式的地址  host为端点所在RPC服务的主机IP ， port是端点所在RPC服务的端口
    val inbox = new Inbox(ref, endpoint)  //消息，所有类型的RPC消息都继承自InboxMessage
  }
  //端点实例名称与端点数据EndpointData之间映射关系的缓存，可以使用端点名称从中快速获取删除EndpointData
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  //端点实例RpcEndpoint与端点实例引用RpcEndpointRef之间映射关系的缓存。，可以从中根据端点实例快速获取删除端点实例引用了
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  // 存储端点数据EndpointData的阻塞队列  (只有Inbox的messages列表种有消息，才会将EndpointData添加到该队列中)
  private val receivers = new LinkedBlockingQueue[EndpointData]
  //Dispatcher是否停止的状态
  @GuardedBy("this")
  private var stopped = false

  //注册RpcEndpoint  而且还会将endpointData加入receivers
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    //使用当前RpcEndpoint所在的NettyRpcEnv的地址和RpcEndpoint的名称创建RpcEndpointAddress
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    //创建rpcendpoint的引用对象
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      val data = endpoints.get(name)
      //将RpcEndpoint与nettyRpcEndpointRef的映射关系放入endpointRefs缓存
      endpointRefs.put(data.endpoint, data.ref)
      //往队列尾部中添加endpointData
      receivers.offer(data)  // for the OnStart message
    }
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    //从endpoints中移除EndpointData
    val data = endpoints.remove(name)
    if (data != null) {
      //调用EndpointData中Inbox的stop方法停止Inbox
      data.inbox.stop()
      //将EndpointData重新放入receivers中
      receivers.offer(data)
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      //首先判断dispatcher是否停止
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      //如果没有停止，需要对RpcEndpoint注册
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
        postMessage(name, message, (e) => { e match {
          case e: RpcEnvStoppedException => logDebug (s"Message $message dropped. ${e.getMessage}")
          case e: Throwable => logWarning(s"Message $message dropped. ${e.getMessage}")
        }}
      )}
  }

  //回复的消息中封装了RpcCallContext
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  //回复的消息中封装了RpcCallContext
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  //回复的消息中没有封装RpcCallContext
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * Posts a message to a specific endpoint.
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
  //用于将消息交给指定的RpcEndpoint
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      //根据端点名称从exdpoints中获取EndpointData
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        //如果当前Dispatcher没有停止并且缓存endpoints中确实存在名为endpointName的消息列表，因此还 需要将endpointData推入receives
        data.inbox.post(message)
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    receivers.offer(PoisonPill)
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  //用于对消息进行调度的线程池
  private val threadpool: ThreadPoolExecutor = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    //获取默认值2 和 当前系统可用处理器数量之间的最大值
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, availableCores))
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  //对消息处理  不断消费endpointData中InBox的消息
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            //从LinkedBlockingQueue 队列种移除并获取第一个元素EndpointData  如果获取不到就会阻塞
            val data = receivers.take()
            if (data == PoisonPill) {
              // 如果取到的消息是空的，会将消息添加到队列中。然后返回，因为threadPool中可能不止一个messageLoop,为了使其他线程获取到该线程也不予处理，达到所有线程遇到毒药都结束的效果
              receivers.offer(PoisonPill)
              return
            }
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new EndpointData(null, null, null)
}
