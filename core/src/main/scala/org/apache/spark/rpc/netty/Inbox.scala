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

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}


//rpc传递消息的类型
private[netty] sealed trait InboxMessage

//rpcEndpoint处理此类型的消息后不需要向客户端回复消息
private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage
//RPC消息。 rpcEndpoint处理完此消息后需要向客户端回复消息
private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage
//用于Inbox实例化后，在通知与此Inbox相关联的RpcEndpoint启动
private[netty] case object OnStart extends InboxMessage
//用于Inbox停止后，在通知与此Inbox相关联的RpcEndpoint停止
private[netty] case object OnStop extends InboxMessage

//此消息用于告诉所有的RpcEndpoint，有远端的进程已经与当前RPC服务建立了连接
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

//此消息用于告诉所有的RpcEndpoint，有远端的进程已经与当前RPC服务断开了连接
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

//此消息用于告诉所有的RPCEndpoint，与远端某个地址之间的连接发生了错误
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

//端点内的盒子。每个RpcEndpoint都有一个对应的盒子，这个盒子里面有存储InboxMessage消息的列表messages.
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.

  //消息列表  用于缓存需要由对应RpcEndpoint处理的消息
  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  //Inbox的停止状态
  @GuardedBy("this")
  private var stopped = false

  //是否允许多个线程同时处理messages中的消息
  @GuardedBy("this")
  private var enableConcurrent = false

  //激活线程的数量，即正在处理messages中消息的线程数量
  @GuardedBy("this")
  private var numActiveThreads = 0

  // 将向Inbo自身的messages列表中放入onstart消息
  inbox.synchronized {
    messages.add(OnStart)
  }

  //inbox中的消息处理
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    //进行线程并发检查，如果不允许多个线程同时处理messages中的消息（enableConcurrent=false）  并且 当前激活线程数不为0，说明已经有线程在处理消息，所以当前线程不允许再去处理消息
    inbox.synchronized {
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      //从message中获取消息，如果有消息未处理，则当前线程需要处理此消息，激活线程数加1.
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    //根据消息类型进行匹配
    while (true) {
      //错误消息可以让onError接收到
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }
      //当消息处理完成之后
      inbox.synchronized {
        //如果不允许多个线程同时处理message中的消息并且当前激活的线程多于1个，那么需要当前线程退出
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        //移除并返回队列头部的消息 ，如果没有消息处理也退出当前线程
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }
  //将消息加入Inbox的消息列表中
  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      onDrop(message)
    } else {
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
     //为了确保并发安全运行，停止会先将enableConcurrent设置为false
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  //当inbox所对应的RpcEndpoint的错误处理方法 onError可以接收到错误信息
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) =>
            if (stopped) {
              logDebug("Ignoring error", ee)
            } else {
              logError("Ignoring error", ee)
            }
        }
    }
  }

}
