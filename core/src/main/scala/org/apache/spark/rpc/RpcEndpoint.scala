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

package org.apache.spark.rpc

import org.apache.spark.SparkException

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The life-cycle of an endpoint is:
 *
 * {constructor -> onStart -> receive* -> onStop}
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
//rpc端点，对能够处理RPC请求，给某一特定服务提供本地调用及跨界点调用的RPC组件的抽象。   是akka种actor的替代产物
private[spark] trait RpcEndpoint {

 //当前RPCEndpoint所属的RpcEnv
  val rpcEnv: RpcEnv

  //获取RpcEndpoint相关联的RpcEndpointRef
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  //接收消息并处理，但不需要给客户端回复
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

 //接收消息并处理，需要给客户端回复 ，回复是通过RpcCallContext来实现的
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  //当处理消息发生异常时调用。可以对异常进行一些处理
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  //当客户端与当前节点连接上之后调用。可以针对连接进行一些处理
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  //当客户端与当前节点的连接断开后调用。可以针对断开连接进行一些处理
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  //当客户端与当前节点之间的连接发生网络错误时调用。可以针对连接发生的网络错误进行一些处理
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  //在RPCEndpoint开始处理消息之前调用。可以在RpcEndpoint正式工作之前做一些准备工作
  def onStart(): Unit = {
    // By default, do nothing.
  }

  //在停止RpcEndpoint时调用。可以在RpcEndpoint停止的时候做一些收尾工作
  def onStop(): Unit = {
    // By default, do nothing.
  }

  //用于停止当前RpcEndpoint
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 */
//继承自RpcEndpoint的特质，主要用于对消息的出库，必须是线程安全的场景，对消息的处理都是串行的，即前一条消息处理完才能接着处理下一条消息
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint
