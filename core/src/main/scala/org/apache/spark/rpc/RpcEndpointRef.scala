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

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
//Rpc端点引用  是akka中actorRef的替代产物    如果想要向一个远端的RpcEndpoint发起请求。就必须持有这个RpcEndpoint的RpcEndpointRef
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {

  //Rpc最大重新连接次数，  spark.rpc.numRetries属性进行配置  默认3次
  private[this] val maxRetries = RpcUtils.numRetries(conf)
  //Rpc每次重新连接需要等待的毫秒数   spark.rpc.retry.wait 配置  默认3秒
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
  //RPC的ack操作的默认超时事件   可以使用（优先级高）spark.rpc.ackTimeout 或者spark.network.timeout属性来进行配置  默认120秒
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

 //返回当前RpcEndpointRef对应RpcEndpoint的RPC地址（RpcAddress）
  def address: RpcAddress
  //返回当前RpcEndpointRef对应RpcEndpoint的名称
  def name: String

  //发送单向异步的消息。不期待服务端的回复  采用了at-most-once的投递规则 每条应用了这种机制的消息会被投递0次或者1次。消息可能会丢失
  def send(message: Any): Unit

  //以默认的超时时间作为timeout参数
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * default timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].

   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  //发送同步的请求，此类请求将会被RpcEndpoint接收，并在指定的超时时间内等待返回类型为T的处理结果  ，超过时间会进行重试，知道达到了默认的重试次数为止
  //采用了at-least-once的投递规则 ，每条应用了这种机制的消息潜在的存在多次投递尝试并保证至少会成功一次，就是说这条消息可能会重复，但是不会丢失
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * specified timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }

}
