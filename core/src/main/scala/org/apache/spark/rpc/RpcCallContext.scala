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

/**
  * A callback that [[RpcEndpoint]] can use to send back a message or failure. It's thread-safe
  * and can be called in any thread.
  */
//用于回调客户端的上下文
private[spark] trait RpcCallContext {

  //用于向发送者回复消息
  def reply(response: Any): Unit

  //用于向发送者发送失败信息
  def sendFailure(e: Throwable): Unit

  //用于获取发送者的地址
  def senderAddress: RpcAddress
}
