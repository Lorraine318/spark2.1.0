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

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 */
//子类有： 在yarn模式下的扩展HistoryServerUI，  MasterWebUi, WorkerWebUi, SparkUi
private[spark] abstract class WebUI(
    val securityManager: SecurityManager,  //sparkEnv中创建的安全管理器
    val sslOptions: SSLOptions,   //使用SecurityManager获取spark.ssl.ui属性指定的WebUI的SSL选项
    port: Int,     //webUI对外服务的端口。可以通过spark.ui.port来进行配置
    conf: SparkConf,
    basePath: String = "",  //webui的基本路径
    name: String = "")  //webui的名称
  extends Logging {

  protected val tabs = ArrayBuffer[WebUITab]()  //webUITab的缓冲数组
  protected val handlers = ArrayBuffer[ServletContextHandler]() //ServletContextHandler的缓冲数组。ServletContextHandler是Jetty提供的API，负责对ServletContext进行处理
  //webUIPage 和ServletContextHandler缓冲数组之间的映射挂你。
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  //用于缓冲serverInfo,即webUI的Jetty服务器信息
  protected var serverInfo: Option[ServerInfo] = None
  //当前WebUI的jetty服务的主机名
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))
  //当前类的简单名称
  private val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath  //获取basePath
  def getTabs: Seq[WebUITab] = tabs  //获取tabs中的所有WebUITab.并以scala的序列返回。
  def getHandlers: Seq[ServletContextHandler] = handlers //获取handlers中的所有ServletContextHandler。并以scala的序列返回
  def getSecurityManager: SecurityManager = securityManager

  //首先向tabs中添加WebUiTab ，然后给每个WebUiTab去绑定对应的ServletContextHandler
  def attachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  /** Detaches a tab from this UI, along with all of its attached pages. */
  def detachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  /** Detaches a page from this UI, along with all of its attached handlers. */
  def detachPage(page: WebUIPage): Unit = {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** Attaches a page to this UI. */
  def attachPage(page: WebUIPage): Unit = {
    val pagePath = "/" + page.prefix
    //调用JettyUtils的createServletHandler的方法给WebuiPage创建于reader和readerJson的两个方法分别关联servletContextHandler
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), securityManager, conf, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    //将映射的这两个ServletContextHandler 和 WebUiPage 更新到pageToHandlers 中
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
  }

  //给handlers缓冲数组中添加ServletContextHandler，并且通过serverInfo的addHandler方法添加到Jetty服务器中
  def attachHandler(handler: ServletContextHandler): Unit = {
    handlers += handler
    serverInfo.foreach(_.addHandler(handler))
  }

  //handlers缓冲数组中移除ServletContextHandler，并且通过serverInfo的addHandler方法从Jetty服务器中移除
  def detachHandler(handler: ServletContextHandler): Unit = {
    handlers -= handler
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
   * Detaches the content handler at `path` URI.
   *
   * @param path Path in UI to unmount.
   */
  def detachHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /**
   * Adds a handler for static content.
   *
   * @param resourceBase Root of where to find resources to serve.
   * @param path Path in UI where to mount the resources.
   */
  //创建静态文件服务的ServletContextHandler
  def addStaticHandler(resourceBase: String, path: String = "/static"): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  //用于初始化webUI服务中的所有组件，需要子类实现
  def initialize(): Unit

 //启动于webUI绑定的Jetty服务
  def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val host = Option(conf.getenv("SPARK_LOCAL_IP")).getOrElse("0.0.0.0")
      serverInfo = Some(startJettyServer(host, port, sslOptions, handlers, conf, name))
      logInfo(s"Bound $className to $host, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(s"Failed to bind $className", e)
        System.exit(1)
    }
  }

  //获取WebUI的web界面的URL
  def webUrl: String = s"http://$publicHostName:$boundPort"

  //获取WebUI的jetty服务的端口
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  //停止webui ，实际上就是停止webUI底层的jetty服务
  def stop(): Unit = {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.foreach(_.stop())
  }
}


/**
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes.
 */
//用于展现一组WebUiPage       parent: 上一级节点 webUiTab的父亲只能是WebUI        prefix :当前webUITab的前缀。 prefix将商机节点的路径一起构成当前WebUItAb的访问路径
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  //当前WebUITab所包含的WebUIPage的缓冲数组
  val pages = ArrayBuffer[WebUIPage]()
  //当前WebUITab的名称
  val name = prefix.capitalize

  //首先将当前WebUITab的前缀与WebUIPage的前缀拼接，作为WebUIPage的访问路径。然后向pages中添加WebUIPage
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  //获取父亲WebUI中的所有WebUITab.
  def headerTabs: Seq[WebUITab] = parent.getTabs
  //获取父亲WebUI的基本路径
  def basePath: String = parent.getBasePath
}


/**
 * A page that represents the leaf node in the UI hierarchy.
 *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
 */
//定义了页面的规范
private[spark] abstract class WebUIPage(var prefix: String) {
  //渲染页面
  def render(request: HttpServletRequest): Seq[Node]
  //生成JSON
  def renderJson(request: HttpServletRequest): JValue = JNothing
}
