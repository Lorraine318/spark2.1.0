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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.*;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
//RPC客户端的工厂类,用来创建TransportClient的传输客户端工厂类
  //spark的各个模块都可以使用它来创建RPC客户端transportClient进行通信。不过一个transportClient只能和一个远程的RPC服务端进行通信。所以如果想要和多个RPC服务通信，就需要持有多个TransportClient实例。
public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes.
   * 一个简单的数据结构，用于跟踪两个对等节点之间的客户端池。
   * 在两个对等节点间维护的关于transportClient的池子，clientPool是TransportClientFactory的内部组件
   * */
  private static class ClientPool {
    /*
    * locks数组中的每个object 与 clients 数组中的每个transportClient的数组索引一一对应。
    * 可以通过对每个transportClient分别采用不同的锁。降低并发情况下线程间对锁的争用。进而减少阻塞，提高并发度。
    * */
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  //在transportClient上执行的客户端引导程序，建立socket连接的初始化操作。
  private final List<TransportClientBootstrap> clientBootstraps;

  //针对每个socket地址的连接池clientPool
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** Random number generator for picking connections between peers.
   * 随机数生成器，用于选择对等体之间的连接。
   * */
  private final Random rand;
  private final int numConnectionsPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private PooledByteBufAllocator pooledAllocator;
  private final NettyMemoryMetrics metrics;
//创建传输客户端工厂
  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    this.conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    //针对每一个socket地址的连接池clientPool的缓存
    this.connectionPool = new ConcurrentHashMap<>();
    //用于指定对等节点间的连接数
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    //对socket地址对应的连接池ClientPool中的transportClient进行随机选择。对每个连接做负载均衡。
    this.rand = new Random();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    //客户端channel被创建时使用的类，通过ioMode来匹配，默认为NioSocketChannel
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    //客户端只有worker组，它的实际类型是NioEventLoopGroup
    this.workerGroup = NettyUtils.createEventLoop(
        ioMode,
        conf.clientThreads(),
        conf.getModuleName() + "-client");
    //汇集ByteBuf但对本地线程缓存禁用的分配器。
    this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
    this.metrics = new NettyMemoryMetrics(
      this.pooledAllocator, conf.getModuleName() + "-client", conf);
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   *
   * Concurrency: This method is safe to call from multiple threads.
   */
  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    //调用InetSocketAddress.createUnresolved 创建地址，如果缓存中有transportClient,可以避免不必要的域名解析
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      //针对每一个socketAddress ,都对应的有一个clientPool 每个clientPool内部都有多个客户端连接transportClient和对应的锁。
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(unresolvedAddress);
    }
    //随机选择一个transportClient
    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    //获取并返回激活的TransportClient
    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }

      if (cachedClient.isActive()) {
        logger.trace("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      clientPool.clients[clientIndex] = createClient(resolvedAddress);
      return clientPool.clients[clientIndex];
    }
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port.
   * This connection is not pooled.
   *
   * As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    return createClient(address);
  }

  /** Create a completely new {@link TransportClient} to the remote address. */
  private TransportClient createClient(InetSocketAddress address)
      throws IOException, InterruptedException {
    logger.debug("Creating new connection to {}", address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator);

    if (conf.receiveBuf() > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.await(conf.connectionTimeoutMs())) {
      throw new IOException(
        String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        clientBootstrap.doBootstrap(client, channel);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }
  }
}
