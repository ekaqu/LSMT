package com.ekaqu.lsmt.ipc;

import com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import static com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos.KeyValue;
import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.DataService;
import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.GetRequest;
import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.GetResponse;

/**
 * Example taken from http://code.google.com/p/protobuf-rpc-pro/wiki/GettingStarted
 */
public class ProtoRPCServerExample {
  private InetSocketAddress socketAddress;

  @BeforeClass(alwaysRun = true)
  public void init() throws UnknownHostException {
    socketAddress = new InetSocketAddress(InetAddress.getLocalHost(), 8080);
  }

  @Test(groups = "example.ipc.server")
  public void startServer() {
    PeerInfo serverInfo = new PeerInfo(socketAddress);

    RpcServerCallExecutor executor = new ThreadPoolCallExecutor(10, 10);

    DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
        serverInfo,
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()),
        executor);

    bootstrap.getRpcServiceRegistry().registerService(DataService.newReflectiveService(new DataService.Interface() {
      public void getData(
          final RpcController controller,
          final GetRequest request,
          final RpcCallback<GetResponse> done) {
        System.out.println(request);
        done.run(GetResponse.newBuilder()
            .addData(KeyValue.newBuilder()
                .setRow(ByteString.copyFromUtf8("row"))
                .setQualifier(ByteString.copyFromUtf8("qualifier"))
                .setTimestamp(System.currentTimeMillis())
                .setValue(ByteString.copyFromUtf8("value")))
            .build());
      }
    }));

    System.out.println("Starting server on " + socketAddress);
    bootstrap.bind();
  }

  @Test(dependsOnGroups = "example.ipc.server")
  public void clientTest() throws IOException, ServiceException, InterruptedException {
    PeerInfo client = new PeerInfo(socketAddress.getHostName(), 1234);

    ThreadPoolCallExecutor executor = new ThreadPoolCallExecutor(3, 10);

    DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        client,
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()),
        executor);

    bootstrap.setOption("connectTimeoutMillis",10000);
    bootstrap.setOption("connectResponseTimeoutMillis",10000);
    bootstrap.setOption("receiveBufferSize", 1048576);
    bootstrap.setOption("tcpNoDelay", false);

    RpcClientChannel channel = bootstrap.peerWith(socketAddress);

    // blocking calll
    DataService.BlockingInterface dataService = DataService.newBlockingStub(channel);
    RpcController controller = channel.newRpcController();

    // make request
    GetRequest request = GetRequest.newBuilder().setRow(ByteString.copyFromUtf8("row1")).build();
    final Stopwatch stopwatch = new Stopwatch().start();
    GetResponse response = dataService.getData(controller, request);
    stopwatch.stop();
    System.out.println(response.getDataList());
    System.out.printf("Request took %s milliseconds\n", stopwatch.elapsedMillis());

    // do it again since the socket is open
    stopwatch.reset().start();
    response = dataService.getData(controller, request);
    stopwatch.stop();
    System.out.println(response.getDataList());
    System.out.printf("Request took %s milliseconds\n", stopwatch.elapsedMillis());
    
    // non-blocking
    DataService.Stub stub = DataService.newStub(channel);
    final Object lock = new Object();
    stopwatch.reset().start();
    stub.getData(controller, request, new RpcCallback<GetResponse>() {
      public void run(final GetResponse parameter) {
        System.out.println("Non-Blocking Callback");
        System.out.println(parameter.getDataList());

        stopwatch.stop();
        System.out.printf("Request took %s milliseconds\n", stopwatch.elapsedMillis());
        synchronized (lock) {
          lock.notify();
        }
      }
    });
    synchronized (lock) {
      lock.wait();
    }
  }
}
