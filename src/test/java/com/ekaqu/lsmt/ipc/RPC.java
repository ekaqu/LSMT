package com.ekaqu.lsmt.ipc;

import com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.stumbleupon.async.Deferred;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.GetRequest;
import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.GetResponse;

/**
 *
 */
public class RPC {

  public static ClientRPC getClientRPC(InetSocketAddress serverAddress) throws IOException {
    PeerInfo client = new PeerInfo(
        InetAddress.getLocalHost().getHostName(),
        (int) Thread.currentThread().getId()); // hope there arn't a lot of threads...

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

    final RpcClient channel = bootstrap.peerWith(serverAddress);


    return new ClientRPC() {
      private ServiceProtos.DataService.Stub stub = ServiceProtos.DataService.newStub(channel);
      private RpcController controller = channel.newRpcController();

      public Deferred<GetResponse> getData(final GetRequest request) throws IOException {
        final Deferred<GetResponse> responseDeferred = new Deferred<GetResponse>();
        stub.getData(controller, request, new RpcCallback<GetResponse>() {
          public void run(final GetResponse parameter) {
            if(parameter == null && controller.failed()) {
              // error
              // docs sound like i build an exception....
              responseDeferred.callback(new IOException(controller.errorText()));
            } else {
              responseDeferred.callback(parameter);
            }
          }
        });
        return responseDeferred;
      }
    };
  }
  public interface ClientRPC {
    Deferred<GetResponse> getData(GetRequest request) throws IOException;
  }
}
