package com.ekaqu.lsmt.ipc;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos.KeyValue;
import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.DataService;
import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.GetRequest;
import static com.ekaqu.lsmt.ipc.protobuf.generated.ServiceProtos.GetResponse;

/**
 *
 */
public class ProtoRPCServerExample {

  @Test
  public void test() {
    PeerInfo serverInfo = new PeerInfo("dcapwell-laptop", 8080);

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

    bootstrap.bind();


    try {
      TimeUnit.SECONDS.sleep(20);
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
