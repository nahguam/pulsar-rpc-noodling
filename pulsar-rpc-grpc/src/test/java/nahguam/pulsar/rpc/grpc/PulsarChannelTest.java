package nahguam.pulsar.rpc.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import io.grpc.stub.StreamObserver;
import lombok.Cleanup;
import nahguam.pulsar.rpc.grpc.GreeterGrpc.GreeterBlockingStub;
import nahguam.pulsar.rpc.grpc.client.PulsarChannel;
import nahguam.pulsar.rpc.grpc.server.PulsarServer;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;

class PulsarChannelTest {
    @Test
    void test() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        String topicPrefix = "greeter";

        @Cleanup
        PulsarServer server = PulsarServer.builder()
                .client(client)
                .topicPrefix(topicPrefix)
                .subscription("subscription")
                .service(new GreeterImpl())
                .build();

        @Cleanup
        PulsarChannel channel = PulsarChannel.builder()
                .client(client)
                .topicPrefix(topicPrefix)
                .subscription("subscription")
                .build();

        HelloRequest request = HelloRequest.newBuilder()
                .setName("Dave")
                .build();

        GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(channel);
        HelloReply reply = stub.sayHello(request);
        assertEquals("Hello Dave!", reply.getMessage());
    }

    static class GreeterImpl extends GreeterGrpc.GreeterImplBase {
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> observer) {
            HelloReply reply = HelloReply.newBuilder()
                    .setMessage("Hello " + request.getName() + "!")
                    .build();
            observer.onNext(reply);
            observer.onCompleted();
        }
    }
}