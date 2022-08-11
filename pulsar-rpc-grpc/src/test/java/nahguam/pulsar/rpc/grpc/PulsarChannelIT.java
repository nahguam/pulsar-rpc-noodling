package nahguam.pulsar.rpc.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import nahguam.pulsar.rpc.grpc.client.PulsarChannel;
import nahguam.pulsar.rpc.grpc.server.PulsarServer;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

// TODO pulsar testcontainers
@Testcontainers
class PulsarChannelIT {
  private static final DockerImageName pulsarImage =
      DockerImageName.parse("apachepulsar/pulsar").withTag("2.10.1");

  @Container
  private static final PulsarContainer pulsar =
      new PulsarContainer(pulsarImage) {
        @Override
        protected void configure() {
          super.configure();
          //noinspection resource
          withStartupTimeout(Duration.ofMinutes(3));
        }
      };

  private PulsarServer server;
  private PulsarChannel channel;

  @BeforeEach
  void beforeEach(TestInfo testInfo) throws Exception {
    var topicPrefix = "example-" + testInfo.getDisplayName();
    var client =
        PulsarClient.builder()
            // .serviceUrl("pulsar://localhost:6650")
            .serviceUrl(pulsar.getPulsarBrokerUrl())
            .build();
    server =
        PulsarServer.builder()
            .client(client)
            .topicPrefix(topicPrefix)
            .subscription("subscription")
            .service(new ExampleImpl())
            .build();
    channel =
        PulsarChannel.builder()
            .client(client)
            .topicPrefix(topicPrefix)
            .subscription("subscription")
            .build();
  }

  @AfterEach
  void afterEach() throws Exception {
    server.close();
    channel.close();
  }

  @Test
  void single() {
    var stub = ExampleGrpc.newBlockingStub(channel);

    var response = stub.single(request("foo"));

    assertEquals("foo-0", response.getValue());
  }

  @Test
  void streamOut() {
    var stub = ExampleGrpc.newBlockingStub(channel);

    var list = ImmutableList.copyOf(stub.streamOut(request("foo")));

    assertEquals(2, list.size());
    assertEquals("foo-0", list.get(0).getValue());
    assertEquals("foo-1", list.get(1).getValue());
  }

  @Test
  void streamIn() throws Exception {
    var stub = ExampleGrpc.newStub(channel);

    var list = new ArrayList<Response>();
    var latch = new CountDownLatch(1);
    var observer =
        stub.streamIn(
            new StreamObserver<>() {
              @Override
              public void onNext(Response response) {
                list.add(response);
              }

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {
                latch.countDown();
              }
            });
    observer.onNext(request("foo"));
    observer.onNext(request("bar"));
    observer.onCompleted();

    //noinspection ResultOfMethodCallIgnored
    latch.await(10, TimeUnit.SECONDS);

    assertEquals(1, list.size());
    assertEquals("foo-bar", list.get(0).getValue());
  }

  static class ExampleImpl extends ExampleGrpc.ExampleImplBase {
    @Override
    public void single(Request request, StreamObserver<Response> observer) {
      var response = response(request, 0);
      observer.onNext(response);
      observer.onCompleted();
    }

    @Override
    public void streamOut(Request request, StreamObserver<Response> observer) {
      observer.onNext(response(request, 0));
      observer.onNext(response(request, 1));
      observer.onCompleted();
    }

    @Override
    public StreamObserver<Request> streamIn(StreamObserver<Response> observer) {
      var list = new ArrayList<String>();
      return new StreamObserver<>() {
        @Override
        public void onNext(Request request) {
          list.add(request.getValue());
        }

        @Override
        public void onError(Throwable t) {
          observer.onError(t);
        }

        @Override
        public void onCompleted() {
          observer.onNext(Response.newBuilder().setValue(String.join("-", list)).build());
          observer.onCompleted();
        }
      };
    }
  }

  private static Request request(String value) {
    return Request.newBuilder().setValue(value).build();
  }

  private static Response response(Request request, int i) {
    return Response.newBuilder().setValue(request.getValue() + "-" + i).build();
  }
}
