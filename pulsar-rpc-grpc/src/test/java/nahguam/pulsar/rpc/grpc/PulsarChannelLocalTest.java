package nahguam.pulsar.rpc.grpc;

import static com.salesforce.reactorgrpc.stub.ReactorCallOptions.CALL_OPTIONS_LOW_TIDE;
import static com.salesforce.reactorgrpc.stub.ReactorCallOptions.CALL_OPTIONS_PREFETCH;
import com.salesforce.reactorgrpc.stub.ReactorCallOptions;
import nahguam.pulsar.rpc.grpc.client.PulsarChannel;
import nahguam.pulsar.rpc.grpc.server.PulsarServer;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class PulsarChannelLocalTest {
  private PulsarServer server;
  private PulsarChannel channel;

  @BeforeEach
  void beforeEach(TestInfo testInfo) throws Exception {
    var topicPrefix = "example-" + testInfo.getDisplayName();
    var client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
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
  void test() throws Exception {
    var stub =
        ReactorExampleGrpc.newReactorStub(channel)
            .withOption(CALL_OPTIONS_PREFETCH, 4)
            .withOption(CALL_OPTIONS_LOW_TIDE, 2);

    stub.streamOut(Request.newBuilder().setValue("foo").build())
        .map(Response::getValue)
        .doOnNext(System.out::println)
        .then()
        .block();

    Thread.sleep(1_000L);
  }

  static class ExampleImpl extends ReactorExampleGrpc.ExampleImplBase {
    @Override
    public Flux<Response> streamOut(Mono<Request> request) {
      return request
          .flatMapMany(r -> Flux.range(0, 10).map(i -> r.getValue() + "-" + i))
          .map(s -> Response.newBuilder().setValue(s).build());
    }
  }
}
