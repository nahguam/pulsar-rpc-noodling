package nahguam.pulsar.rpc.grpc.server;

import static lombok.AccessLevel.PRIVATE;
import static nahguam.pulsar.rpc.grpc.common.MessagingFactory.closeChannel;
import io.grpc.BindableService;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import nahguam.pulsar.rpc.grpc.common.MessagingFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@RequiredArgsConstructor(access = PRIVATE)
public class PulsarServer implements AutoCloseable {
  private final Consumer<byte[]> consumer;
  private final Producer<byte[]> producer;

  @SuppressWarnings("unused")
  @Builder
  private static PulsarServer create(
      PulsarClient client, String topicPrefix, String subscription, BindableService service)
      throws PulsarClientException {
    //TODO support the ability to listen on multiple channels, perhaps a topics pattern/prefix
    var messagingFactory = new MessagingFactory(client, topicPrefix, subscription);
    var producer = messagingFactory.responsesProducer();
    var handlerFactory = new PulsarServerHandler.Factory(producer);
    var listener = new PulsarServerListener(handlerFactory, service.bindService());
    var consumer = messagingFactory.requestsConsumer(listener);
    return new PulsarServer(consumer, producer);
  }

  @Override
  public void close() throws Exception {
    closeChannel(consumer, producer);
  }
}
