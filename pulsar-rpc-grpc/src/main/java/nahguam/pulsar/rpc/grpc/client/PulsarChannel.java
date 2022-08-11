package nahguam.pulsar.rpc.grpc.client;

import static java.util.UUID.randomUUID;
import static nahguam.pulsar.rpc.grpc.common.MessagingFactory.closeChannel;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import nahguam.pulsar.rpc.grpc.common.MessageMarshaller;
import nahguam.pulsar.rpc.grpc.common.MessageSender;
import nahguam.pulsar.rpc.grpc.common.MessagingFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@RequiredArgsConstructor
public class PulsarChannel extends Channel implements AutoCloseable {
  private final Supplier<String> correlationIdSupplier = () -> randomUUID().toString();
  private final Consumer<byte[]> consumer;
  private final Producer<byte[]> producer;
  private final Map<String, PulsarClientHandler<?>> handlers;

  @SuppressWarnings("unused")
  @Builder
  private static PulsarChannel create(PulsarClient client, String topicPrefix, String subscription)
      throws PulsarClientException {
    var handlers = new HashMap<String, PulsarClientHandler<?>>();
    var messagingFactory = new MessagingFactory(client, topicPrefix, subscription);
    var listener = new PulsarClientListener(handlers);
    var consumer = messagingFactory.responsesConsumer(listener);
    var producer = messagingFactory.requestsProducer();
    return new PulsarChannel(consumer, producer, handlers);
  }

  @Override
  public <T, R> ClientCall<T, R> newCall(MethodDescriptor<T, R> descriptor, CallOptions options) {
    var correlationId = correlationIdSupplier.get();
    var sender = new MessageSender(correlationId, producer);
    var requestMarshaller = new MessageMarshaller<>(descriptor.getRequestMarshaller());
    var responseMarshaller = new MessageMarshaller<>(descriptor.getResponseMarshaller());
    var handlerFactory =
        new PulsarClientHandler.Factory<>(responseMarshaller, options.getExecutor());
    return new PulsarClientCall<>(
        correlationId,
        descriptor.getFullMethodName(),
        requestMarshaller,
        handlerFactory,
        sender,
        handlers);
  }

  @Override
  public String authority() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws Exception {
    closeChannel(consumer, producer);
  }
}
