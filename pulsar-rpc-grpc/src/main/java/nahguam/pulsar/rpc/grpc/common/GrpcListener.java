package nahguam.pulsar.rpc.grpc.common;

import java.io.IOException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

@Slf4j
public abstract class GrpcListener implements MessageListener<byte[]> {
  @SneakyThrows // TODO deal with exceptions
  @Override
  public void received(Consumer<byte[]> consumer, Message<byte[]> message) {
    consumer.acknowledge(message);
    var type = MessageType.get(message.getProperties());
    var correlationId = message.getKey();
    log.debug("Received {}: {}", type, correlationId);
    onReceived(correlationId, type, message.getValue());
  }

  protected abstract void onReceived(String correlationId, MessageType type, byte[] message)
      throws IOException;
}
