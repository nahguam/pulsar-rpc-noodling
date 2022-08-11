package nahguam.pulsar.rpc.grpc.common;

import com.google.protobuf.AbstractMessageLite;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
@RequiredArgsConstructor
public class MessageSender {
  private final String correlationId;
  private final Producer<byte[]> producer;

  public void send(MessageType type, AbstractMessageLite<?, ?> message)
      throws PulsarClientException {
    log.debug("Sending {}: {}", type, correlationId);
    producer
        .newMessage()
        .property(MessageType.PROPERTY, type.name())
        .key(correlationId)
        .value(message.toByteArray())
        .send();
  }
}
