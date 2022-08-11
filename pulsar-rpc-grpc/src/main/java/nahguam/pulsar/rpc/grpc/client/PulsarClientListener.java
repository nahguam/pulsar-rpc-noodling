package nahguam.pulsar.rpc.grpc.client;

import java.io.IOException;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nahguam.pulsar.rpc.grpc.common.GrpcListener;
import nahguam.pulsar.rpc.grpc.common.MessageType;

@Slf4j
@RequiredArgsConstructor
class PulsarClientListener extends GrpcListener {
  private final Map<String, PulsarClientHandler<?>> handlers;

  @Override
  protected void onReceived(String correlationId, MessageType type, byte[] message)
      throws IOException {
    var handler = handlers.get(correlationId);

    if (handler == null) {
      log.warn("No handler found for {}", correlationId);
      return;
    }

    switch (type) {
      case SERVER_HEADERS -> handler.onHeaders(message);
      case SERVER_OUTPUT -> handler.onOutput(message);
      case SERVER_CLOSE -> {
        handler.onClose(message);
        handlers.remove(correlationId);
      }
      default -> throw new IllegalStateException("Unexpected type: " + type);
    }
  }
}
