package nahguam.pulsar.rpc.grpc.server;

import static nahguam.pulsar.rpc.grpc.common.MessageType.CLIENT_START;
import io.grpc.ServerServiceDefinition;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nahguam.pulsar.rpc.grpc.common.GrpcListener;
import nahguam.pulsar.rpc.grpc.common.MessageType;
import nahguam.pulsar.rpc.grpc.common.GrpcUtils;
import nahguam.pulsar.rpc.grpc.protocol.Start;

@Slf4j
@RequiredArgsConstructor
class PulsarServerListener extends GrpcListener {
  private final Map<String, PulsarServerHandler<?>> handlers = new HashMap<>();
  private final PulsarServerHandler.Factory handlerFactory;
  private final ServerServiceDefinition definition;

  @Override
  protected void onReceived(String correlationId, MessageType type, byte[] message)
      throws IOException {
    var handler = handlers.get(correlationId);

    if (type != CLIENT_START && handler == null) {
      log.warn("No handler found for {}", correlationId);
      return;
    }

    switch (type) {
      case CLIENT_START -> {
        handler = onStart(correlationId, message);
        handlers.put(correlationId, handler);
      }
      case CLIENT_REQUEST -> handler.onRequest(message);
      case CLIENT_CANCEL -> {
        handler.onCancel(message);
        handlers.remove(correlationId);
      }
      case CLIENT_HALF_CLOSE -> handler.onHalfClose(message);
      case CLIENT_INPUT -> handler.onInput(message);
      case CLIENT_COMPLETE -> {
        handler.onComplete(message);
        handlers.remove(correlationId);
      }
      default -> throw new IllegalStateException("Unexpected type: " + type);
    }
  }

  private PulsarServerHandler<?> onStart(String correlationId, byte[] message) throws IOException {
    var start = Start.parseFrom(message);
    var method = definition.getMethod(start.getMethodName());
    var metadata = GrpcUtils.metadata(start.getMetadata());
    return handlerFactory.create(correlationId, method, metadata);
  }
}
