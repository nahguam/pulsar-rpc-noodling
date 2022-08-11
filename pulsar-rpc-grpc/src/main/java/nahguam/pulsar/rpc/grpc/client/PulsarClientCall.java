package nahguam.pulsar.rpc.grpc.client;

import static nahguam.pulsar.rpc.grpc.common.MessageType.CLIENT_START;
import static nahguam.pulsar.rpc.grpc.common.MessageType.CLIENT_HALF_CLOSE;
import static nahguam.pulsar.rpc.grpc.common.MessageType.CLIENT_INPUT;
import static nahguam.pulsar.rpc.grpc.common.MessageType.CLIENT_REQUEST;
import static nahguam.pulsar.rpc.grpc.common.MessageType.CLIENT_CANCEL;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import nahguam.pulsar.rpc.grpc.common.MessageMarshaller;
import nahguam.pulsar.rpc.grpc.common.MessageSender;
import nahguam.pulsar.rpc.grpc.common.GrpcUtils;
import nahguam.pulsar.rpc.grpc.protocol.Cancel;
import nahguam.pulsar.rpc.grpc.protocol.HalfClose;
import nahguam.pulsar.rpc.grpc.protocol.Input;
import nahguam.pulsar.rpc.grpc.protocol.Request;
import nahguam.pulsar.rpc.grpc.protocol.Start;

@Slf4j
@RequiredArgsConstructor
class PulsarClientCall<T, R> extends ClientCall<T, R> {
  private final String correlationId;
  private final String methodName;
  private final MessageMarshaller<T> requestMarshaller;
  private final PulsarClientHandler.Factory<R> handlerFactory;
  private final MessageSender sender;
  private final Map<String, PulsarClientHandler<?>> handlers;

  @SneakyThrows
  @Override
  public void start(Listener<R> listener, Metadata headers) {
    handlers.put(correlationId, handlerFactory.create(listener));
    var start =
        Start.newBuilder()
            .setMethodName(methodName)
            .setMetadata(GrpcUtils.metadata(headers))
            .build();
    sender.send(CLIENT_START, start);
  }

  @SneakyThrows
  @Override
  public void request(int numMessages) {
    log.debug("request {}", numMessages);
    var request = Request.newBuilder().setCount(numMessages).build();
    sender.send(CLIENT_REQUEST, request);
  }

  @SneakyThrows
  @Override
  public void cancel(@Nullable String message, @Nullable Throwable cause) {
    var cancel = Cancel.newBuilder().setMessage(message).build();
    sender.send(CLIENT_CANCEL, cancel);
    handlers.remove(correlationId);
  }

  @SneakyThrows
  @Override
  public void halfClose() {
    var halfClose = HalfClose.newBuilder().build();
    sender.send(CLIENT_HALF_CLOSE, halfClose);
  }

  @SneakyThrows
  @Override
  public void sendMessage(T message) {
    var value = requestMarshaller.stream(message);
    var input = Input.newBuilder().setValue(value).build();
    sender.send(CLIENT_INPUT, input);
  }
}
