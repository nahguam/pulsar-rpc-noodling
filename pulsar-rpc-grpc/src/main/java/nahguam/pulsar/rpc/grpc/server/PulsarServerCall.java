package nahguam.pulsar.rpc.grpc.server;

import static nahguam.pulsar.rpc.grpc.common.MessageType.SERVER_CLOSE;
import static nahguam.pulsar.rpc.grpc.common.MessageType.SERVER_HEADERS;
import static nahguam.pulsar.rpc.grpc.common.MessageType.SERVER_OUTPUT;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import nahguam.pulsar.rpc.grpc.common.MessageMarshaller;
import nahguam.pulsar.rpc.grpc.common.MessageSender;
import nahguam.pulsar.rpc.grpc.common.GrpcUtils;
import nahguam.pulsar.rpc.grpc.protocol.Close;
import nahguam.pulsar.rpc.grpc.protocol.Headers;
import nahguam.pulsar.rpc.grpc.protocol.Output;

@RequiredArgsConstructor
class PulsarServerCall<T, R> extends ServerCall<T, R> {
  private final MessageMarshaller<R> marshaller;
  private final MethodDescriptor<T, R> descriptor;
  private final MessageSender sender;
  private final AtomicBoolean cancelled;

  PulsarServerCall(
      MethodDescriptor<T, R> descriptor, MessageSender sender, AtomicBoolean cancelled) {
    this(
        new MessageMarshaller<>(descriptor.getResponseMarshaller()), descriptor, sender, cancelled);
  }

  @Override
  public void request(int numMessages) {}

  @SneakyThrows
  @Override
  public void sendHeaders(Metadata headers) {
    var metadata = Headers.newBuilder().setMetadata(GrpcUtils.metadata(headers)).build();
    sender.send(SERVER_HEADERS, metadata);
  }

  @SneakyThrows
  @Override
  public void sendMessage(R message) {
    var value = marshaller.stream(message);
    var output = Output.newBuilder().setValue(value).build();
    sender.send(SERVER_OUTPUT, output);
  }

  @SneakyThrows
  @Override
  public void close(Status status, Metadata trailers) {
    var close =
        Close.newBuilder()
            .setStatusCode(status.getCode().value())
            .setTrailers(GrpcUtils.metadata(trailers))
            .build();
    sender.send(SERVER_CLOSE, close);
  }

  @Override
  public boolean isCancelled() {
    return cancelled.getAcquire();
  }

  public MethodDescriptor<T, R> getMethodDescriptor() {
    return descriptor;
  }
}
