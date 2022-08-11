package nahguam.pulsar.rpc.grpc.client;

import io.grpc.ClientCall.Listener;
import io.grpc.Status;
import java.io.IOException;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nahguam.pulsar.rpc.grpc.common.GrpcUtils;
import nahguam.pulsar.rpc.grpc.common.MessageMarshaller;
import nahguam.pulsar.rpc.grpc.protocol.Close;
import nahguam.pulsar.rpc.grpc.protocol.Headers;
import nahguam.pulsar.rpc.grpc.protocol.Output;

@Slf4j
@RequiredArgsConstructor
class PulsarClientHandler<R> {
  private final MessageMarshaller<R> marshaller;
  private final Listener<R> listener;
  private final Executor executor;

  void onHeaders(byte[] message) throws IOException {
    var headers = Headers.parseFrom(message);
    var metadata = GrpcUtils.metadata(headers.getMetadata());
    listener.onHeaders(metadata);
  }

  void onOutput(byte[] message) throws IOException {
    var output = Output.parseFrom(message);
    var value = marshaller.parse(output.getValue());
    listener.onMessage(value);
  }

  void onClose(byte[] message) throws IOException {
    var close = Close.parseFrom(message);
    var status = Status.fromCodeValue(close.getStatusCode());
    var trailers = GrpcUtils.metadata(close.getTrailers());
    listener.onClose(status, trailers);
    if (executor != null) {
      executor.execute(() -> log.debug("Execute runnable, because why not?"));
    }
  }

  @RequiredArgsConstructor
  static class Factory<R> {
    private final MessageMarshaller<R> marshaller;
    private final Executor executor;

    PulsarClientHandler<R> create(Listener<R> listener) {
      return new PulsarClientHandler<>(marshaller, listener, executor);
    }
  }
}
