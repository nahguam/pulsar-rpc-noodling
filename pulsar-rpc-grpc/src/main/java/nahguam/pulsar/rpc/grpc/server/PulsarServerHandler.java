package nahguam.pulsar.rpc.grpc.server;

import io.grpc.Metadata;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerMethodDefinition;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nahguam.pulsar.rpc.grpc.common.MessageMarshaller;
import nahguam.pulsar.rpc.grpc.common.MessageSender;
import nahguam.pulsar.rpc.grpc.protocol.Cancel;
import nahguam.pulsar.rpc.grpc.protocol.Complete;
import nahguam.pulsar.rpc.grpc.protocol.HalfClose;
import nahguam.pulsar.rpc.grpc.protocol.Input;
import nahguam.pulsar.rpc.grpc.protocol.Request;
import org.apache.pulsar.client.api.Producer;

@Slf4j
@RequiredArgsConstructor
class PulsarServerHandler<T> {
  private final MessageMarshaller<T> marshaller;
  private final Listener<T> listener;
  private final AtomicBoolean cancelled;

  void onRequest(byte[] message) throws IOException {
    @SuppressWarnings("unused")
    var request = Request.parseFrom(message);
    log.debug("request {}", request.getCount());
    listener.onReady();
  }

  void onCancel(byte[] message) throws IOException {
    @SuppressWarnings("unused")
    var cancel = Cancel.parseFrom(message);
    listener.onCancel();
    cancelled.compareAndSet(false, true);
  }

  void onHalfClose(byte[] message) throws IOException {
    @SuppressWarnings("unused")
    var halfClose = HalfClose.parseFrom(message);
    listener.onHalfClose();
  }

  void onInput(byte[] message) throws IOException {
    var input = Input.parseFrom(message);
    var value = marshaller.parse(input.getValue());
    listener.onMessage(value);
  }

  void onComplete(byte[] message) throws IOException {
    @SuppressWarnings("unused")
    var complete = Complete.parseFrom(message);
    listener.onComplete();
  }

  @RequiredArgsConstructor
  static class Factory {
    private final Producer<byte[]> producer;

    <T, R> PulsarServerHandler<T> create(
        String correlationId, ServerMethodDefinition<T, R> method, Metadata metadata) {
      var descriptor = method.getMethodDescriptor();
      var marshaller = new MessageMarshaller<>(descriptor.getRequestMarshaller());
      var sender = new MessageSender(correlationId, producer);
      var cancelled = new AtomicBoolean();
      var call = new PulsarServerCall<>(descriptor, sender, cancelled);
      var callHandler = method.getServerCallHandler();
      var listener = callHandler.startCall(call, metadata);
      return new PulsarServerHandler<>(marshaller, listener, cancelled);
    }
  }
}
