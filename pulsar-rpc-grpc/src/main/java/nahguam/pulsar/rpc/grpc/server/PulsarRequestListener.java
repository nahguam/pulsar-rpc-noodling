package nahguam.pulsar.rpc.grpc.server;

import static nahguam.pulsar.rpc.grpc.common.PulsarUtils.METHOD_NAME_PROPERTY;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import java.io.ByteArrayInputStream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;

@Slf4j
@RequiredArgsConstructor
class PulsarRequestListener implements MessageListener<byte[]> {
    private final Producer<byte[]> producer;
    private final ServerServiceDefinition definition;

    @SneakyThrows
    @Override
    public void received(Consumer<byte[]> consumer, Message<byte[]> message) {
        consumer.acknowledge(message);
        String methodName = message.getProperty(METHOD_NAME_PROPERTY);
        ServerMethodDefinition<?, ?> method = definition.getMethod(methodName);
        String correlationId = message.getKey();
        handleRequest(method, message.getValue(), correlationId);
    }

    private <T, R> void handleRequest(ServerMethodDefinition<T, R> method, byte[] bytes, String correlationId) {
        MethodDescriptor<T, R> descriptor = method.getMethodDescriptor();
        PulsarServerCall<T, R> call = new PulsarServerCall<>(descriptor, producer, correlationId);
        ServerCallHandler<T, R> handler = method.getServerCallHandler();
        ServerCall.Listener<T> listener = handler.startCall(call, new Metadata());
        T request = descriptor.parseRequest(new ByteArrayInputStream(bytes));
        log.info("handleRequest {} - {}", correlationId, request);
        listener.onMessage(request);
        listener.onComplete();
        listener.onHalfClose();
    }
}
