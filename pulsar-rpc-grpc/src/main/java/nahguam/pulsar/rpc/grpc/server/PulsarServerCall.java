package nahguam.pulsar.rpc.grpc.server;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import java.io.InputStream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.shade.org.apache.commons.io.IOUtils;

@Slf4j
@RequiredArgsConstructor
class PulsarServerCall<T, R> extends ServerCall<T, R> {
    private final MethodDescriptor<T, R> descriptor;
    private final Producer<byte[]> producer;
    private final String correlationId;

    @Override
    public void request(int numMessages) {
        log.debug("request {}", numMessages);
    }

    @Override
    public void sendHeaders(Metadata headers) {
        log.debug("sendHeaders {}", headers);
    }

    @SneakyThrows
    @Override
    public void sendMessage(R message) {
        log.info("sendMessage {} - {} ", correlationId, message);
        InputStream inputStream = descriptor.streamResponse(message);
        byte[] bytes = IOUtils.toByteArray(inputStream);
        producer.newMessage()
                .key(correlationId)
                .value(bytes)
                .send();
    }

    @Override
    public void close(Status status, Metadata trailers) {
        log.debug("close {}, {}", status, trailers);
    }

    @Override
    public boolean isCancelled() {
        log.debug("isCancelled");
        return false;
    }

    public MethodDescriptor<T, R> getMethodDescriptor() {
        log.debug("getMethodDescriptor");
        return descriptor;
    }
}
