package nahguam.pulsar.rpc.grpc.client;

import static nahguam.pulsar.rpc.grpc.common.PulsarUtils.METHOD_NAME_PROPERTY;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.shade.org.apache.commons.io.IOUtils;

@Slf4j
@RequiredArgsConstructor
class PulsarClientCall<RequestT, ResponseT> extends ClientCall<RequestT, ResponseT> {
    private final MethodDescriptor<RequestT, ResponseT> descriptor;
    private final Producer<byte[]> producer;
    private final Map<String, PulsarResponseHandler<?>> handlers;
    private final Executor executor;
    private String correlationId;

    @Override
    public void start(Listener<ResponseT> listener, Metadata headers) {
        log.debug("start {}, {}", listener, headers);
        correlationId = UUID.randomUUID().toString();
        handlers.put(correlationId, new PulsarResponseHandler<>(listener, descriptor, executor));
    }

    @Override
    public void request(int numMessages) {
        log.debug("request {}", numMessages);
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
        log.debug("cancel {}", message, cause);
    }

    @Override
    public void halfClose() {
        log.debug("halfClose");
    }

    @SneakyThrows
    @Override
    public void sendMessage(RequestT message) {
        log.info("sendMessage {} - {}", correlationId, message);
        InputStream inputStream = descriptor.streamRequest(message);
        byte[] bytes = IOUtils.toByteArray(inputStream);
        producer.newMessage()
                .property(METHOD_NAME_PROPERTY, descriptor.getFullMethodName())
                .key(correlationId)
                .value(bytes)
                .send();
    }
}
