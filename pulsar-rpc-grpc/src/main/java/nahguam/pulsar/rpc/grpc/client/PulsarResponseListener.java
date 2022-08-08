package nahguam.pulsar.rpc.grpc.client;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

@Slf4j
@RequiredArgsConstructor
class PulsarResponseListener implements MessageListener<byte[]> {
    private final Map<String, PulsarResponseHandler<?>> handlers;

    @SneakyThrows
    @Override
    public void received(Consumer<byte[]> consumer, Message<byte[]> message) {
        consumer.acknowledge(message);
        String correlationId = message.getKey();
        byte[] bytes = message.getValue();
        PulsarResponseHandler<?> context = handlers.remove(correlationId);
        if (context != null) {
            log.info("handleResponse {} - '{}'", correlationId, new String(bytes));
            context.handleResponse(bytes);
        }
    }
}
