package nahguam.pulsar.rpc.grpc.client;

import static nahguam.pulsar.rpc.grpc.common.PulsarUtils.requests;
import static nahguam.pulsar.rpc.grpc.common.PulsarUtils.responses;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

@Slf4j
@RequiredArgsConstructor
public class PulsarChannel extends Channel implements AutoCloseable {
    private final Map<String, PulsarResponseHandler<?>> handlers = new HashMap<>();
    private final PulsarClient client;
    private final Producer<byte[]> producer;

    @Builder
    PulsarChannel(PulsarClient client, String topicPrefix, String subscription) {
        this(client, producer(client, topicPrefix));
        consumer(client, topicPrefix, subscription, new PulsarResponseListener(handlers));
    }

    @SneakyThrows
    private static Consumer<byte[]> consumer(PulsarClient client, String topicPrefix, String subscription,
                                             MessageListener<byte[]> listener) {
        return client.newConsumer()
                .topic(responses(topicPrefix))
                .subscriptionName(subscription)
                .messageListener(listener)
                .subscribe();
    }

    @SneakyThrows
    private static Producer<byte[]> producer(PulsarClient client, String topicPrefix) {
        return client.newProducer()
                .topic(requests(topicPrefix))
                .create();
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
            MethodDescriptor<RequestT, ResponseT> descriptor, CallOptions options) {
        return new PulsarClientCall<>(descriptor, producer, handlers, options.getExecutor());
    }

    @Override
    public String authority() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
