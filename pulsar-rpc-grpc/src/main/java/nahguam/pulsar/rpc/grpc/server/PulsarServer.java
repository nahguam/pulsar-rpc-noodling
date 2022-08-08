package nahguam.pulsar.rpc.grpc.server;

import static nahguam.pulsar.rpc.grpc.common.PulsarUtils.requests;
import static nahguam.pulsar.rpc.grpc.common.PulsarUtils.responses;
import io.grpc.BindableService;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

@Slf4j
@RequiredArgsConstructor
public class PulsarServer implements AutoCloseable {
    private final PulsarClient client;

    @Builder
    PulsarServer(PulsarClient client, String topicPrefix, String subscription, BindableService service) {
        this(client);
        consumer(client, topicPrefix, subscription,
                new PulsarRequestListener(producer(client, topicPrefix), service.bindService()));
    }

    @SneakyThrows
    private static void consumer(PulsarClient client, String topicPrefix, String subscription,
                                 MessageListener<byte[]> listener) {
        client.newConsumer()
                .topic(requests(topicPrefix))
                .subscriptionName(subscription)
                .messageListener(listener)
                .subscribe();
    }

    @SneakyThrows
    private static Producer<byte[]> producer(PulsarClient client, String topicPrefix) {
        return client.newProducer()
                .topic(responses(topicPrefix))
                .create();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
