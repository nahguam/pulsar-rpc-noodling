package nahguam.pulsar.rpc.grpc.common;

import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

@RequiredArgsConstructor
public class MessagingFactory {
    private static final String REQUESTS_SUFFIX = "-requests";
    private static final String RESPONSES_SUFFIX = "-responses";
    private final PulsarClient client;
    private final String topicPrefix;
    private final String subscription;

    public Consumer<byte[]> responsesConsumer(MessageListener<byte[]> listener) throws PulsarClientException {
        return client.newConsumer()
                .topic(responses(topicPrefix))
                .subscriptionName(subscription)
                // only a single client per channel
                .subscriptionType(SubscriptionType.Exclusive)
                .messageListener(listener)
                .subscribe();
    }

    public Producer<byte[]> requestsProducer() throws PulsarClientException {
        return client.newProducer()
                .topic(requests(topicPrefix))
                // only a single client per channel
                .accessMode(ProducerAccessMode.Exclusive)
                .create();
    }

    public Consumer<byte[]> requestsConsumer(MessageListener<byte[]> listener) throws PulsarClientException {
        return client.newConsumer()
                .topic(requests(topicPrefix))
                .subscriptionName(subscription)
                // servers can be horizontally scaled but the same server should receive
                // all messages for the same request
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(listener)
                .subscribe();
    }

    public Producer<byte[]> responsesProducer() throws PulsarClientException {
        return client.newProducer()
                .topic(responses(topicPrefix))
                // servers can produce messages to the same responses topic
                .accessMode(ProducerAccessMode.Shared)
                .create();
    }

    private static String requests(String prefix) {
        return prefix + REQUESTS_SUFFIX;
    }

    private static String responses(String prefix) {
        return prefix + RESPONSES_SUFFIX;
    }

    public static void closeChannel(Consumer<byte[]> consumer, Producer<byte[]> producer) throws PulsarClientException {
        try {
            consumer.close();
        } finally {
            producer.close();
        }
    }
}
