package nahguam.pulsar.rpc.core;

import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Function;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ServerChannel implements AutoCloseable {
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;

    public static ServerChannel create(@NonNull PulsarClient client, @NonNull String baseTopic,
                                       @NonNull String subscriptionName, Function<byte[], byte[]> requestHandler)
            throws PulsarClientException {
        Producer<byte[]> producer = client.newProducer().topic(baseTopic + "-responses").create();
        RequestListener requestListener = new RequestListener(producer, requestHandler);
        Consumer<byte[]> consumer =
                client.newConsumer().subscriptionName(subscriptionName).topic(baseTopic + "-requests")
                        .messageListener(requestListener).subscribe();
        return new ServerChannel(producer, consumer);
    }

    @Slf4j
    @RequiredArgsConstructor
    static class RequestListener implements MessageListener<byte[]> {
        private final Producer<byte[]> producer;
        private final Function<byte[], byte[]> requestHandler;

        @Override
        public void received(Consumer<byte[]> consumer, Message<byte[]> message) {
            try {
                byte[] response = requestHandler.apply(message.getValue());
                producer.newMessage().properties(message.getProperties()).value(response).send();
            } catch (PulsarClientException e) {
                throw new UncheckedIOException(e);
            }

            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                log.warn("Could not ack message [{}]", message.getMessageId());
            }
        }
    }

    public void close() throws PulsarClientException {
        producer.close();
        consumer.close();
    }
}
