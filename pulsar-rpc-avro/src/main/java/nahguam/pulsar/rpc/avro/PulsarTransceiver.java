package nahguam.pulsar.rpc.avro;

import static java.util.concurrent.TimeUnit.SECONDS;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@RequiredArgsConstructor
public class PulsarTransceiver extends BaseTransceiver {
    private static final String REQUESTS_SUFFIX = "-requests";
    private static final String RESPONSES_SUFFIX = "-responses";

    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;
    private volatile boolean running = true;

    enum Type { REQUESTOR, RESPONSER}
    private final Type type;

    public static PulsarTransceiver requestor(PulsarClient client, String baseTopic, String subscriptionName)
            throws PulsarClientException {
        Producer<byte[]> producer = producer(client, baseTopic + REQUESTS_SUFFIX);
        Consumer<byte[]> consumer = consumer(client, baseTopic + RESPONSES_SUFFIX, subscriptionName);
        return new PulsarTransceiver(producer, consumer, Type.REQUESTOR);
    }

    static PulsarTransceiver responder(PulsarClient client, String baseTopic, String subscriptionName)
            throws PulsarClientException {
        Producer<byte[]> producer = producer(client, baseTopic + RESPONSES_SUFFIX);
        Consumer<byte[]> consumer = consumer(client, baseTopic + REQUESTS_SUFFIX, subscriptionName);
        return new PulsarTransceiver(producer, consumer, Type.RESPONSER);
    }

    private static Producer<byte[]> producer(PulsarClient client, String topic) throws PulsarClientException {
        return client.newProducer()
                .topic(topic)
                .create();
    }

    private static Consumer<byte[]> consumer(PulsarClient client, String topic, String subscriptionName)
            throws PulsarClientException {
        return client.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscribe();
    }


    @Override
    public String getRemoteName() {
        return "pulsar"; //TODO remote name
    }

    @Override
    void writeBuffer(byte[] buffer) throws IOException {
        if (running) {
            producer.send(buffer);
            System.out.println("sent: "+type);
            System.out.println("[" + new String(buffer) + "]");
        }
    }

    @Override
    byte[] readBuffer() throws IOException {
        while (running) {
            Message<byte[]> message = consumer.receive(1, SECONDS);
            if (message != null) {
                System.out.println("received: "+type);
                System.out.println("[" + new String(message.getValue()) + "]");
                consumer.acknowledge(message);
                return message.getValue();
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        running = false;
        producer.close();
        consumer.close();
    }
}
