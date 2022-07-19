package nahguam.pulsar.rpc.core;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class ClientChannel implements AutoCloseable {
    static final String CORRELATION_ID_KEY = "correlationId";
    private final ConcurrentMap<UUID, Context> contexts;
    private final ScheduledExecutorService executor;
    private final Duration responseTimeout;
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;

    public static ClientChannel create(@NonNull PulsarClient client, @NonNull String baseTopic,
                                       @NonNull String subscriptionName, ScheduledExecutorService executor,
                                       Duration responseTimeout) throws PulsarClientException {
        ConcurrentMap<UUID, Context> contexts = new ConcurrentHashMap<>();
        Producer<byte[]> producer = client.newProducer().topic(baseTopic + "-requests").create();
        ResponseListener responseListener = new ResponseListener(contexts);
        Consumer<byte[]> consumer =
                client.newConsumer().subscriptionName(subscriptionName).topic(baseTopic + "-responses")
                        .messageListener(responseListener).subscribe();
        return new ClientChannel(contexts, executor, responseTimeout, producer, consumer);
    }

    @Slf4j
    @RequiredArgsConstructor
    static class ResponseListener implements MessageListener<byte[]> {
        private final ConcurrentMap<UUID, Context> contexts;

        @Override
        public void received(Consumer<byte[]> consumer, Message<byte[]> message) {
            String correlationIdProperty = message.getProperty(CORRELATION_ID_KEY);
            if (correlationIdProperty != null) {
                UUID correlationId = UUID.fromString(correlationIdProperty);

                Context context = contexts.remove(correlationId);

                if (context != null) {
                    ScheduledFuture<?> timeout = context.getTimeout();
                    if (timeout != null) {
                        timeout.cancel(false);
                    }
                    context.getResponse().complete(message.getValue());
                } else {
                    log.warn("Request with correlationId [{}] in message [{}] not found, did it timeout or was it "
                            + "already " + "processed?", correlationId, message.getMessageId());
                }

                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.warn("Could not ack message [{}] with correlationId [{}]", message.getMessageId(),
                            correlationId);
                }
            } else {
                log.warn("Ignoring message [{}] because no '{}' property was found", message.getMessageId(),
                        CORRELATION_ID_KEY);
            }
        }
    }

    public CompletableFuture<byte[]> call(byte[] request) {
        CompletableFuture<byte[]> response = new CompletableFuture<>();

        UUID correlationId = UUID.randomUUID();

        Context context = new Context(response);
        contexts.put(correlationId, context);

        executor.execute(() -> {
            try {
                producer.newMessage().property(CORRELATION_ID_KEY, correlationId.toString()).value(request).send();
            } catch (PulsarClientException e) {
                response.completeExceptionally(e);
            }

            if (!response.isDone()) {
                ScheduledFuture<?> timeout =
                        executor.schedule(() -> onTimeout(correlationId), responseTimeout.toMillis(), MILLISECONDS);
                context.setTimeout(timeout);
            }
        });

        return response;
    }

    private void onTimeout(UUID correlationId) {
        Context context = contexts.remove(correlationId);
        if (context != null) {
            context.getResponse().completeExceptionally(new TimeoutException(
                    String.format("Request with correlationId [%s] exceeded its timeout [%s]", correlationId,
                            responseTimeout)));
        } else {
            log.warn("Request with correlationId [{}] not found, it may have already completed", correlationId);
        }
    }

    public void close() throws PulsarClientException {
        producer.close();
        consumer.close();
    }

    @Data
    static class Context {
        private final CompletableFuture<byte[]> response;
        private ScheduledFuture<?> timeout;
    }
}
