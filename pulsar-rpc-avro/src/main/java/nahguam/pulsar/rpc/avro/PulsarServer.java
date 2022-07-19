package nahguam.pulsar.rpc.avro;

import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PACKAGE;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import org.apache.avro.ipc.Responder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@RequiredArgsConstructor(access = PACKAGE)
public class PulsarServer implements AutoCloseable {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final PulsarTransceiver transceiver;
    private final Responder responder;
    private volatile boolean running = false;

    public static PulsarServer create(PulsarClient client, String baseTopic, String subscriptionName,
                                      Responder responder)
            throws PulsarClientException {
        PulsarTransceiver transceiver = PulsarTransceiver.responder(client, baseTopic, subscriptionName);
        PulsarServer server = new PulsarServer(transceiver, responder);
        server.start();
        return server;
    }

    void start() {
        if (!running) {
            synchronized (this) {
                if (!running) {
                    executor.execute(this::processRequests);
                    running = true;
                }
            }
        }
    }

    private void processRequests() {
        try {
            while (running) {
                List<ByteBuffer> buffers = transceiver.readBuffers();
                if (!buffers.isEmpty()) {
                    transceiver.writeBuffers(responder.respond(buffers));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (running) {
            synchronized (this) {
                if (running) {
                    running = false;
                    transceiver.close();
                    executor.shutdown();
                    executor.awaitTermination(5, SECONDS);
                }
            }
        }
    }
}
