package nahguam.pulsar.rpc.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ChannelTest {
    @Test
    void test() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        String baseTopic = "persistent://public/default/channel";

        Function<byte[], byte[]> requestHandler = buf -> ("Hello " + new String(buf) + "!").getBytes();
        @Cleanup
        ServerChannel serverChannel = ServerChannel
                .create(client, baseTopic, "foo", requestHandler);

        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        Duration responseTimeout = Duration.ofMinutes(1);
        @Cleanup
        ClientChannel clientChannel = ClientChannel
                .create(client, baseTopic, "foo", executor, responseTimeout);

        String response = new String(clientChannel.call("Dave".getBytes()).join());
        assertEquals("Hello Dave!", response);
    }
}
