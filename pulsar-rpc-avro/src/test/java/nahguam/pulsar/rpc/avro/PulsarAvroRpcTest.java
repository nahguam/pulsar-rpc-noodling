package nahguam.pulsar.rpc.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import lombok.Cleanup;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;

class PulsarAvroRpcTest {

    @Test
    void test() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        String baseTopic = "non-persistent://public/default/greeting6";

        SpecificResponder responder = new SpecificResponder(
                io.streamnative.nahguam.rpc.Example.class, new ExampleImpl());

        @Cleanup
        PulsarServer server = PulsarServer.create(client, baseTopic, "foo", responder);
        @Cleanup
        PulsarTransceiver transceiver = PulsarTransceiver.requestor(client, baseTopic, "foo");

        io.streamnative.nahguam.rpc.Example
                example = SpecificRequestor.getClient(io.streamnative.nahguam.rpc.Example.class, transceiver);

        String response = example.greeting("Dave").toString();
        assertEquals("Hello Dave!", response);
    }

    static class ExampleImpl implements io.streamnative.nahguam.rpc.Example {
        @Override
        public CharSequence greeting(CharSequence name) {
            return "Hello " + name + "!";
        }
    }
}