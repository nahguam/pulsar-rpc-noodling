package nahguam.pulsar.rpc.grpc.client;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class PulsarResponseHandler<R> {
    private final ClientCall.Listener<R> listener;
    private final MethodDescriptor<?, R> descriptor;
    private final Executor executor;

    @SneakyThrows
    void handleResponse(byte[] bytes) {
        R response = descriptor.parseResponse(new ByteArrayInputStream(bytes));
        listener.onHeaders(new Metadata());
        listener.onMessage(response);
        listener.onClose(Status.OK, new Metadata());
        executor.execute(() -> log.info("execute runnable"));
    }
}
