package nahguam.pulsar.rpc.grpc.common;

import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor.Marshaller;
import java.io.IOException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MessageMarshaller<T> {
  private final Marshaller<T> marshaller;

  public ByteString stream(T message) throws IOException {
    try (var in = marshaller.stream(message)) {
      return ByteString.readFrom(in);
    }
  }

  public T parse(ByteString message) throws IOException {
    try (var in = message.newInput()) {
      return marshaller.parse(in);
    }
  }
}
