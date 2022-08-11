package nahguam.pulsar.rpc.grpc.common;

import static lombok.AccessLevel.PRIVATE;
import com.google.protobuf.ByteString;
import io.grpc.InternalMetadata;
import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import nahguam.pulsar.rpc.grpc.protocol.Metadata;

@RequiredArgsConstructor(access = PRIVATE)
public final class GrpcUtils {
  public static io.grpc.Metadata metadata(Metadata metadata) {
    var bytes =
        metadata.getValuesList().stream().map(ByteString::toByteArray).toArray(byte[][]::new);
    // Internal gRPC API
    return InternalMetadata.newMetadata(bytes);
  }

  public static Metadata metadata(io.grpc.Metadata metadata) {
    // Internal gRPC API
    var bytes = InternalMetadata.serialize(metadata);
    var values = Arrays.stream(bytes).map(ByteString::copyFrom).toList();
    return Metadata.newBuilder().addAllValues(values).build();
  }
}
