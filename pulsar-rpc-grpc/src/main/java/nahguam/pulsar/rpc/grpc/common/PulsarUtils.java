package nahguam.pulsar.rpc.grpc.common;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public final class PulsarUtils {
    public static final String METHOD_NAME_PROPERTY = "grpc-method-fullname";
    private static final String REQUESTS_SUFFIX = "-requests";
    private static final String RESPONSES_SUFFIX = "-responses";

    public static String requests(String prefix) {
        return prefix + REQUESTS_SUFFIX;
    }

    public static String responses(String prefix) {
        return prefix + RESPONSES_SUFFIX;
    }
}
