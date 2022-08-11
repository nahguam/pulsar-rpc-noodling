package nahguam.pulsar.rpc.grpc.common;

import java.util.Map;

public enum MessageType {
  CLIENT_START,
  CLIENT_REQUEST,
  CLIENT_CANCEL,
  CLIENT_HALF_CLOSE,
  CLIENT_INPUT,
  CLIENT_COMPLETE,
  SERVER_HEADERS,
  SERVER_OUTPUT,
  SERVER_CLOSE;
  static final String PROPERTY = "type";

  public static MessageType get(Map<String, String> map) {
    return valueOf(map.get(PROPERTY));
  }
}
