package nahguam.pulsar.rpc.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.ipc.Transceiver;

abstract class BaseTransceiver extends Transceiver {
    @Override
    public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writeBuffers(buffers, baos);
            writeBuffer(baos.toByteArray());
        }
    }

    abstract void writeBuffer(byte[] buffer) throws IOException;

    @Override
    public List<ByteBuffer> readBuffers() throws IOException {
        byte[] buffer = readBuffer();
        if (buffer == null) {
            return Collections.emptyList();
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(buffer)) {
            return readBuffers(bais);
        }
    }

    abstract byte[] readBuffer() throws IOException;

    // below copied from HttpTransceiver
    private static List<ByteBuffer> readBuffers(InputStream in) throws IOException {
        List<ByteBuffer> buffers = new ArrayList<>();
        while (true) {
            int length = (in.read() << 24) + (in.read() << 16) + (in.read() << 8) + in.read();
            if (length == 0) { // end of buffers
                return buffers;
            }
            ByteBuffer buffer = ByteBuffer.allocate(length);
            while (buffer.hasRemaining()) {
                int p = buffer.position();
                int i = in.read(buffer.array(), p, buffer.remaining());
                if (i < 0) {
                    throw new EOFException("Unexpected EOF");
                }
                ((Buffer) buffer).position(p + i);
            }
            ((Buffer) buffer).flip();
            buffers.add(buffer);
        }
    }

    private static void writeBuffers(List<ByteBuffer> buffers, OutputStream out) throws IOException {
        for (ByteBuffer buffer : buffers) {
            writeLength(buffer.limit(), out); // length-prefix
            out.write(buffer.array(), buffer.position(), buffer.remaining());
            ((Buffer) buffer).position(buffer.limit());
        }
        writeLength(0, out); // null-terminate
    }

    private static void writeLength(int length, OutputStream out) throws IOException {
        out.write(0xff & (length >>> 24));
        out.write(0xff & (length >>> 16));
        out.write(0xff & (length >>> 8));
        out.write(0xff & length);
    }
}
