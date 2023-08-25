package io.delta.sharing.client.fs;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class InMemoryHttpInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
    public InMemoryHttpInputStream(URI uri) throws IOException {
        super(IOUtils.toByteArray(uri));
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        this.pos = (int) pos;
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        // We don't support this feature
        return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        long oldPos = getPos();
        int nread = -1;
        try {
            seek(position);
            nread = read(buffer, offset, length);
        } finally {
            seek(oldPos);
        }
        return nread;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        int nread = 0;
        while (nread < length) {
            int nbytes = read(position + nread, buffer, offset + nread, length - nread);
            if (nbytes < 0) {
                throw new EOFException("End of file reached before reading fully.");
            }
            nread += nbytes;
        }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }
}
