package io.delta.sharing.client.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class DeltaSharingFileSystem extends FileSystem {
    private static final String SCHEME = "delta-sharing";
    private static final ConcurrentHashMap<String, String> pathToUrl = new ConcurrentHashMap<>();

    public DeltaSharingFileSystem() {
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public URI getUri() {
        return URI.create(SCHEME + ":///");
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        String url = pathToUrl.get(path.toString());
        try {
            // `InMemoryHttpInputStream` loads the content into the memory immediately
            return new FSDataInputStream(new InMemoryHttpInputStream(new URI(url)));
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid url: " + url);
        }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException("create");
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException("append");
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        throw new UnsupportedOperationException("rename");
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        throw new UnsupportedOperationException("delete");
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("listStatus");
    }

    @Override
    public void setWorkingDirectory(Path path) {
        throw new UnsupportedOperationException("setWorkingDirectory");
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(getUri());
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        throw new UnsupportedOperationException("mkdirs");
    }

    // TODO: Is this method used by `Scan`? If so where can we get the file size?
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        throw new UnsupportedOperationException("getFileStatus");
    }

    // Converts pre-signed url to the following path and stores path to url mapping locally.
    // Path format: "delta-sharing:///<table path>/<file id>"
    // File id is the md5 hash of the pre-signed url.
    // TODO: Use this function to replace the pre-signed url with the custom path
    //       when parsing JSON response from Delta Sharing server
    public static String preSignedUrlToPath(String tablePath, String preSignedUrl) {
        String id = DigestUtils.md5Hex(preSignedUrl);
        String path = SCHEME + ":///" + tablePath + "/" + id;
        pathToUrl.put(path, preSignedUrl);
        return path;
    }
}
