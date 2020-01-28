package edu.upenn.zktester.ensemble;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class ZKHelper {
    public static final int CONNECTION_TIMEOUT = 15_000;

    private static final Path VAR = Path.of("var");
    private static final String TEMP_DIRECTORY_PREFIX = "zknode";

    public static Path createTempDir() throws IOException {
        if (!Files.isDirectory(VAR)) {
            Files.createDirectory(VAR);
        }
        return Files.createTempDirectory(VAR, TEMP_DIRECTORY_PREFIX);
    }

    private static final AtomicInteger nextPort = new AtomicInteger(11221);

    public static int getUniquePort() {
        return nextPort.getAndIncrement();
    }
}
