package edu.upenn.zootester.ensemble;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class ZKHelper {
    public static final int CONNECTION_TIMEOUT = 60_000;

    private static final Path VAR = Path.of("var");
    private static final String TEMP_DIRECTORY_PREFIX = "zknode";

    public static Path createTempDir() throws IOException {
        if (!Files.isDirectory(VAR)) {
            Files.createDirectory(VAR);
        }
        return Files.createTempDirectory(VAR, TEMP_DIRECTORY_PREFIX);
    }

    private static AtomicInteger nextPort = new AtomicInteger(11221);

    public static void setBasePort(final int basePort) {
        nextPort = new AtomicInteger(basePort);
    }

    public static int getUniquePort() {
        return nextPort.getAndIncrement();
    }
}
