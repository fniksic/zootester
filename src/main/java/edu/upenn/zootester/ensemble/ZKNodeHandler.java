package edu.upenn.zootester.ensemble;

import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class ZKNodeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ZKNodeHandler.class);

    private final int myId;
    private final Path tempDir;
    private final Path confFile;
    private final Path dataDir;

    private volatile QuorumPeerMainWithShutdown quorumPeerMain;
    private Thread currentThread;
    private boolean running = false;

    public ZKNodeHandler(final int myId, final int clientPort, final String quorumCfgSection) throws IOException {
        this.myId = myId;
        tempDir = ZKHelper.createTempDir();
        LOG.info("id = {} tempDir = {} clientPort = {}", myId, tempDir.toString(), clientPort);

        confFile = Files.createFile(Paths.get(tempDir.toString(), "zoo.cfg"));
        dataDir = Files.createDirectory(Paths.get(tempDir.toString(), "data"));

        try (final BufferedWriter writer = Files.newBufferedWriter(confFile)) {
            writer.write("tickTime=2000\n");
            writer.write("initLimit=5\n");
            writer.write("syncLimit=3\n");
            writer.write("dataDir=" + dataDir.toString() + "\n");
            writer.write("clientPort=" + clientPort + "\n");
            writer.write(quorumCfgSection + "\n");
            writer.flush();
        }
        Files.writeString(Paths.get(dataDir.toString(), "myid"), Integer.toString(myId));
    }

    public void start() {
        LOG.debug("Starting new QuorumPeerMain with id={}", myId);
        quorumPeerMain = new QuorumPeerMainWithShutdown();
        currentThread = new Thread(() -> {
            try {
                quorumPeerMain.initializeAndRun(new String[]{confFile.toString()});
            } catch (final IOException | QuorumPeerConfig.ConfigException | AdminServer.AdminServerException e) {
//            } catch (final IOException | QuorumPeerConfig.ConfigException e) {
                LOG.error("Unexpected exception in currentThread", e);
                quorumPeerMain.shutdown();
            } finally {
                currentThread = null;
            }
        });
        currentThread.start();
        running = true;
    }

    public void shutdown() throws InterruptedException {
        LOG.debug("Shutting down QuorumPeerMain with id={}", myId);
        final Thread t = currentThread;
        if (t != null && t.isAlive()) {
            quorumPeerMain.shutdown();
            t.join(500);
            if (t.isAlive()) {
                LOG.error("Failed to join QuorumPeerMain's thread after 500 ms");
            }
        }
        running = false;
    }

    public boolean isAlive() {
        final Thread t = currentThread;
        return t != null && t.isAlive();
    }

    public boolean isRunning() {
        return running;
    }

    private void deleteDir(final Path dir) throws IOException {
        LOG.info("Deleting {}", dir);
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    public void clean() throws IOException {
        deleteDir(quorumPeerMain.getTxnFactoryDataDir());
    }

    public void tearDown() throws IOException {
        deleteDir(tempDir);
    }

    public boolean isLeader() {
        final Thread t = currentThread;
        return t != null && t.isAlive() && quorumPeerMain.isLeader();
    }
}
