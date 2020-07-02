package edu.upenn.zootester.ensemble;

import edu.upenn.zootester.util.Assert;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.quorum.Election;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class QuorumPeerMainWithShutdown extends QuorumPeerMain {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMainWithShutdown.class);

    /***
     * This method is basically copied over from org.apache.zookeeper.test.QuorumBase.shutdown().
     * It shuts down quorumPeer; in particular it joins its thread and waits or it to exit.
     */
    public void shutdown() {
        if (quorumPeer != null) {
            final String quorumPeerName = quorumPeer.getName();

            try {
                LOG.debug("Shutting down quorum peer {}", quorumPeerName);
                quorumPeer.shutdown();

                final Election e = quorumPeer.getElectionAlg();
                if (e != null) {
                    LOG.debug("Shutting down leader election {}", quorumPeerName);
                    e.shutdown();
                } else {
                    LOG.debug("No election available to shutdown {}", quorumPeerName);
                }

                LOG.debug("Waiting for {} to exit thread", quorumPeerName);
                final long readTimeout = quorumPeer.getTickTime() * quorumPeer.getInitLimit();
                final long connectTimeout = quorumPeer.getTickTime() * quorumPeer.getSyncLimit();
                final long maxTimeout = Math.max(Math.max(readTimeout, connectTimeout), ZKHelper.CONNECTION_TIMEOUT);
                quorumPeer.join(maxTimeout * 2);
                if (quorumPeer.isAlive()) {
                    Assert.fail("QP failed to shutdown in " + (maxTimeout * 2) + " milliseconds: " + quorumPeerName);
                }
            } catch (final InterruptedException e) {
                LOG.debug("QP interrupted: {}\n{}", quorumPeerName, e.getMessage());
            }
        }
    }

    public void initializeAndRun(final String[] args) throws IOException, QuorumPeerConfig.ConfigException, AdminServer.AdminServerException {
//    public void initializeAndRun(final String[] args) throws IOException, QuorumPeerConfig.ConfigException {
        super.initializeAndRun(args);
    }

    public Path getTxnFactoryDataDir() {
        return quorumPeer.getTxnFactory().getDataDir().toPath();
    }

    public boolean isLeader() {
        return quorumPeer.leader != null;
    }
}
