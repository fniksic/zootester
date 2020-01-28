package edu.upenn.zktester.scenario;

import edu.upenn.zktester.ensemble.ZKEnsemble;
import edu.upenn.zktester.util.Assert;
import edu.upenn.zktester.util.Config;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DivergenceResyncScenario2 implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(DivergenceResyncScenario2.class);

    private static final int TOTAL_SERVERS = 3;

    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);

    @Override
    public void init(final Config config) throws IOException {
        zkEnsemble.init();
    }

    @Override
    public void execute() throws Exception {
        try (final AutoCloseable cleanUp = zkEnsemble::stopEnsemble) {
            zkEnsemble.startEnsemble();

            final int srvC = zkEnsemble.getLeader();
            final int srvA = (srvC + 1) % TOTAL_SERVERS;
            final int srvB = (srvC + 2) % TOTAL_SERVERS;

            Assert.assertTrue("There should be a leader", srvC >= 0);

            final String path = "/testDivergenceResync";
            final List<String> keys = List.of(path + 0, path + 1);

            // Create initial znodes
            zkEnsemble.handleRequest(srvC, zk -> {
                zk.create(keys.get(0), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.create(keys.get(1), "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            });
            zkEnsemble.stopAllServers();

            // Resync A and B
            zkEnsemble.startServers(List.of(srvA, srvB));
            Assert.assertTrue("Server B should be the leader", zkEnsemble.getLeader() == srvB);

            // Divergence
            zkEnsemble.crashServers(List.of(srvA));
            zkEnsemble.handleRequest(srvB, zk -> {
                zk.setData(keys.get(0), "1000".getBytes(), -1, null, null);
                Thread.sleep(500);
                System.gc();
            });
            zkEnsemble.stopServers(List.of(srvA, srvB));

            // Start and stop B and C
            zkEnsemble.startServers(List.of(srvB, srvC));
            Assert.assertTrue("Server B should be the leader", zkEnsemble.getLeader() == srvB);
            zkEnsemble.stopServers(List.of(srvB, srvC));

            // Resync A and C
            zkEnsemble.startServers(List.of(srvA, srvC));
            Assert.assertTrue("Server C should be the leader", zkEnsemble.getLeader() == srvC);

            // Divergence
            zkEnsemble.handleRequest(srvA, zk -> {
                zk.setData(keys.get(1), "1001".getBytes(), -1, null, null);
                Thread.sleep(500);
                System.gc();
            });
            zkEnsemble.stopServers(List.of(srvA, srvC));

            // Resync B and C
            zkEnsemble.startServers(List.of(srvA, srvC));
            Assert.assertTrue("Server C should be the leader", zkEnsemble.getLeader() == srvC);

            // Start A
            zkEnsemble.startServers(List.of(srvB));
            Assert.assertTrue("Server C should be the leader", zkEnsemble.getLeader() == srvC);

            final boolean result = checkProperty(keys);
            Assert.assertTrue("All keys on all servers should have the same value", result);
        }
    }

    /***
     * Checks that all servers have all keys mapped to the same values
     *
     * @param keys
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    private boolean checkProperty(final List<String> keys) throws KeeperException, InterruptedException {
        return zkEnsemble.checkProperty(zookeepers -> {
            boolean result = true;
            for (final var key : keys) {
                final ZooKeeper zk0 = zookeepers.get(0);
                final byte[] rawValue0 = zk0.getData(key, false, null);
                LOG.info("{}\n\tAssociation: {} -> {}", zk0.toString(), key, new String(rawValue0));

                final boolean valueOK = zookeepers.subList(1, zookeepers.size()).stream()
                        .allMatch(zk -> {
                            try {
                                final byte[] rawValue = zk.getData(key, false, null);
                                LOG.info("{}\n\tAssociation: {} -> {}", zk.toString(), key, new String(rawValue));
                                return Arrays.equals(rawValue0, rawValue);
                            } catch (final KeeperException | InterruptedException e) {
                                return false;
                            }
                        });
                result = result && valueOK;
            }
            return result;
        });
    }
}
