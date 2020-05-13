package edu.upenn.zktester.scenario;

import edu.upenn.zktester.ensemble.ConsistentValues;
import edu.upenn.zktester.ensemble.ZKEnsemble;
import edu.upenn.zktester.ensemble.ZKProperty;
import edu.upenn.zktester.util.Assert;
import edu.upenn.zktester.util.Config;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DivergenceResyncScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(DivergenceResyncScenario.class);

    private static final int TOTAL_SERVERS = 3;
    private static final List<String> KEYS = List.of("/testDivergenceResync0", "/testDivergenceResync1");
    private static final ZKProperty CONSISTENT_VALUES = new ConsistentValues(KEYS);

    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);

    @Override
    public void init(final Config config) throws IOException {
        zkEnsemble.init();
    }

    @Override
    public void execute() throws Exception {
        try (final AutoCloseable cleanUp = () -> {
            zkEnsemble.stopEnsemble();
            zkEnsemble.tearDown();
        }) {
            zkEnsemble.startEnsemble();

            final int srvC = zkEnsemble.getLeader();
            final int srvA = (srvC + 1) % TOTAL_SERVERS;
            final int srvB = (srvC + 2) % TOTAL_SERVERS;

            Assert.assertTrue("There should be a leader", srvC >= 0);

            // Create initial znodes
            zkEnsemble.handleRequest(srvC, (zk, serverId) -> {
                zk.create(KEYS.get(0), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.create(KEYS.get(1), "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            });
            zkEnsemble.stopAllServers();

            // Resync A and B
            zkEnsemble.startServers(List.of(srvA, srvB));
            Assert.assertTrue("Server B should be the leader", zkEnsemble.getLeader() == srvB);

            // Divergence
            zkEnsemble.crashServers(List.of(srvA));
            zkEnsemble.handleRequest(srvB, (zk, serverId) -> {
                zk.setData(KEYS.get(0), "1000".getBytes(), -1, null, null);
                Thread.sleep(500);
//                System.gc();
            });
            zkEnsemble.stopServers(List.of(srvA, srvB));

            // Start and stop A and C
            zkEnsemble.startServers(List.of(srvA, srvC));
            Assert.assertTrue("Server A should be the leader", zkEnsemble.getLeader() == srvA);
            zkEnsemble.stopServers(List.of(srvA, srvC));

            // Resync B and C
            zkEnsemble.startServers(List.of(srvB, srvC));
            Assert.assertTrue("Server C should be the leader", zkEnsemble.getLeader() == srvC);

            // Divergence
            zkEnsemble.handleRequest(srvC, (zk, serverId) -> {
                zk.setData(KEYS.get(1), "1001".getBytes(), -1, null, null);
                Thread.sleep(500);
//                System.gc();
            });
            zkEnsemble.stopServers(List.of(srvB, srvC));

            // Resync B and C
            zkEnsemble.startServers(List.of(srvB, srvC));
            Assert.assertTrue("Server C should be the leader", zkEnsemble.getLeader() == srvC);

            // Start A
            zkEnsemble.startServers(List.of(srvA));
            Assert.assertTrue("Server C should be the leader", zkEnsemble.getLeader() == srvC);

            final boolean result = zkEnsemble.checkProperty(CONSISTENT_VALUES);
            Assert.assertTrue("All keys on all servers should have the same value", result);
        }
    }
}
