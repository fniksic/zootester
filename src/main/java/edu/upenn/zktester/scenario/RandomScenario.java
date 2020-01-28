package edu.upenn.zktester.scenario;

import edu.upenn.zktester.ensemble.ConsistentValues;
import edu.upenn.zktester.ensemble.ZKEnsemble;
import edu.upenn.zktester.ensemble.ZKProperty;
import edu.upenn.zktester.fault.ExactFaultGenerator;
import edu.upenn.zktester.fault.FaultGenerator;
import edu.upenn.zktester.subset.MinimalQuorumGenerator;
import edu.upenn.zktester.subset.RandomSubsetGenerator;
import edu.upenn.zktester.util.Assert;
import edu.upenn.zktester.util.AssertionFailureError;
import edu.upenn.zktester.util.Config;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class RandomScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(RandomScenario.class);

    private static final int TOTAL_SERVERS = 3;
    private static final int QUORUM_SIZE = 2;
    private static final List<String> KEYS = List.of("/key0", "/key1");
    private static final ZKProperty CONSISTENT_VALUES = new ConsistentValues(KEYS);

    private final Random random = new Random();
    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);

    private Config config;
    private MinimalQuorumGenerator quorumGenerator;
    private RandomSubsetGenerator subsetGenerator;
    private FaultGenerator faultGenerator;

    @Override
    public void init(final Config config) throws IOException {
        this.config = config;
        this.quorumGenerator = new MinimalQuorumGenerator(TOTAL_SERVERS, random);
        this.subsetGenerator = new RandomSubsetGenerator(random);
        this.faultGenerator = new ExactFaultGenerator(config.getPhases(), QUORUM_SIZE - 1,
                config.getFaults(), random);
        zkEnsemble.init();
    }

    @Override
    public void execute() {
        int failedAssertions = 0;
        int failedOtherwise = 0;

        for (int i = 1; i <= config.getExecutions(); ++i) {
            final long seed;
            if (config.hasSeed()) {
                seed = config.getSeed();
            } else {
                seed = random.nextLong();
            }
            random.setSeed(seed);
            LOG.info("Starting execution {}: seed = {}", i, seed);
            try {
                singleExecution();
            } catch (final Exception e) {
                LOG.error("Exception while executing scenario", e);
                ++failedOtherwise;
            } catch (final AssertionFailureError e) {
                LOG.error("Assertion failed", e);
                ++failedAssertions;
            }
            LOG.info("Finished execution {}: seed = {}", i, seed);
        }

        LOG.info("Finished executions:\n\tFailed assertions: {}\tFailed otherwise: {}\tTotal: {}",
                failedAssertions, failedOtherwise, config.getExecutions());
    }

    private void singleExecution() throws Exception {
        try (final AutoCloseable cleanUp = () -> {
            zkEnsemble.stopEnsemble();
            faultGenerator.reset();
        }) {
            zkEnsemble.startEnsemble();

            // We have an initial phase in which we create the znodes
            final int leader = zkEnsemble.getLeader();
            zkEnsemble.handleRequest(leader, zk -> {
                for (final var key : KEYS) {
                    zk.create(key, Integer.toString(leader).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOG.info("Initial association: {} -> {}", key, leader);
                }
            });
            zkEnsemble.stopAllServers();

            for (int phase = 1; phase <= config.getPhases(); ++phase) {
                final List<Integer> serversToStart = quorumGenerator.generate();
                zkEnsemble.startServers(serversToStart);

                final int faults = faultGenerator.generate();
                final List<Integer> serversToCrash = subsetGenerator.generate(serversToStart.size(), faults).stream()
                        .map(i -> serversToStart.get(i)).collect(Collectors.toList());
                zkEnsemble.crashServers(serversToCrash);

                // Make a client request in phases 1 and 3
                if (phase == 1 || phase == 3) {
                    // In this scenario the client to make request to is chosen at random among the nodes
                    // that are up.
                    final List<Integer> serversStillRunning = serversToStart.stream()
                            .filter(i -> !serversToCrash.contains(i)).collect(Collectors.toList());
                    final int id = serversStillRunning.get(random.nextInt(serversStillRunning.size()));

                    // We want to make requests on different keys in the two phases
                    final String key = KEYS.get(phase / 2);
                    final int value = 100 * phase + id;
                    final byte[] rawValue = Integer.toString(value).getBytes();
                    LOG.info("Initiating request to {}: set {} -> {}", id, key, value);
                    zkEnsemble.handleRequest(id, zk -> {
                        zk.setData(key, rawValue, -1, null, null);
                        Thread.sleep(500);
                        System.gc();
                    });
                }

                zkEnsemble.stopServers(serversToStart);
            }

            zkEnsemble.startAllServers();
            final boolean result = zkEnsemble.checkProperty(CONSISTENT_VALUES);
            Assert.assertTrue("All keys on all servers should have the same value", result);
        }
    }
}
