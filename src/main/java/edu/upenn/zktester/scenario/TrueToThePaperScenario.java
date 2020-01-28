package edu.upenn.zktester.scenario;

import edu.upenn.zktester.ensemble.ConsistentValues;
import edu.upenn.zktester.ensemble.ZKEnsemble;
import edu.upenn.zktester.ensemble.ZKProperty;
import edu.upenn.zktester.fault.AtMostFaultGenerator;
import edu.upenn.zktester.fault.FaultGenerator;
import edu.upenn.zktester.subset.RandomSubsetGenerator;
import edu.upenn.zktester.util.Assert;
import edu.upenn.zktester.util.AssertionFailureError;
import edu.upenn.zktester.util.Config;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TrueToThePaperScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(TrueToThePaperScenario.class);

    private static final int TOTAL_SERVERS = 3;
    private static final int QUORUM = 2;
    private static final List<Integer> ALL_SERVERS = List.of(0, 1, 2);
    private static final List<String> KEYS = List.of("/key0", "/key1");
    private static final ZKProperty CONSISTENT_VALUES = new ConsistentValues(KEYS);

    private final Random random = new Random();
    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);

    private Config config;
    private RandomSubsetGenerator subsetGenerator;
    private FaultGenerator faultGenerator;

    @Override
    public void init(Config config) throws IOException {
        this.config = config;
        this.subsetGenerator = new RandomSubsetGenerator(random);

        // In this scenario we allow all nodes to be crashed in a phase
        this.faultGenerator = new AtMostFaultGenerator(config.getPhases(), TOTAL_SERVERS, config.getFaults(), random);

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

        LOG.info("Finished executions: \tFailed assertions: {}\tFailed otherwise: {}\tTotal: {}",
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
                    zk.create(key, Integer.toString(leader).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    LOG.info("Initial association: {} -> {}", key, leader);
                }
            });
            zkEnsemble.stopAllServers();

            for (int phase = 1; phase <= config.getPhases(); ++phase) {
                final int faults = faultGenerator.generate();
                final List<Integer> serversToCrash = subsetGenerator.generate(TOTAL_SERVERS, faults);
                final List<Integer> serversToStart;

                if (phaseHasRequest(phase)) {
                    final List<Integer> serversToCrashLater = subsetGenerator.generate(faults).stream()
                            .map(i -> serversToCrash.get(i)).collect(Collectors.toList());
                    serversToStart = ALL_SERVERS.stream()
                            .filter(i -> !serversToCrash.contains(i) || serversToCrashLater.contains(i))
                            .collect(Collectors.toList());
                    if (serversToStart.size() < QUORUM) {
                        // The phase will be stuck with the servers trying to elect a leader, so we skip it
                        continue;
                    }
                    zkEnsemble.startServers(serversToStart);
                    zkEnsemble.crashServers(serversToCrashLater);

                    // We always make a client request to server 2
                    final int id = 2;
                    if (!serversToCrash.contains(id)) {
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
                } else {
                    serversToStart = ALL_SERVERS.stream()
                            .filter(i -> !serversToCrash.contains(i))
                            .collect(Collectors.toList());
                    if (serversToStart.size() < QUORUM) {
                        // The phase will be stuck with the servers trying to elect a leader
                        continue;
                    }
                    zkEnsemble.startServers(serversToStart);
                }
                zkEnsemble.stopServers(serversToStart);
            }

            zkEnsemble.startAllServers();
            final boolean result = zkEnsemble.checkProperty(CONSISTENT_VALUES);
            Assert.assertTrue("All keys on all servers should have the same value", result);
        }
    }

    private boolean phaseHasRequest(final int phase) {
        return phase == 1 || phase == 3;
    }
}
