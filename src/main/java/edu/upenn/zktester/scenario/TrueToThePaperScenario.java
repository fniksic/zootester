package edu.upenn.zktester.scenario;

import edu.upenn.zktester.ensemble.ConsistentValues;
import edu.upenn.zktester.ensemble.ZKEnsemble;
import edu.upenn.zktester.ensemble.ZKProperty;
import edu.upenn.zktester.fault.AtMostFaultGenerator;
import edu.upenn.zktester.fault.FaultGenerator;
import edu.upenn.zktester.harness.EmptyPhase;
import edu.upenn.zktester.harness.Harness;
import edu.upenn.zktester.harness.UnconditionalWritePhase;
import edu.upenn.zktester.subset.RandomSubsetGenerator;
import edu.upenn.zktester.util.Assert;
import edu.upenn.zktester.util.AssertionFailureError;
import edu.upenn.zktester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
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
    private Harness harness;

    @Override
    public void init(Config config) throws IOException {
        this.config = config;
        this.subsetGenerator = new RandomSubsetGenerator(random);

        // In this scenario we allow all nodes to be crashed in a phase
        this.faultGenerator = new AtMostFaultGenerator(config.getPhases(), TOTAL_SERVERS, config.getFaults(), random);

        // We have a fixed harness in this scenario
        this.harness = new Harness(List.of(
                new UnconditionalWritePhase(2, "/key0", 102),
                new EmptyPhase(),
                new UnconditionalWritePhase(2, "/key1", 302)
        ), 2);

        zkEnsemble.init();
    }

    @Override
    public void execute() throws Exception {
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
        zkEnsemble.tearDown();
    }

    private void singleExecution() throws Exception {
        try (final AutoCloseable cleanUp = () -> {
            zkEnsemble.stopEnsemble();
            faultGenerator.reset();
        }) {
            zkEnsemble.startEnsemble();

            // We have an initial phase in which we create the znodes
            final int leader = zkEnsemble.getLeader();
            zkEnsemble.handleRequest(leader, harness.getInitRequest());
            zkEnsemble.stopAllServers();

            for (final var phase : harness.getPhases()) {
                final int faults = faultGenerator.generate();
                final List<Integer> serversToCrash = subsetGenerator.generate(TOTAL_SERVERS, faults);
                final List<Integer> serversToStop = phase.throwingMatch(
                        emptyPhase -> {
                            final List<Integer> serversToStart = ALL_SERVERS.stream()
                                    .filter(i -> !serversToCrash.contains(i))
                                    .collect(Collectors.toList());
                            if (serversToStart.size() < QUORUM) {
                                // The phase will be stuck with the servers trying to elect a leader,
                                // so we skip it by not starting/stopping any servers.
                                return Collections.emptyList();
                            }
                            zkEnsemble.startServers(serversToStart);
                            return serversToStart;
                        },
                        requestPhase -> {
                            final List<Integer> serversToCrashLater = subsetGenerator.generate(faults).stream()
                                    .map(i -> serversToCrash.get(i)).collect(Collectors.toList());
                            final List<Integer> serversToStart = ALL_SERVERS.stream()
                                    .filter(i -> !serversToCrash.contains(i) || serversToCrashLater.contains(i))
                                    .collect(Collectors.toList());
                            if (serversToStart.size() < QUORUM) {
                                // The phase will be stuck with the servers trying to elect a leader,
                                // so we skip it by not starting/stopping any servers.
                                return Collections.emptyList();
                            }
                            zkEnsemble.startServers(serversToStart);
                            zkEnsemble.crashServers(serversToCrashLater);

                            if (!serversToCrash.contains(requestPhase.getNode())) {
                                LOG.info("Initiating request for {}", requestPhase);
                                zkEnsemble.handleRequest(requestPhase.getNode(), requestPhase.getRequest());
                            }
                            return serversToStart;
                        }
                );
                zkEnsemble.stopServers(serversToStop);
            }

            zkEnsemble.startAllServers();
            final boolean result = zkEnsemble.checkProperty(CONSISTENT_VALUES);
            Assert.assertTrue("All keys on all servers should have the same value", result);
        }
    }
}
