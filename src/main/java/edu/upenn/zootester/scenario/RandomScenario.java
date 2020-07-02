package edu.upenn.zootester.scenario;

import edu.upenn.zootester.ensemble.ZKEnsemble;
import edu.upenn.zootester.ensemble.ZKProperty;
import edu.upenn.zootester.fault.ExactFaultGenerator;
import edu.upenn.zootester.fault.FaultGenerator;
import edu.upenn.zootester.harness.EmptyPhase;
import edu.upenn.zootester.harness.Harness;
import edu.upenn.zootester.harness.Phase;
import edu.upenn.zootester.harness.UnconditionalWritePhase;
import edu.upenn.zootester.subset.MinimalQuorumGenerator;
import edu.upenn.zootester.subset.RandomSubsetGenerator;
import edu.upenn.zootester.util.Assert;
import edu.upenn.zootester.util.AssertionFailureError;
import edu.upenn.zootester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RandomScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(RandomScenario.class);

    private static final int TOTAL_SERVERS = 3;
    private static final int QUORUM_SIZE = 2;

    private final Random random = new Random();
    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);
    private final Harness harness;

    private Config config;
    private MinimalQuorumGenerator quorumGenerator;
    private RandomSubsetGenerator subsetGenerator;
    private FaultGenerator faultGenerator;

    public RandomScenario(final Harness harness) {
        this.harness = harness;
    }

    public RandomScenario() {
        // TODO: Not ideal. In this scenario it is really desirable to issue requests
        //       to nodes that are running. The probability that node 1 is running in
        //       the first phase and node 2 is running in the third phase, provided
        //       that there is 1 additional fault, is 8/27 = 29%
        this(new Harness(List.of(
                new UnconditionalWritePhase(1, "/key0", 101),
                new EmptyPhase(),
                new UnconditionalWritePhase(2, "/key1", 302)
        )));
    }

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

        LOG.info("Finished executions:\n\tFailed assertions: {}\tFailed otherwise: {}\tTotal: {}",
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

            final Map<Integer, Boolean> executedPhases = new ConcurrentHashMap<>();
            final Map<Integer, Boolean> maybeExecutedPhases = new ConcurrentHashMap<>();
            final ListIterator<Phase> it = harness.getPhases().listIterator();
            while (it.hasNext()) {
                final int phaseIndex = it.nextIndex();
                final Phase phase = it.next();
                final List<Integer> serversToStart = quorumGenerator.generate();
                zkEnsemble.startServers(serversToStart);

                final int faults = faultGenerator.generate();
                final List<Integer> serversToCrash = subsetGenerator.generate(serversToStart.size(), faults).stream()
                        .map(serversToStart::get).collect(Collectors.toList());
                zkEnsemble.crashServers(serversToCrash);
                final List<Integer> serversStillRunning = serversToStart.stream()
                        .filter(i -> !serversToCrash.contains(i)).collect(Collectors.toList());

                phase.throwingMatch(
                        empty -> null,
                        request -> {
                            if (serversStillRunning.contains(request.getNode())) {
                                LOG.info("Initiating request for {}", request);
                                zkEnsemble.handleRequest(request.getNode(), request.getRequest(
                                        // If successful, add to the map of executed phases
                                        () -> {
                                            LOG.info("Phase {} request completed", phaseIndex);
                                            executedPhases.put(phaseIndex, true);
                                        },
                                        // On undetermined result, add to the map of maybe executed phases
                                        () -> {
                                            LOG.info("Phase {} request undetermined", phaseIndex);
                                            maybeExecutedPhases.put(phaseIndex, true);
                                        },
                                        // No cleanup here
                                        () -> {
                                        }
                                ));
                            }
                            return null;
                        }
                );
                zkEnsemble.stopServers(serversToStart);
            }

            zkEnsemble.startAllServers();
            final ZKProperty property =
                    harness.getConsistencyProperty(executedPhases.keySet(), maybeExecutedPhases.keySet());
            final boolean result = zkEnsemble.checkProperty(property);
            Assert.assertTrue("All servers should be in the same state" +
                    ", and the state should be allowed under sequential consistency", result);
        }
    }
}
