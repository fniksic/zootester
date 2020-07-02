package edu.upenn.zootester.scenario;

import edu.upenn.zootester.ensemble.ZKEnsemble;
import edu.upenn.zootester.ensemble.ZKProperty;
import edu.upenn.zootester.fault.ExactFaultGenerator;
import edu.upenn.zootester.fault.FaultGenerator;
import edu.upenn.zootester.harness.EmptyPhase;
import edu.upenn.zootester.harness.Harness;
import edu.upenn.zootester.harness.Phase;
import edu.upenn.zootester.harness.UnconditionalWritePhase;
import edu.upenn.zootester.subset.RandomSubsetGenerator;
import edu.upenn.zootester.util.Assert;
import edu.upenn.zootester.util.AssertionFailureError;
import edu.upenn.zootester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TrueToThePaperScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(TrueToThePaperScenario.class);

    private static final int TOTAL_SERVERS = 3;
    private static final int QUORUM = 2;
    private static final List<Integer> ALL_SERVERS = List.of(0, 1, 2);

    private final Random random = new Random();
    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);
    private final Harness harness;

    private Config config;
    private RandomSubsetGenerator subsetGenerator;
    private FaultGenerator faultGenerator;

    public TrueToThePaperScenario(final Harness harness) {
        this.harness = harness;
    }

    public TrueToThePaperScenario() {
        this(new Harness(List.of(
                new UnconditionalWritePhase(1, "/key0", 101),
                new EmptyPhase(),
                new UnconditionalWritePhase(2, "/key1", 302)
        )));
    }

    @Override
    public void init(Config config) throws IOException {
        this.config = config;
        this.subsetGenerator = new RandomSubsetGenerator(random);

        // In this scenario we allow all nodes to be crashed in a phase
        this.faultGenerator = new ExactFaultGenerator(config.getPhases(), TOTAL_SERVERS, config.getFaults(), random);

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

            final Map<Integer, Boolean> executedPhases = new ConcurrentHashMap<>();
            final Map<Integer, Boolean> maybeExecutedPhases = new ConcurrentHashMap<>();
            final ListIterator<Phase> it = harness.getPhases().listIterator();
            while (it.hasNext()) {
                final int phaseIndex = it.nextIndex();
                final Phase phase = it.next();
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
                                    .map(serversToCrash::get).collect(Collectors.toList());
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
                                zkEnsemble.handleRequest(requestPhase.getNode(), requestPhase.getRequest(
                                        // On success, add to the map of executed phases
                                        () -> {
                                            LOG.info("Phase {} request completed", phaseIndex);
                                            executedPhases.put(phaseIndex, true);
                                        },
                                        // On undetermined result, add to the map of maybe executed phases
                                        () -> {
                                            LOG.warn("Phase {} request undetermined", phaseIndex);
                                            maybeExecutedPhases.put(phaseIndex, true);
                                        },
                                        // No cleanup here
                                        () -> {
                                        }
                                ));
                            }
                            return serversToStart;
                        }
                );
                zkEnsemble.stopServers(serversToStop);
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
