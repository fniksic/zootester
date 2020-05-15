package edu.upenn.zktester.scenario;

import edu.upenn.zktester.ensemble.ZKEnsemble;
import edu.upenn.zktester.ensemble.ZKProperty;
import edu.upenn.zktester.harness.Harness;
import edu.upenn.zktester.harness.Phase;
import edu.upenn.zktester.util.Assert;
import edu.upenn.zktester.util.AssertionFailureError;
import edu.upenn.zktester.util.Config;
import edu.upenn.zktester.util.ThrowingFunction;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class BaselineScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(BaselineScenario.class);

    private static final int TOTAL_SERVERS = 3;
    private static final List<Integer> ALL_SERVERS = List.of(0, 1, 2);

    private final Random random = new Random();
    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);
    private final Harness harness;

    public BaselineScenario(final Harness harness) {
        this.harness = harness;
    }

    private Config config;

    @Override
    public void init(Config config) throws IOException {
        this.config = config;
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
        try (final AutoCloseable cleanUp = zkEnsemble::stopEnsemble) {
            zkEnsemble.startEnsemble();

            // We have an initial phase in which we create the znodes
            final int leader = zkEnsemble.getLeader();
            zkEnsemble.handleRequest(leader, harness.getInitRequest());

            final PriorityQueue<Event> events = new PriorityQueue<>(Comparator.comparingLong(Event::getTimestamp));
            final ListIterator<Phase> phaseIterator = harness.getPhases().listIterator();
            final int totalPhases = harness.getPhases().size();

            // We'll try to execute the phases in 5 second intervals, and crashes in random intervals centered
            // around 3 seconds (generated using Poisson distribution).
            enqueueExecutePhase(events, phaseIterator);
            enqueueStartOrStop(events);

            final Map<Integer, Boolean> executedPhases = new ConcurrentHashMap<>();
            final Map<Integer, Boolean> maybeExecutedPhases = new ConcurrentHashMap<>();
            boolean done = false;
            while (!events.isEmpty() && !done) {
                final long pause = events.peek().getTimestamp() - System.currentTimeMillis();
                if (pause > 0) {
                    try {
                        Thread.sleep(pause);
                    } catch (final InterruptedException e) {
                        LOG.warn("Interrupted while waiting for the next event");
                    }
                } else {
                    final Event event = events.remove();
                    done = event.match(
                            executePhase -> {
                                enqueueExecutePhase(events, phaseIterator);
                                executeExecutePhase(executePhase, executedPhases, maybeExecutedPhases);
                                return executePhase.getPhaseIndex() + 1 == totalPhases;
                            },
                            startOrStop -> {
                                enqueueStartOrStop(events);
                                executeStartOrStop(startOrStop);
                                return false;
                            }
                    );
                }
            }

            final List<Integer> serversToStart = ALL_SERVERS.stream()
                    .filter(Predicate.not(zkEnsemble::isRunning))
                    .collect(Collectors.toList());
            zkEnsemble.startServers(serversToStart);
            final ZKProperty property =
                    harness.getConsistencyProperty(executedPhases.keySet(), maybeExecutedPhases.keySet());
            final boolean result = zkEnsemble.checkProperty(property);
            Assert.assertTrue("All servers should be in the same state" +
                    ", and the state should be allowed under sequential consistency", result);
        }
    }

    private ExecutePhase getExecutePhase(final int phaseIndex, final Phase phase) {
        final long timestamp = System.currentTimeMillis() + 5_000;
        return new ExecutePhase(phaseIndex, phase, timestamp);
    }

    private void enqueueExecutePhase(final PriorityQueue<Event> queue, final ListIterator<Phase> phaseIterator) {
        if (phaseIterator.hasNext()) {
            final ExecutePhase executePhase = getExecutePhase(phaseIterator.nextIndex(), phaseIterator.next());
            LOG.info("Enqueueing {}", executePhase);
            queue.add(executePhase);
        }
    }

    private void executeExecutePhase(final ExecutePhase executePhase,
                                     final Map<Integer, Boolean> executedPhases,
                                     final Map<Integer, Boolean> maybeExecutedPhases)
            throws InterruptedException, IOException, KeeperException {
        LOG.info("Executing {}", executePhase);
        final int phaseIndex = executePhase.getPhaseIndex();
        final Phase phase = executePhase.getPhase();
        phase.throwingMatch(
                empty -> null,
                requestPhase -> {
                    if (zkEnsemble.isRunning(requestPhase.getNode())) {
                        zkEnsemble.handleRequest(requestPhase.getNode(),
                                requestPhase.getRequest(
                                        // On success, add to the map of executed phases
                                        () -> {
                                            LOG.info("Phase {} request completed", phaseIndex);
                                            executedPhases.put(phaseIndex, true);
                                        },
                                        // On undetermined result, add to the map of maybe executed phases
                                        () -> {
                                            LOG.warn("Phase {} request undetermined", phaseIndex);
                                            maybeExecutedPhases.put(phaseIndex, true);
                                        })
                        );
                    }
                    return null;
                }
        );
    }

    private StartOrStop getStartOrStop() {
        final int serverId = random.nextInt(TOTAL_SERVERS);
        // Samples a Poisson interval with the parameter lambda = 1/3000
        final long timestamp = System.currentTimeMillis() - (long) (2_000.0 * Math.log(1.0 - random.nextDouble()));
        return new StartOrStop(serverId, timestamp);
    }

    private void enqueueStartOrStop(final PriorityQueue<Event> queue) {
        final StartOrStop startOrStop = getStartOrStop();
        LOG.info("Enqueueing {}", startOrStop);
        queue.add(startOrStop);
    }

    private void executeStartOrStop(final StartOrStop startOrStop) throws InterruptedException {
        LOG.info("Executing {}", startOrStop);
        final int serverId = startOrStop.getServerId();
        if (zkEnsemble.isRunning(serverId)) {
            LOG.info("Stopping {}", serverId);
            zkEnsemble.stopSingle(serverId);
        } else {
            LOG.info("Starting {}", serverId);
            zkEnsemble.startSingle(serverId);
        }
    }

    private interface Event {
        <T> T match(ThrowingFunction<ExecutePhase, T> casePhase,
                    ThrowingFunction<StartOrStop, T> caseStartOrStop) throws InterruptedException, IOException, KeeperException;

        long getTimestamp();
    }

    private static class ExecutePhase implements Event {

        private final int phaseIndex;
        private final Phase phase;
        private final long timestamp;

        public ExecutePhase(final int phaseIndex, final Phase phase, final long timestamp) {
            this.phaseIndex = phaseIndex;
            this.phase = phase;
            this.timestamp = timestamp;
        }

        @Override
        public <T> T match(final ThrowingFunction<ExecutePhase, T> casePhase,
                           final ThrowingFunction<StartOrStop, T> caseStartOrStop)
                throws InterruptedException, IOException, KeeperException {
            return casePhase.apply(this);
        }

        public int getPhaseIndex() {
            return phaseIndex;
        }

        public Phase getPhase() {
            return phase;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "ExecutePhase{" +
                    "phaseIndex=" + phaseIndex +
                    ", phase=" + phase +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    private static class StartOrStop implements Event {

        private final int serverId;
        private final long timestamp;

        public StartOrStop(final int serverId, final long timestamp) {
            this.serverId = serverId;
            this.timestamp = timestamp;
        }

        @Override
        public <T> T match(final ThrowingFunction<ExecutePhase, T> casePhase,
                           final ThrowingFunction<StartOrStop, T> caseStartOrStop)
                throws InterruptedException, IOException, KeeperException {
            return caseStartOrStop.apply(this);
        }

        public int getServerId() {
            return serverId;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "StartOrStop{" +
                    "serverId=" + serverId +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
