package edu.upenn.zootester.scenario;

import edu.upenn.zootester.ensemble.ZKEnsemble;
import edu.upenn.zootester.ensemble.ZKProperty;
import edu.upenn.zootester.harness.ConditionalWritePhase;
import edu.upenn.zootester.harness.EmptyPhase;
import edu.upenn.zootester.harness.Harness;
import edu.upenn.zootester.harness.Phase;
import edu.upenn.zootester.util.Assert;
import edu.upenn.zootester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FailureSC implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(FailureSC.class);
    private static final int TOTAL_SERVERS = 3;

    private final ZKEnsemble zkEnsemble = new ZKEnsemble(TOTAL_SERVERS);

    private final Harness harness = new Harness(List.of(
            new ConditionalWritePhase(1, "/key1", 0, "/key1", 101),
            new EmptyPhase(),
            new ConditionalWritePhase(0, "/key1", 101, "/key0", 200),
            new ConditionalWritePhase(1, "/key1", 0, "/key1", 301)
    ));

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

            final int leader = zkEnsemble.getLeader();
            zkEnsemble.handleRequest(leader, harness.getInitRequest());
            zkEnsemble.stopAllServers();

            final Map<Integer, Boolean> executedPhases = new ConcurrentHashMap<>();
            final Map<Integer, Boolean> maybeExecutedPhases = new ConcurrentHashMap<>();
            final ListIterator<Phase> it = harness.getPhases().listIterator();

            final int phaseIndex1 = it.nextIndex();
            final Phase phase1 = it.next();
            zkEnsemble.startServers(List.of(0, 1));
            zkEnsemble.crashServers(List.of(0));
            phase1.throwingMatch(
                    ignore -> null,
                    requestPhase -> {
                        zkEnsemble.handleRequest(requestPhase.getNode(), requestPhase.getRequest(
                                () -> executedPhases.put(phaseIndex1, true),
                                () -> maybeExecutedPhases.put(phaseIndex1, true),
                                () -> {
                                }));
                        return null;
                    }
            );
            zkEnsemble.stopServers(List.of(0, 1));

            final int phaseIndex2 = it.nextIndex();
            final Phase phase2 = it.next();
            zkEnsemble.startServers(List.of(0, 1));
            zkEnsemble.stopServers(List.of(0, 1));

            final int phaseIndex3 = it.nextIndex();
            final Phase phase3 = it.next();
            zkEnsemble.startServers(List.of(0, 2));
            phase3.throwingMatch(
                    ignore -> null,
                    requestPhase -> {
                        zkEnsemble.handleRequest(requestPhase.getNode(), requestPhase.getRequest(
                                () -> executedPhases.put(phaseIndex3, true),
                                () -> maybeExecutedPhases.put(phaseIndex3, true),
                                () -> {
                                }));
                        return null;
                    }
            );
            zkEnsemble.stopServers(List.of(0, 2));

            final int phaseIndex4 = it.nextIndex();
            final Phase phase4 = it.next();
            zkEnsemble.startServers(List.of(0, 1, 2));
            phase4.throwingMatch(
                    ignore -> null,
                    requestPhase -> {
                        zkEnsemble.handleRequest(requestPhase.getNode(), requestPhase.getRequest(
                                () -> executedPhases.put(phaseIndex4, true),
                                () -> maybeExecutedPhases.put(phaseIndex4, true),
                                () -> {
                                }));
                        return null;
                    }
            );
            zkEnsemble.stopServers(List.of(0, 1, 2));

//            final int phaseIndex5 = it.nextIndex();
//            final Phase phase5 = it.next();
//            zkEnsemble.startServers(List.of(0, 1, 2));
//            zkEnsemble.crashServers(List.of(1, 2));
//            phase5.throwingMatch(
//                    ignore -> null,
//                    requestPhase -> {
//                        zkEnsemble.handleRequest(requestPhase.getNode(), requestPhase.getRequest(
//                                () -> executedPhases.put(phaseIndex5, true),
//                                () -> maybeExecutedPhases.put(phaseIndex5, true),
//                                () -> {
//                                }));
//                        return null;
//                    }
//            );
//            zkEnsemble.stopServers(List.of(0, 1, 2));

            zkEnsemble.startAllServers();
            final ZKProperty property =
                    harness.getConsistencyProperty(executedPhases.keySet(), maybeExecutedPhases.keySet());
            final boolean result = zkEnsemble.checkProperty(property);
            Assert.assertTrue("All servers should be in the same state" +
                    ", and the state should be allowed under sequential consistency", result);
        }
    }
}
