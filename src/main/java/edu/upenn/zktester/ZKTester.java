package edu.upenn.zktester;

import edu.upenn.zktester.scenario.RandomScenario;
import edu.upenn.zktester.scenario.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ZKTester {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTester.class);

    private static void runRandom(final int numThreads, final int executionsPerThread, final int numPhases,
                                  final int faultBudget, final int requestBudget) {
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; ++i) {
            final Thread thread = new Thread(() -> {
                try {
                    final Scenario scenario = new RandomScenario(executionsPerThread,
                            numPhases, faultBudget, requestBudget);
                    scenario.init();
                    scenario.execute();
                } catch (final Exception e) {
                    LOG.error("Exception while executing scenario", e);
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (final var thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException e) {
                LOG.info("Interrupted from waiting on {}", thread.getName());
            }
        }
    }

    public static void main(final String[] args) {
        runRandom(20, 50, 3, 1, 2);
//        try {
//            final Scenario scenario = new DivergenceResyncScenario();
//            scenario.init();
//            scenario.execute();
//        } catch (final Exception e) {
//            LOG.error("Exception failure", e);
//        } catch (final AssertionFailureError e) {
//            LOG.error("Assertion failure", e);
//        }
    }
}
