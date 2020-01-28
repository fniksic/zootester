package edu.upenn.zktester;

import edu.upenn.zktester.scenario.RandomScenario;
import edu.upenn.zktester.scenario.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ZKTester {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTester.class);

    public static void main(final String[] args) {
//        final Scenario scenario = new DivergenceResyncScenario();
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 16; ++i) {
            final Thread thread = new Thread(() -> {
                try {
                    final Scenario scenario = new RandomScenario(1000, 3, 1, 2);
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
}
