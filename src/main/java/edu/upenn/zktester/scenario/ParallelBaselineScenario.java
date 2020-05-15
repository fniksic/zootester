package edu.upenn.zktester.scenario;

import edu.upenn.zktester.ensemble.ZKHelper;
import edu.upenn.zktester.harness.EmptyPhase;
import edu.upenn.zktester.harness.Harness;
import edu.upenn.zktester.harness.UnconditionalWritePhase;
import edu.upenn.zktester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ParallelBaselineScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelBaselineScenario.class);

    // We use a hardcoded list of harnesses that matches the randomly generated harnesses
    // from other tests.
    private final List<Harness> harnesses = List.of(
            new Harness(List.of(
                    new UnconditionalWritePhase(1, "/key0", 101),
                    new EmptyPhase(),
                    new UnconditionalWritePhase(2, "/key1", 302)
            ), 2)
    );

    private Config config;

    @Override
    public void init(final Config config) {
        this.config = config;
    }

    @Override
    public void execute() {
        for (final var harness : harnesses) {
            LOG.info("Starting execution with {}", harness.toString());

            // Reset unique port counter to reuse the port numbers over executions
            ZKHelper.setBasePort(config.getBasePort());

            final List<Thread> threads = new ArrayList<>();
            for (int j = 0; j < config.getThreads(); ++j) {
                final Thread thread = new Thread(() -> {
                    try {
                        final Scenario scenario = new BaselineScenario(harness);
                        scenario.init(config);
                        scenario.execute();
                    } catch (final Exception e) {
                        LOG.error("Exception while executing scenario", e);
                    }
                });
                threads.add(thread);
                thread.start();
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
}
