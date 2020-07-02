package edu.upenn.zootester.scenario;

import edu.upenn.zootester.ensemble.ZKHelper;
import edu.upenn.zootester.harness.EmptyPhase;
import edu.upenn.zootester.harness.Harness;
import edu.upenn.zootester.harness.UnconditionalWritePhase;
import edu.upenn.zootester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ParallelBaselineScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelBaselineScenario.class);

    private final List<Harness> harnesses;

    public ParallelBaselineScenario(final List<Harness> harness) {
        this.harnesses = harness;
    }

    public ParallelBaselineScenario() {
        // We use a hardcoded list of harnesses that matches the randomly generated harnesses
        // from other tests.
        this(List.of(
                new Harness(List.of(
                        new UnconditionalWritePhase(1, "/key0", 101),
                        new EmptyPhase(),
                        new UnconditionalWritePhase(2, "/key1", 302)
                ))
        ));
    }

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
                    LOG.warn("Interrupted from waiting on {}", thread.getName());
                }
            }
        }
    }
}
