package edu.upenn.zootester.scenario;

import edu.upenn.zootester.ensemble.ZKHelper;
import edu.upenn.zootester.harness.Harness;
import edu.upenn.zootester.harness.RandomHarnessGenerator;
import edu.upenn.zootester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomHarnessScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(RandomHarnessScenario.class);

    private final Random random = new Random();

    private Config config;
    private RandomHarnessGenerator harnessGenerator;

    @Override
    public void init(Config config) {
        this.config = config;

        final long seed;
        if (config.hasSeed()) {
            seed = config.getSeed();
        } else {
            seed = random.nextLong();
        }
        random.setSeed(seed);
        LOG.info("Initialized random number generator: seed = {}", seed);

        harnessGenerator = new RandomHarnessGenerator(2, 3, config.getRequests(), config.getPhases(), random);
    }

    @Override
    public void execute() {
        for (int i = 0; i < config.getHarnesses(); ++i) {
            final Harness harness = harnessGenerator.next();
            LOG.info("Starting execution with {}", harness.toString());

            // Reset unique port counter to reuse the port numbers over executions
            ZKHelper.setBasePort(config.getBasePort());

            final List<Thread> threads = new ArrayList<>();
            for (int j = 0; j < config.getThreads(); ++j) {
                final Thread thread = new Thread(() -> {
                    try {
                        final Scenario scenario = new TrueToThePaperScenario(harness);
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
