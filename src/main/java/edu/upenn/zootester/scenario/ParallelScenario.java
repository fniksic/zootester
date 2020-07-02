package edu.upenn.zootester.scenario;

import edu.upenn.zootester.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ParallelScenario implements Scenario {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelScenario.class);

    private Config config;

    @Override
    public void init(final Config config) {
        this.config = config;
    }

    @Override
    public void execute() {
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.getThreads(); ++i) {
            final Thread thread = new Thread(() -> {
                try {
                    final Scenario scenario;
                    switch (config.getScenario()) {
                        case "random":
                            scenario = new RandomScenario();
                            break;
                        case "paper":
                            scenario = new TrueToThePaperScenario();
                            break;
                        case "interesting":
                            scenario = new InterestingScenario();
                            break;
                        default:
                            throw new Exception("Unknown scenario");
                    }
                    scenario.init(config);
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
