package edu.upenn.zktester;

import edu.upenn.zktester.ensemble.ZKHelper;
import edu.upenn.zktester.scenario.*;
import edu.upenn.zktester.util.AssertionFailureError;
import edu.upenn.zktester.util.Config;
import edu.upenn.zktester.util.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKTester {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTester.class);

    public static void main(final String[] args) {
        try {
            final Config config = Config.parseArgs(args);
            ZKHelper.setBasePort(config.getBasePort());
            final Scenario scenario;
            if (config.getThreads() > 1) {
                scenario = new ParallelScenario(config);
            } else {
                switch (config.getScenario()) {
                    case "divergence":
                        scenario = new DivergenceResyncScenario();
                        break;
                    case "divergence-2":
                        scenario = new DivergenceResyncScenario2();
                        break;
                    case "random":
                        scenario = new RandomScenario();
                        break;
                    default:
                        LOG.error("Unknown scenario!");
                        throw new Exception("Unknown scenario");
                }
            }
            scenario.init(config);
            scenario.execute();
        } catch (final ConfigException e) {
            LOG.error("Configuration exception", e);
            Config.showUsage();
        } catch (final AssertionFailureError e) {
            LOG.info("Assertion failure", e);
        } catch (final Exception e) {
            LOG.error("Exception failure", e);
        }
    }
}
