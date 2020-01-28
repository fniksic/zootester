package edu.upenn.zktester;

import edu.upenn.zktester.scenario.RandomScenario;
import edu.upenn.zktester.scenario.Scenario;
import edu.upenn.zktester.util.AssertionFailureError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKTester {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTester.class);

    public static void main(final String[] args) {
        final Scenario scenario = new RandomScenario(1000, 3, 1, 2);
//        final Scenario scenario = new DivergenceResyncScenario();
        try {
            scenario.init();
            scenario.execute();
        } catch (final Exception e) {
            LOG.error("Exception while executing scenario", e);
        } catch (final AssertionFailureError e) {
            LOG.error("Assertion failed", e);
        }
    }
}
