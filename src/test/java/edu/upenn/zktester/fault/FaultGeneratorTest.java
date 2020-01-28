package edu.upenn.zktester.fault;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FaultGeneratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(FaultGeneratorTest.class);

    @Test
    public void testGenerate() {
        final FaultGenerator faultGenerator = new FaultGenerator(3, 2, 3);
        final Map<List<Integer>, Integer> histogram = new HashMap<>(Map.of(
                List.of(2, 1, 0), 0,
                List.of(2, 0, 1), 0,
                List.of(1, 2, 0), 0,
                List.of(1, 1, 1), 0,
                List.of(1, 0, 2), 0,
                List.of(0, 2, 1), 0,
                List.of(0, 1, 2), 0));
        for (int i = 0; i < 700 * 1000; ++i) {
            faultGenerator.reset();
            final List<Integer> arrangement = List.of(faultGenerator.generate(),
                    faultGenerator.generate(), faultGenerator.generate());
            Assert.assertEquals("Total faults should be 3",
                    3, arrangement.get(0) + arrangement.get(1) + arrangement.get(2));
            histogram.compute(arrangement, (k, v) -> v + 1);
        }
        LOG.info("Histogram of samples after {} iterations (it should look uniform): {}",
                700_000, histogram.toString());
    }
}
