package edu.upenn.zktester.subset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MinimalQuorumComplementTest {

    private static final Logger LOG = LogManager.getLogger();

    @Test
    public void testGenerate() {
        final SubsetGenerator generator = new MinimalQuorumComplement(5);
        final Map<Set<Integer>, Integer> histogram = new HashMap<>();
        for (int i = 0; i < 1000; ++i) {
            final Set<Integer> sample = new HashSet(generator.generate());
            histogram.merge(sample, 1, (prev, one) -> prev + 1);
        }
        LOG.info("Histogram of samples after 1000 iterations: {}", histogram.toString());
    }
}
