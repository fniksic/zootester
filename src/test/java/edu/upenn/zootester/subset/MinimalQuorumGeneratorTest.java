package edu.upenn.zootester.subset;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MinimalQuorumGeneratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(MinimalQuorumGeneratorTest.class);

    @Test
    public void testGenerate() {
        final MinimalQuorumGenerator generator = new MinimalQuorumGenerator(5);
        final Map<Set<Integer>, Integer> histogram = new HashMap<>();
        for (int i = 0; i < 1000; ++i) {
            final Set<Integer> sample = new HashSet<>(generator.generate());
            histogram.merge(sample, 1, (prev, one) -> prev + 1);
        }
        LOG.info("Histogram of samples after 1000 iterations: {}", histogram.toString());
    }
}
