package edu.upenn.zootester.subset;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class QuorumComplementTest {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumComplementTest.class);

    @Test
    public void testGenerateSize5() {
        generateSubsets(5, 1024 * 1024);
    }

    @Test
    public void testGenerateSize3() {
        generateSubsets(3, 1024 * 1024);
    }

    private void generateSubsets(final int totalSize, final int numSamples) {
        final QuorumComplement generator = new QuorumComplement(totalSize);
        final Map<Set<Integer>, Integer> histogram = new HashMap<>();
        for (int i = 0; i < numSamples; ++i) {
            final Set<Integer> sample = new HashSet<>(generator.generate());
            histogram.merge(sample, 1, (prev, one) -> prev + 1);
        }
        LOG.info("Histogram of samples after {} iterations (it should look uniform): {}",
                numSamples, histogram.toString());
    }
}
