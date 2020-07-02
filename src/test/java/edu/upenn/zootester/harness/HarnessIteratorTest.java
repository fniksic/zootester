package edu.upenn.zootester.harness;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class HarnessIteratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(HarnessIteratorTest.class);

    @Test
    public void testHarnessIterator() {
        final HarnessIterator harnessIterator = Harness.generate(2, 3, 2, 3);
        int count = 0;
        final Set<String> unique = new HashSet<>();
        while (harnessIterator.hasNext()) {
            final Harness harness = harnessIterator.next();
            final String phases = harness.getPhases().toString();
            LOG.info(phases);
            unique.add(phases);
            ++count;
        }
        LOG.info("Total harnesses: {}", count);
        assertEquals(count, unique.size());
    }
}
