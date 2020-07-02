package edu.upenn.zootester.subset;

import java.util.List;
import java.util.Random;

public class FixedSizeSubset extends RandomSubsetGenerator {

    private final int totalSize;
    private final int subsetSize;

    public FixedSizeSubset(final int totalSize, final int subsetSize) {
        this(totalSize, subsetSize, new Random());
    }

    public FixedSizeSubset(final int totalSize, final int subsetSize, final Random random) {
        super(random);
        this.totalSize = totalSize;
        this.subsetSize = subsetSize;
    }

    public List<Integer> generate() {
        return generate(totalSize, subsetSize);
    }
}
