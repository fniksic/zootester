package edu.upenn.zktester.subset;

import java.util.List;

public class FixedSizeSubset extends RandomSubsetGenerator {

    private final int totalSize;
    private final int subsetSize;

    public FixedSizeSubset(final int totalSize, final int subsetSize) {
        this.totalSize = totalSize;
        this.subsetSize = subsetSize;
    }

    @Override
    public List<Integer> generate() {
        return generateSubset(totalSize, subsetSize);
    }
}
