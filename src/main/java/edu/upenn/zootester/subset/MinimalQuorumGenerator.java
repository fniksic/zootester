package edu.upenn.zootester.subset;

import java.util.Random;

public class MinimalQuorumGenerator extends FixedSizeSubset {

    public MinimalQuorumGenerator(final int totalSize) {
        this(totalSize, new Random());
    }

    public MinimalQuorumGenerator(final int totalSize, final Random random) {
        // subsetSize should be the least integer strictly greater than totalSize / 2.0
        // The following snippet takes care of the cases when totalSize is odd or even
        super(totalSize, (totalSize - (totalSize & 1)) / 2 + 1, random);
    }
}
