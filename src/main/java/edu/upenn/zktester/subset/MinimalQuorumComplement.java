package edu.upenn.zktester.subset;

public class MinimalQuorumComplement extends FixedSizeSubset {

    public MinimalQuorumComplement(final int totalSize) {
        // complementSize should be the greatest integer strictly less than totalSize / 2.0
        // The following snippet takes care of the cases when totalSize is odd or even
        super(totalSize, (totalSize + (totalSize & 1)) / 2 - 1);
    }
}
