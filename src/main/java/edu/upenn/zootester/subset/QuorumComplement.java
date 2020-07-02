package edu.upenn.zootester.subset;

import java.util.List;
import java.util.Random;

public class QuorumComplement extends RandomSubsetGenerator {

    private final int totalSize;

    /***
     *
     * @param totalSize Total size of the set from which we are sampling. Assumed to be odd.
     */
    public QuorumComplement(final int totalSize) {
        this(totalSize, new Random());
    }

    public QuorumComplement(final int totalSize, final Random random) {
        super(random);
        this.totalSize = totalSize;
    }

    public List<Integer> generate() {
        // First we randomly generate the size of the quorum complement. This is a stupid, subptimal way to do it
        int quorumComplementSize = 0;
        for (int i = 0; i < totalSize; ++i) {
            if (random.nextDouble() < 0.5) {
                ++quorumComplementSize;
            }
        }
        if (quorumComplementSize >= (totalSize + 1) / 2) {
            quorumComplementSize = totalSize - quorumComplementSize;
        }
        return generate(totalSize, quorumComplementSize);
    }
}
