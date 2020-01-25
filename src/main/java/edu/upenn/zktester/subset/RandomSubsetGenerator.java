package edu.upenn.zktester.subset;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public abstract class RandomSubsetGenerator implements SubsetGenerator {

    final protected Random random = new Random();

    protected List<Integer> generateSubset(final int n, final int k) {
        final List<Integer> result =
                IntStream.range(0, k).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        for (int i = k; i < n; ++i) {
            final int j = random.nextInt(i + 1);
            if (j < k) {
                result.set(j, i);
            }
        }
        return result;
    }
}
