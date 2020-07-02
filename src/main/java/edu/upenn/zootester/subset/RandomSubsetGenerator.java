package edu.upenn.zootester.subset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomSubsetGenerator {

    final protected Random random;

    public RandomSubsetGenerator() {
        this(new Random());
    }

    public RandomSubsetGenerator(final Random random) {
        this.random = random;
    }

    public List<Integer> generate(final int n, final int k) {
        final List<Integer> result =
                IntStream.range(0, k).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        for (int i = k; i < n; ++i) {
            final int j = random.nextInt(i + 1);
            if (j < k) {
                result.set(j, i);
            }
        }
        Collections.sort(result);
        return result;
    }

    public List<Integer> generate(final int n) {
        return IntStream.range(0, n).filter(i -> random.nextDouble() < 0.5).boxed().collect(Collectors.toList());
    }
}
