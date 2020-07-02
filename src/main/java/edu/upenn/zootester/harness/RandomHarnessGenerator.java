package edu.upenn.zootester.harness;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomHarnessGenerator {

    private final Random random;
    private final List<Harness> harnesses = new ArrayList<>();

    public RandomHarnessGenerator(final int numKeys, final int numNodes, final int numRequests, final int numPhases, final Random random) {
        this.random = random;

        // For simplicity we precompute all harnesses and store them in a list
        // TODO: Figure out how to generate harnesses uniformly at random without precomputing all of them
        final HarnessIterator iterator = Harness.generate(numKeys, numNodes, numRequests, numPhases);
        while (iterator.hasNext()) {
            this.harnesses.add(iterator.next());
        }
    }

    public Harness next() {
        return harnesses.get(random.nextInt(harnesses.size()));
    }
}
