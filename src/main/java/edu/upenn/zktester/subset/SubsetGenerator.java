package edu.upenn.zktester.subset;

import java.util.List;

@FunctionalInterface
public interface SubsetGenerator {

    List<Integer> generate();

}
