package edu.upenn.zootester.fault;

import java.util.Random;

public class ExactFaultGenerator extends FaultGenerator {

    public ExactFaultGenerator(final int totalRounds, final int maxFaultsPerRound, final int faultBudget,
                               final Random random) {
        super(totalRounds, maxFaultsPerRound, faultBudget, random);
    }

    @Override
    protected void initalizeArrangements(final int[] arrangementsZerothRow) {
        arrangementsZerothRow[0] = 1;
    }
}
