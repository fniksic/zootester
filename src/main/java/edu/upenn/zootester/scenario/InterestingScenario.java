package edu.upenn.zootester.scenario;

import edu.upenn.zootester.harness.ConditionalWritePhase;
import edu.upenn.zootester.harness.EmptyPhase;
import edu.upenn.zootester.harness.Harness;

import java.util.List;

public class InterestingScenario extends TrueToThePaperScenario {

    public InterestingScenario() {
        super(new Harness(List.of(
                new ConditionalWritePhase(1, "/key1", 0, "/key1", 101),
                new EmptyPhase(),
                new ConditionalWritePhase(0, "/key1", 101, "/key0", 200),
                new ConditionalWritePhase(1, "/key1", 0, "/key1", 301),
                new ConditionalWritePhase(0, "/key1", 0, "/key0", 400)
        )));
    }
}
