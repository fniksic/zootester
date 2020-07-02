package edu.upenn.zootester.scenario;

import edu.upenn.zootester.harness.ConditionalWritePhase;
import edu.upenn.zootester.harness.EmptyPhase;
import edu.upenn.zootester.harness.Harness;
import edu.upenn.zootester.harness.UnconditionalWritePhase;

import java.util.List;

public class ParallelBaselineHarnessShort extends ParallelBaselineScenario {

    public ParallelBaselineHarnessShort() {
        super(List.of(
                new Harness(List.of(
                        new UnconditionalWritePhase(0, "/key0", 100),
                        new EmptyPhase(),
                        new ConditionalWritePhase(1, "/key0", 0, "/key1", 201)
                )),
                new Harness(List.of(
                        new UnconditionalWritePhase(0, "/key1", 100),
                        new EmptyPhase(),
                        new UnconditionalWritePhase(0, "/key1", 200)
                )),
                new Harness(List.of(
                        new UnconditionalWritePhase(2, "/key0", 102),
                        new UnconditionalWritePhase(1, "/key0", 201),
                        new EmptyPhase()
                )),
                new Harness(List.of(
                        new EmptyPhase(),
                        new ConditionalWritePhase(1, "/key0", 0, "/key1", 101),
                        new ConditionalWritePhase(1, "/key0", 0, "/key0", 201)
                )),
                new Harness(List.of(
                        new ConditionalWritePhase(0, "/key1", 0, "/key0", 100),
                        new UnconditionalWritePhase(1, "/key0", 201),
                        new EmptyPhase()
                )),
                new Harness(List.of(
                        new UnconditionalWritePhase(2, "/key0", 102),
                        new UnconditionalWritePhase(1, "/key1", 201),
                        new EmptyPhase()
                )),
                new Harness(List.of(
                        new ConditionalWritePhase(1, "/key1", 0, "/key1", 101),
                        new EmptyPhase(),
                        new ConditionalWritePhase(0, "/key0", 0, "/key0", 200)
                )),
                new Harness(List.of(
                        new ConditionalWritePhase(0, "/key1", 0, "/key0", 100),
                        new EmptyPhase(),
                        new ConditionalWritePhase(2, "/key1", 0, "/key0", 202)
                )),
                new Harness(List.of(
                        new UnconditionalWritePhase(2, "/key0", 102),
                        new ConditionalWritePhase(0, "/key0", 102, "/key0", 200),
                        new EmptyPhase()
                )),
                new Harness(List.of(
                        new EmptyPhase(),
                        new UnconditionalWritePhase(1, "/key1", 101),
                        new ConditionalWritePhase(2, "/key0", 0, "/key1", 202)
                )),
                new Harness(List.of(
                        new EmptyPhase(),
                        new UnconditionalWritePhase(0, "/key1", 100),
                        new ConditionalWritePhase(1, "/key0", 0, "/key1", 201)
                )),
                new Harness(List.of(
                        new ConditionalWritePhase(2, "/key1", 0, "/key1", 102),
                        new EmptyPhase(),
                        new ConditionalWritePhase(0, "/key0", 0, "/key0", 200)
                ))
        ));
    }
}
