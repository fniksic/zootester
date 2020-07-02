package edu.upenn.zootester.scenario;

import edu.upenn.zootester.harness.ConditionalWritePhase;
import edu.upenn.zootester.harness.EmptyPhase;
import edu.upenn.zootester.harness.Harness;
import edu.upenn.zootester.harness.UnconditionalWritePhase;

import java.util.List;

public class ParallelBaselineHarnessLong extends ParallelBaselineScenario {

    public ParallelBaselineHarnessLong() {
        super(List.of(
                new Harness(List.of(
                        new ConditionalWritePhase(0, "/key1", 0, "/key0", 100),
                        new ConditionalWritePhase(1, "/key0", 0, "/key0", 201),
                        new ConditionalWritePhase(2, "/key0", 100, "/key0", 302),
                        new EmptyPhase(),
                        new ConditionalWritePhase(2, "/key0", 100, "/key0", 402)
                )),
                new Harness(List.of(
                        new UnconditionalWritePhase(0, "/key1", 100),
                        new ConditionalWritePhase(0, "/key1", 100, "/key0", 200),
                        new ConditionalWritePhase(2, "/key0", 0, "/key1", 302),
                        new ConditionalWritePhase(0, "/key0", 200, "/key1", 400),
                        new EmptyPhase())
                ),
                new Harness(List.of(
                        new ConditionalWritePhase(2, "/key1", 0, "/key1", 102),
                        new EmptyPhase(),
                        new ConditionalWritePhase(0, "/key1", 0, "/key0", 200),
                        new ConditionalWritePhase(0, "/key1", 0, "/key1", 300),
                        new ConditionalWritePhase(0, "/key1", 102, "/key1", 400)
                )),
                new Harness(List.of(
                        new ConditionalWritePhase(2, "/key0", 0, "/key0", 102),
                        new ConditionalWritePhase(2, "/key1", 0, "/key0", 202),
                        new ConditionalWritePhase(1, "/key1", 0, "/key1", 301),
                        new UnconditionalWritePhase(1, "/key1", 401),
                        new EmptyPhase()
                )),
                //Interesting one
//                new Harness(List.of(
//                        new ConditionalWritePhase(1, "/key1", 0, "/key1", 101),
//                        new EmptyPhase(),
//                        new ConditionalWritePhase(0, "/key1", 101, "/key0", 200),
//                        new ConditionalWritePhase(1, "/key1", 0, "/key1", 301),
//                        new ConditionalWritePhase(0, "/key1", 0, "/key0", 400)
//                )),
                new Harness(List.of(
                        new EmptyPhase(),
                        new UnconditionalWritePhase(2, "/key1", 102),
                        new ConditionalWritePhase(1, "/key1", 0, "/key0", 201),
                        new ConditionalWritePhase(1, "/key1", 102, "/key0", 301),
                        new ConditionalWritePhase(1, "/key1", 0, "/key0", 401)
                )),
                new Harness(List.of(
                        new ConditionalWritePhase(2, "/key0", 0, "/key0", 102),
                        new UnconditionalWritePhase(0, "/key0", 200),
                        new ConditionalWritePhase(1, "/key1", 0, "/key0", 301),
                        new EmptyPhase(),
                        new ConditionalWritePhase(2, "/key0", 0, "/key1", 402)
                ))
//                new Harness(List.of(
//                        new EmptyPhase(),
//                        new ConditionalWritePhase(2, "/key1", 0, "/key1", 102),
//                        new ConditionalWritePhase(1, "/key0", 0, "/key1", 201),
//                        new UnconditionalWritePhase(1, "/key0", 301),
//                        new ConditionalWritePhase(2, "/key0", 301, "/key0", 402)
//                )),
//                new Harness(List.of(
//                        new UnconditionalWritePhase(2, "/key0", 102),
//                        new EmptyPhase(),
//                        new ConditionalWritePhase(1, "/key0", 102, "/key0", 201),
//                        new UnconditionalWritePhase(1, "/key0", 301),
//                        new ConditionalWritePhase(1, "/key0", 102, "/key0", 401)
//                )),
//                new Harness(List.of(
//                        new ConditionalWritePhase(0, "/key1", 0, "/key0", 100),
//                        new ConditionalWritePhase(2, "/key0", 100, "/key0", 202),
//                        new EmptyPhase(),
//                        new ConditionalWritePhase(1, "/key1", 0, "/key1", 301),
//                        new ConditionalWritePhase(0, "/key1", 0, "/key0", 400)
//                )),
//                new Harness(List.of(
//                        new ConditionalWritePhase(2, "/key1", 0, "/key1", 102),
//                        new EmptyPhase(),
//                        new ConditionalWritePhase(2, "/key1", 102, "/key1", 202),
//                        new ConditionalWritePhase(0, "/key1", 202, "/key1", 300),
//                        new ConditionalWritePhase(2, "/key1", 0, "/key0", 402)
//                )),
//                new Harness(List.of(
//                        new ConditionalWritePhase(1, "/key0", 0, "/key0", 101),
//                        new ConditionalWritePhase(0, "/key0", 101, "/key0", 200),
//                        new ConditionalWritePhase(1, "/key0", 101, "/key0", 301),
//                        new EmptyPhase(),
//                        new ConditionalWritePhase(1, "/key0", 101, "/key1", 401)
//                ))
        ));
    }
}
