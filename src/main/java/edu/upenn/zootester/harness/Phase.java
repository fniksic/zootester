package edu.upenn.zootester.harness;

import edu.upenn.zootester.util.ThrowingFunction;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.function.Function;

public interface Phase {

    <T> T throwingMatch(ThrowingFunction<EmptyPhase, T> caseEmpty,
                        ThrowingFunction<RequestPhase, T> caseRequest) throws InterruptedException, KeeperException, IOException;

    <T> T match(Function<EmptyPhase, T> caseEmpty, Function<RequestPhase, T> caseRequest);

    <T> T fullMatch(Function<EmptyPhase, T> caseEmpty,
                    Function<UnconditionalWritePhase, T> caseUnconditionalWrite,
                    Function<ConditionalWritePhase, T> caseConditionalWrite,
                    Function<VirtualWritePhase, T> caseVirtualWrite);
}
