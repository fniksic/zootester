package edu.upenn.zktester.harness;

import edu.upenn.zktester.util.ThrowingFunction;
import org.apache.zookeeper.KeeperException;

import java.util.function.Function;

public interface Phase {

    <T> T throwingMatch(ThrowingFunction<EmptyPhase, T> caseEmpty,
                        ThrowingFunction<RequestPhase, T> caseRequest) throws InterruptedException, KeeperException;

    <T> T match(Function<EmptyPhase, T> caseEmpty, Function<RequestPhase, T> caseRequest);

    <T> T fullMatch(Function<EmptyPhase, T> caseEmpty,
                    Function<UnconditionalWritePhase, T> caseUnconditionalWrite,
                    Function<ConditionalWritePhase, T> caseConditionalWrite,
                    Function<VirtualWritePhase, T> caseVirtualWrite);
}
