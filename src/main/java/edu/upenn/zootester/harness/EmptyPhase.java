package edu.upenn.zootester.harness;

import edu.upenn.zootester.util.ThrowingFunction;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.function.Function;

public class EmptyPhase implements Phase {

    @Override
    public <T> T throwingMatch(ThrowingFunction<EmptyPhase, T> caseEmpty,
                               ThrowingFunction<RequestPhase, T> caseRequest) throws InterruptedException, KeeperException, IOException {
        return caseEmpty.apply(this);
    }

    @Override
    public <T> T match(Function<EmptyPhase, T> caseEmpty, Function<RequestPhase, T> caseRequest) {
        return caseEmpty.apply(this);
    }

    @Override
    public <T> T fullMatch(final Function<EmptyPhase, T> caseEmpty,
                           final Function<UnconditionalWritePhase, T> caseUnconditionalWrite,
                           final Function<ConditionalWritePhase, T> caseConditionalWrite,
                           final Function<VirtualWritePhase, T> caseVirtualWrite) {
        return caseEmpty.apply(this);
    }

    @Override
    public String toString() {
        return "EmptyPhase{}";
    }
}
