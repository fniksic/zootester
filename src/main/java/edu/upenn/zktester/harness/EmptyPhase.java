package edu.upenn.zktester.harness;

import edu.upenn.zktester.util.ThrowingFunction;
import org.apache.zookeeper.KeeperException;

import java.util.function.Function;

public class EmptyPhase implements Phase {

    @Override
    public <T> T throwingMatch(ThrowingFunction<EmptyPhase, T> caseEmpty,
                               ThrowingFunction<RequestPhase, T> caseRequest) throws InterruptedException, KeeperException {
        return caseEmpty.apply(this);
    }

    @Override
    public <T> T match(Function<EmptyPhase, T> caseEmpty, Function<RequestPhase, T> caseRequest) {
        return caseEmpty.apply(this);
    }
}
