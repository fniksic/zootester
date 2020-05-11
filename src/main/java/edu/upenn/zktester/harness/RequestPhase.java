package edu.upenn.zktester.harness;

import edu.upenn.zktester.ensemble.ZKRequest;
import edu.upenn.zktester.util.ThrowingFunction;
import org.apache.zookeeper.KeeperException;

import java.util.function.Function;

public interface RequestPhase extends Phase {

    ZKRequest getRequest(Runnable onSuccess, Runnable onUnknown);

    int getNode();

    String getWriteKey();

    int getWriteValue();

    boolean isWrite();

    @Override
    default <T> T throwingMatch(ThrowingFunction<EmptyPhase, T> caseEmpty,
                                ThrowingFunction<RequestPhase, T> caseRequest) throws InterruptedException, KeeperException {
        return caseRequest.apply(this);
    }

    @Override
    default <T> T match(Function<EmptyPhase, T> caseEmpty, Function<RequestPhase, T> caseRequest) {
        return caseRequest.apply(this);
    }
}
