package edu.upenn.zootester.harness;

import edu.upenn.zootester.ensemble.ZKRequest;
import edu.upenn.zootester.util.ThrowingFunction;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.function.Function;

public interface RequestPhase extends Phase {

    ZKRequest getRequest(Runnable onSuccess, Runnable onUnknown, Runnable cleanup);

    int getNode();

    String getWriteKey();

    int getWriteValue();

    boolean isWrite();

    @Override
    default <T> T throwingMatch(ThrowingFunction<EmptyPhase, T> caseEmpty,
                                ThrowingFunction<RequestPhase, T> caseRequest) throws InterruptedException, KeeperException, IOException {
        return caseRequest.apply(this);
    }

    @Override
    default <T> T match(Function<EmptyPhase, T> caseEmpty, Function<RequestPhase, T> caseRequest) {
        return caseRequest.apply(this);
    }
}
