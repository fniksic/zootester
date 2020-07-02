package edu.upenn.zootester.harness;

import edu.upenn.zootester.ensemble.ZKRequest;

import java.util.function.Function;

/**
 * This phase is used as the write counterpart of the {@link ConditionalWritePhase} (which itself is considered as its
 * own read counterpart) for the purpose of computing possible sequentially consistent final states.
 */
public class VirtualWritePhase implements RequestPhase {

    private final ConditionalWritePhase conditionalWritePhase;

    public VirtualWritePhase(final ConditionalWritePhase conditionalWritePhase) {
        this.conditionalWritePhase = conditionalWritePhase;
    }

    public ConditionalWritePhase getConditionalWritePhase() {
        return conditionalWritePhase;
    }

    @Override
    public ZKRequest getRequest(final Runnable onSuccess, final Runnable onUnknown, final Runnable cleanup) {
        throw new UnsupportedOperationException("VirtualWritePhase doesn't provide an actual Zookeeper request.");
    }

    @Override
    public int getNode() {
        return conditionalWritePhase.getNode();
    }

    public String getWriteKey() {
        return conditionalWritePhase.getWriteKey();
    }

    public int getWriteValue() {
        return conditionalWritePhase.getWriteValue();
    }

    @Override
    public boolean isWrite() {
        return conditionalWritePhase.isReadSuccessful();
    }

    @Override
    public <T> T fullMatch(final Function<EmptyPhase, T> caseEmpty,
                           final Function<UnconditionalWritePhase, T> caseUnconditionalWrite,
                           final Function<ConditionalWritePhase, T> caseConditionalWrite,
                           final Function<VirtualWritePhase, T> caseVirtualWrite) {
        return caseVirtualWrite.apply(this);
    }

    @Override
    public String toString() {
        return "VirtualWritePhase{" +
                "node=" + getNode() +
                ", isWrite=" + isWrite() +
                ", writeKey='" + getWriteKey() + '\'' +
                ", writeValue=" + getWriteValue() +
                '}';
    }
}
