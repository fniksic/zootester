package edu.upenn.zktester.harness;

import edu.upenn.zktester.ensemble.ZKRequest;

import java.util.function.Function;

public class UnconditionalWritePhase implements RequestPhase {

    private final int node;
    private final String writeKey;
    private final int writeValue;
    private final byte[] rawWriteValue;

    public UnconditionalWritePhase(final int node, final String writeKey, final int writeValue) {
        this.node = node;
        this.writeKey = writeKey;
        this.writeValue = writeValue;
        this.rawWriteValue = Integer.toString(writeValue).getBytes();
    }

    @Override
    public ZKRequest getRequest() {
        return zk -> {
            zk.setData(writeKey, rawWriteValue, -1, null, null);
            Thread.sleep(500);
            System.gc();
        };
    }

    @Override
    public int getNode() {
        return node;
    }

    @Override
    public String getWriteKey() {
        return writeKey;
    }

    @Override
    public int getWriteValue() {
        return writeValue;
    }

    @Override
    public boolean isWrite() {
        return true;
    }

    @Override
    public <T> T fullMatch(final Function<EmptyPhase, T> caseEmpty,
                           final Function<UnconditionalWritePhase, T> caseUnconditionalWrite,
                           final Function<ConditionalWritePhase, T> caseConditionalWrite,
                           final Function<VirtualWritePhase, T> caseVirtualWrite) {
        return caseUnconditionalWrite.apply(this);
    }

    @Override
    public String toString() {
        return "UnconditionalWritePhase{" +
                "node=" + node +
                ", writeKey='" + writeKey + '\'' +
                ", writeValue=" + writeValue +
                '}';
    }
}
