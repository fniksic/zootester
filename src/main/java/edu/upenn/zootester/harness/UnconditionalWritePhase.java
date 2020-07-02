package edu.upenn.zootester.harness;

import edu.upenn.zootester.ensemble.ZKRequest;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class UnconditionalWritePhase implements RequestPhase {

    private static final Logger LOG = LoggerFactory.getLogger(UnconditionalWritePhase.class);

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
    public ZKRequest getRequest(final Runnable onSuccess, final Runnable onUnknown, final Runnable cleanup) {
        return (zk, serverId) -> {
            LOG.info("Request @ {}: Write {} -> {}", serverId, writeKey, writeValue);
            zk.setData(writeKey, rawWriteValue, -1, (sReturnCode, sKey, sCtx, sStat) -> {
                if (KeeperException.Code.OK.intValue() == sReturnCode) {
                    onSuccess.run();
                } else {
                    LOG.warn("zk.setData() returned {}", KeeperException.Code.get(sReturnCode));
                    onUnknown.run();
                }
                cleanup.run();
            }, null);
            Thread.sleep(100);
//            System.gc();
            LOG.info("Woke up!");
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
