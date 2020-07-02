package edu.upenn.zootester.harness;

import edu.upenn.zootester.ensemble.ZKRequest;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Function;

public class ConditionalWritePhase implements RequestPhase {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionalWritePhase.class);

    private final int node;
    private final String readKey;
    private final int readValue;
    private final String writeKey;
    private final int writeValue;
    private final byte[] rawReadValue;
    private final byte[] rawWriteValue;

    private boolean readSuccessful;

    public ConditionalWritePhase(final int node, final String readKey, final int readValue,
                                 final String writeKey, final int writeValue) {
        this.node = node;
        this.readKey = readKey;
        this.readValue = readValue;
        this.writeKey = writeKey;
        this.writeValue = writeValue;
        this.rawReadValue = Integer.toString(readValue).getBytes();
        this.rawWriteValue = Integer.toString(writeValue).getBytes();
    }

    @Override
    public ZKRequest getRequest(final Runnable onSuccess, final Runnable onUnknown, final Runnable cleanup) {
        return (zk, serverId) -> {
            LOG.info("Request @ {}: Read {} (expecting {})", serverId, readKey, readValue);
            zk.getData(readKey, false, (gReturnCode, gKey, gCtx, gResult, gStat) -> {
                if (KeeperException.Code.OK.intValue() == gReturnCode) {
                    LOG.info("Request @ {}: Got back {} -> {} (expected {})",
                            serverId, readKey, new String(gResult), readValue);
                    if (Arrays.equals(gResult, rawReadValue)) {
                        LOG.info("Request @ {}: Write {} -> {}", serverId, writeKey, writeValue);
                        zk.setData(writeKey, rawWriteValue, -1, (sReturnCode, sKey, sCtx, sStat) -> {
                            if (KeeperException.Code.OK.intValue() == sReturnCode) {
                                onSuccess.run();
                            } else {
                                LOG.warn("zk.setData() returned {}", KeeperException.Code.get(sReturnCode));
                                onUnknown.run();
                            }
                        }, null);
                    } else {
                        onSuccess.run();
                    }
                } else {
                    LOG.warn("zk.getData() returned {}. Treating as non-executed.", KeeperException.Code.get(gReturnCode));
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

    public String getReadKey() {
        return readKey;
    }

    public int getReadValue() {
        return readValue;
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
        // In the context of computing sequentially consistent states, this phase is considered its read counterpart
        return false;
    }

    @Override
    public <T> T fullMatch(final Function<EmptyPhase, T> caseEmpty,
                           final Function<UnconditionalWritePhase, T> caseUnconditionalWrite,
                           final Function<ConditionalWritePhase, T> caseConditionalWrite,
                           final Function<VirtualWritePhase, T> caseVirtualWrite) {
        return caseConditionalWrite.apply(this);
    }

    public boolean isReadSuccessful() {
        return readSuccessful;
    }

    public void setReadSuccessful(final boolean readSuccessful) {
        this.readSuccessful = readSuccessful;
    }

    @Override
    public String toString() {
        return "ConditionalWritePhase{" +
                "node=" + node +
                ", readKey='" + readKey + '\'' +
                ", readValue=" + readValue +
                ", readSuccessful=" + readSuccessful +
                ", writeKey='" + writeKey + '\'' +
                ", valueToWrite=" + writeValue +
                '}';
    }
}
