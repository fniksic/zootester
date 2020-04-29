package edu.upenn.zktester.harness;

import edu.upenn.zktester.ensemble.ZKRequest;
import org.apache.zookeeper.KeeperException;

import java.util.Arrays;

public class ConditionalWritePhase implements RequestPhase {

    private final int node;
    private final String key;
    private final int valueToRead;
    private final int valueToWrite;
    private final byte[] rawValueToRead;
    private final byte[] rawValueToWrite;

    public ConditionalWritePhase(final int node, final String key, final int valueToRead, final int valueToWrite) {
        this.node = node;
        this.key = key;
        this.valueToRead = valueToRead;
        this.valueToWrite = valueToWrite;
        this.rawValueToRead = Integer.toString(valueToRead).getBytes();
        this.rawValueToWrite = Integer.toString(valueToWrite).getBytes();
    }

    @Override
    public ZKRequest getRequest() {
        return zk -> {
            // Zookeeper doesn't provide a native compare-and-swap. We simulate one by getting the value
            // and setting the new value only if the version hasn't changed.
            zk.getData(key, false, (returnCode, key, ctx, result, stat) -> {
                if (KeeperException.Code.OK.intValue() == returnCode && Arrays.equals(result, rawValueToRead)) {
                    zk.setData(key, rawValueToWrite, stat.getVersion(), null, null);
                }
            }, null);
            Thread.sleep(500);
            System.gc();
        };
    }

    @Override
    public int getNode() {
        return node;
    }

    @Override
    public int getValueToWrite() {
        return valueToWrite;
    }

    @Override
    public String toString() {
        return "ConditionalWritePhase{" +
                "node=" + node +
                ", key='" + key + '\'' +
                ", valueToRead=" + valueToRead +
                ", valueToWrite=" + valueToWrite +
                '}';
    }
}
