package edu.upenn.zktester.harness;

import edu.upenn.zktester.ensemble.ZKRequest;
import org.apache.zookeeper.KeeperException;

import java.util.Arrays;

public class ConditionalWritePhase implements RequestPhase {

    private final int node;
    private final String readKey;
    private final int readValue;
    private final String writeKey;
    private final int writeValue;
    private final byte[] rawReadValue;
    private final byte[] rawWriteValue;

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
    public ZKRequest getRequest() {
        return zk -> {
            zk.getData(readKey, false, (returnCode, key, ctx, result, stat) -> {
                if (KeeperException.Code.OK.intValue() == returnCode && Arrays.equals(result, rawReadValue)) {
                    zk.setData(writeKey, rawWriteValue, -1, null, null);
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
    public int getWriteValue() {
        return writeValue;
    }

    @Override
    public String toString() {
        return "ConditionalWritePhase{" +
                "node=" + node +
                ", readKey='" + readKey + '\'' +
                ", readValue=" + readValue +
                ", writeKey='" + writeKey + '\'' +
                ", valueToWrite=" + writeValue +
                '}';
    }
}
