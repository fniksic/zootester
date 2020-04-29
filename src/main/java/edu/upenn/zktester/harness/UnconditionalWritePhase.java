package edu.upenn.zktester.harness;

import edu.upenn.zktester.ensemble.ZKRequest;

public class UnconditionalWritePhase implements RequestPhase {

    private final int node;
    private final String key;
    private final int valueToWrite;
    private final byte[] rawValueToWrite;

    public UnconditionalWritePhase(final int node, final String key, final int valueToWrite) {
        this.node = node;
        this.key = key;
        this.valueToWrite = valueToWrite;
        this.rawValueToWrite = Integer.toString(valueToWrite).getBytes();
    }

    @Override
    public ZKRequest getRequest() {
        return zk -> {
            zk.setData(key, rawValueToWrite, -1, null, null);
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
        return "UnconditionalWritePhase{" +
                "node=" + node +
                ", key='" + key + '\'' +
                ", valueToWrite=" + valueToWrite +
                '}';
    }
}
