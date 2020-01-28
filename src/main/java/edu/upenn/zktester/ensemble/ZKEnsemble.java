package edu.upenn.zktester.ensemble;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ZKEnsemble implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZKEnsemble.class);

    private final int totalNodes;
    private final List<Integer> clientPorts = new ArrayList<>();
    private final List<ZKNodeHandler> servers = new ArrayList<>();
    private final List<ZooKeeper> clients = new ArrayList<>();

    private final List<Integer> allIds;

    public ZKEnsemble(final int totalNodes) {
        this.totalNodes = totalNodes;
        this.allIds = IntStream.range(0, totalNodes).boxed().collect(Collectors.toList());
    }

    public void init() throws IOException {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < totalNodes; ++i) {
            clientPorts.add(ZKHelper.getUniquePort());
            sb.append("server." + i + "=127.0.0.1:" + ZKHelper.getUniquePort() + ":" + ZKHelper.getUniquePort() + "\n");
        }
        final String quorumCfgSection = sb.toString();

        for (int i = 0; i < totalNodes; ++i) {
            final ZKNodeHandler server = new ZKNodeHandler(i, clientPorts.get(i), quorumCfgSection);
            servers.add(server);
        }
    }

    public void startEnsemble() throws IOException, InterruptedException {
        for (int i = 0; i < totalNodes; ++i) {
            servers.get(i).start();
            final ZooKeeper client = new ZooKeeper("127.0.0.1:" + clientPorts.get(i),
                    ZKHelper.CONNECTION_TIMEOUT, this);
            clients.add(client);
        }
        waitForAllClients(ZooKeeper.States.CONNECTED);
    }

    public void startServers(final List<Integer> serverIds) throws InterruptedException {
        LOG.info("Starting servers: {}", serverIds);
        for (final var i : serverIds) {
            servers.get(i).start();
        }
        waitForClients(States.CONNECTED, serverIds);
    }

    public void startAllServers() throws InterruptedException {
        startServers(allIds);
    }

    public int getLeader() {
        return IntStream.range(0, servers.size())
                .filter(i -> servers.get(i).isLeader())
                .findFirst()
                .orElseThrow();
    }

    public void stopServers(final List<Integer> serverIds) throws InterruptedException {
        LOG.info("Stopping servers: {}", serverIds);
        for (final var i : serverIds) {
            servers.get(i).shutdown();
        }
        waitForClients(States.CONNECTING, serverIds);
    }

    /***
     * Unlike stopNodes(), this method does not wait for the ZooKeeper client to
     * detect that the servers have been stopped.
     *
     * @param serverIds
     * @throws InterruptedException
     */
    public void crashServers(final List<Integer> serverIds) throws InterruptedException {
        LOG.info("Crashing servers: {}", serverIds);
        for (final var i : serverIds) {
            servers.get(i).shutdown();
        }
    }

    public void stopAllServers() throws InterruptedException {
        stopServers(allIds);
    }


    private void waitForClients(final States state, final List<Integer> clientIds) throws InterruptedException {
        if (clientIds.isEmpty()) {
            return;
        }

        int iterations = 100;
        boolean notAllInState = true;
        while (notAllInState && iterations-- > 0) {
            Thread.sleep(100);
            notAllInState = clientIds.stream()
                    .map(i -> clients.get(i))
                    .anyMatch(zk -> zk.getState() != state);
        }
        if (notAllInState) {
            throw new RuntimeException("Waiting too long in waitForAll");
        }
    }

    private void waitForAllClients(final States state) throws InterruptedException {
        waitForClients(state, allIds);
    }

    public void handleRequest(final int clientId, final ZKRequest request) throws KeeperException, InterruptedException {
        request.apply(clients.get(clientId));
    }

    public boolean checkProperty(final ZKProperty property) throws KeeperException, InterruptedException {
        return property.test(clients.stream().collect(Collectors.toList()));
    }

    public void stopEnsemble() throws InterruptedException, IOException {
        for (final var client : clients) {
            client.close();
        }
        clients.clear();
        for (final var server : servers) {
            server.shutdown();
            server.clean();
        }
    }

    @Override
    public void process(final WatchedEvent watchedEvent) {
        // Ignore
    }
}
