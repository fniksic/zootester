package edu.upenn.zootester.ensemble;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ZKEnsemble implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZKEnsemble.class);

    private final int totalNodes;
    private final List<Integer> clientPorts = new ArrayList<>();
    private final List<ZKNodeHandler> servers = new ArrayList<>();
    private final List<ZooKeeper> clients = new ArrayList<>();
    private final List<Integer> allIds;

    // Given i, clientForServer.get(i) is the ID of the client that talks to server i.
    // Conversely, serverForClient.get(i) is the ID of the server that client i talks to.
    // During the execution, a particular client may no longer be able to connect to a server,
    // so we may need to reassign it to another server.
    private final List<Integer> clientForServer = new ArrayList<>();
    private final List<Integer> serverForClient = new ArrayList<>();

    public ZKEnsemble(final int totalNodes) {
        this.totalNodes = totalNodes;
        this.allIds = IntStream.range(0, totalNodes).boxed().collect(Collectors.toList());
    }

    public void init() throws IOException {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < totalNodes; ++i) {
            clientPorts.add(ZKHelper.getUniquePort());
            sb.append("server.").append(i).append("=127.0.0.1:").append(ZKHelper.getUniquePort())
                    .append(':').append(ZKHelper.getUniquePort()).append('\n');
        }
        final String quorumCfgSection = sb.toString();

        for (int i = 0; i < totalNodes; ++i) {
            final ZKNodeHandler server = new ZKNodeHandler(i, clientPorts.get(i), quorumCfgSection);
            servers.add(server);
        }
    }

    public void startEnsemble() throws IOException, InterruptedException {
        for (int i = 0; i < totalNodes; ++i) {
            startSingle(i);
            final ZooKeeper client = new ZooKeeper("127.0.0.1:" + clientPorts.get(i),
                    ZKHelper.CONNECTION_TIMEOUT, this);
            clients.add(client);

            // Initially client i speaks to server i
            clientForServer.add(i);
            serverForClient.add(i);
        }
        waitForAllClients(ZooKeeper.States.CONNECTED);
    }

    public void startServers(final List<Integer> serverIds) throws InterruptedException, IOException {
        LOG.info("Starting servers: {}", serverIds);
        for (final int i : serverIds) {
            startSingle(i);
        }
        waitForClients(States.CONNECTED, serverIds);
    }

    public void startSingle(final int serverId) {
        final ZKNodeHandler server = servers.get(serverId);
        if (!server.isRunning()) {
            server.start();
        }
    }

    public void startAllServers() throws InterruptedException, IOException {
        startServers(allIds);
    }

    public boolean isRunning(final int serverId) {
        return servers.get(serverId).isRunning();
    }

    public int getLeader() {
        return IntStream.range(0, servers.size())
                .filter(i -> servers.get(i).isLeader())
                .findFirst()
                .orElseThrow();
    }

    public void stopServers(final List<Integer> serverIds) throws InterruptedException, IOException {
        LOG.info("Stopping servers: {}", serverIds);
        for (final int i : serverIds) {
            stopSingle(i);
        }
        waitForClients(States.CONNECTING, serverIds);
    }

    public void stopSingle(final int serverId) throws InterruptedException {
        servers.get(serverId).shutdown();
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
        for (final int i : serverIds) {
            stopSingle(i);
        }
    }

    public void stopAllServers() throws InterruptedException, IOException {
        stopServers(allIds);
    }

    private void waitForClients(final States state, final List<Integer> serverIds) throws InterruptedException, IOException {
        List<Integer> unmatchedServerIds = waitFor(state, serverIds);
        if (unmatchedServerIds.isEmpty()) {
            return;
        } else if (state.equals(States.CONNECTING)) {
            LOG.error("After waiting for 10s, not all clients transitioned to CONNECTING state.");
            throw new RuntimeException("Waited for too long in waitForClients");
        }

        // If state == States.CONNECTED, meaning we're waiting for clients to connect to the servers
        final boolean[][] canTalkTo = allCanTalkToAll(totalNodes);
        while (!unmatchedServerIds.isEmpty()) {
            final List<States> clientStates = unmatchedServerIds.stream()
                    .mapToInt(clientForServer::get)
                    .mapToObj(clients::get)
                    .map(ZooKeeper::getState)
                    .collect(Collectors.toList());
            LOG.info("Servers whose clients couldn't connect to them: {}. Corresponding client states: {}",
                    unmatchedServerIds, clientStates);
            unmatchedServerIds.forEach(serverId -> canTalkTo[serverId][clientForServer.get(serverId)] = false);
            if (!reassignClients(canTalkTo, unmatchedServerIds)) {
                final StringBuilder sb = new StringBuilder();
                sb.append("Cannot reassign clients to servers. Final client-server conflicts:");
                for (int i = 0; i < totalNodes; ++i) {
                    sb.append(" ").append(i).append(": {");
                    String sep = "";
                    for (int j = 0; j < totalNodes; ++j) {
                        if (!canTalkTo[j][i]) {
                            sb.append(sep).append(j);
                            sep = ",";
                        }
                    }
                    sb.append("}");
                }
                LOG.error(sb.toString());
                throw new RuntimeException("ZK clients cannot be reassigned to servers " +
                        "so that all connections can be established");
            }
            LOG.info("Reassigned clients to servers. The assignment is: {}", clientForServer);
            updateClientConnections();
            unmatchedServerIds = waitFor(state, serverIds);
        }
    }

    private static boolean[][] allCanTalkToAll(final int totalNodes) {
        final boolean[][] canTalkTo = new boolean[totalNodes][totalNodes];
        for (int i = 0; i < totalNodes; i++) {
            for (int j = 0; j < totalNodes; j++) {
                canTalkTo[i][j] = true;
            }
        }
        return canTalkTo;
    }

    private List<Integer> waitFor(final States state, final List<Integer> serverIds) throws InterruptedException {
        List<Integer> unmatchedServerIds = serverIds;
        int iterations = 100;
        while (!unmatchedServerIds.isEmpty() && iterations-- > 0) {
            Thread.sleep(100);
            unmatchedServerIds = serverIds.stream()
                    .filter(serverId -> clients.get(clientForServer.get(serverId)).getState() != state)
                    .collect(Collectors.toList());
        }
        return unmatchedServerIds;
    }

    private boolean reassignClients(final boolean[][] canTalkTo, final List<Integer> serverIds) {
        final Set<Integer> unmatchedServerIds = new HashSet<>(serverIds);
        for (final int serverId : serverIds) {
            if (!tryMatch(serverId, canTalkTo, unmatchedServerIds, new HashSet<>())) {
                return false;
            }
            unmatchedServerIds.remove(serverId);
        }
        return true;
    }

    private void updateClientConnections() throws IOException {
        for (final int clientId : allIds) {
            final int serverId = serverForClient.get(clientId);
            final ZooKeeper client = clients.get(clientId);
            client.updateServerList("127.0.0.1:" + clientPorts.get(serverId));
        }
    }

    private boolean tryMatch(int serverId,
                             final boolean[][] canTalkTo,
                             final Set<Integer> unmatchedServerIds,
                             final Set<Integer> visitedClientIds) {
        for (int clientId = 0; clientId < totalNodes; ++clientId) {
            if (canTalkTo[serverId][clientId] && !visitedClientIds.contains(clientId)) {
                visitedClientIds.add(clientId);
                final int otherServerId = serverForClient.get(clientId);
                if (unmatchedServerIds.contains(otherServerId)
                        || tryMatch(otherServerId, canTalkTo, unmatchedServerIds, visitedClientIds)) {
                    clientForServer.set(serverId, clientId);
                    serverForClient.set(clientId, serverId);
                    return true;
                }
            }
        }
        return false;
    }

    private void waitForAllClients(final States state) throws InterruptedException, IOException {
        waitForClients(state, allIds);
    }

    public int totalRunningServers() {
        return servers.stream().filter(ZKNodeHandler::isRunning).mapToInt(ignored -> 1).sum();
    }

    public void handleRequest(final int serverId, final ZKRequest request) throws KeeperException, InterruptedException {
        final int clientId = clientForServer.get(serverId);
        request.apply(clients.get(clientId), serverId);
    }

    public boolean checkProperty(final ZKProperty property) throws KeeperException, InterruptedException {
        return property.test(clients, clientForServer);
    }

    public void stopEnsemble() throws InterruptedException, IOException {
        for (final var client : clients) {
            client.close();
        }
        clients.clear();
        clientForServer.clear();
        serverForClient.clear();
        for (final var server : servers) {
            server.shutdown();
            server.clean();
        }
    }

    public void tearDown() throws IOException {
        for (final var server : servers) {
            server.tearDown();
        }
        LOG.info("Ensemble teardown complete");
    }

    @Override
    public void process(final WatchedEvent watchedEvent) {
        // Ignore
    }
}
