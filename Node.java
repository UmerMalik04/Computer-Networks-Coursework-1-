// IN2011 Computer Networks
// Coursework 2025/2026
//
// Submission by
//  Umer Malik
//  230035630
//  Umer.malik@city.ac.uk

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;


public class    Node implements NodeInterface {
    private static final int BUFFER_SIZE = 65535;
    private static final int RESPONSE_TIMEOUT_MS = 5000;
    private static final int MAX_RETRIES = 3;
    private static final int DUPLICATE_CACHE_LIMIT = 1024;

    private final Map<String, String> dataStore = new ConcurrentHashMap<>();
    private final Map<String, InetSocketAddress> knownNodes = new ConcurrentHashMap<>();
    private final Map<String, BlockingQueue<IncomingMessage>> pendingResponses = new ConcurrentHashMap<>();
    private final Map<String, RelayContext> relayContexts = new ConcurrentHashMap<>();
    private final Map<String, String> duplicateResponses = Collections.synchronizedMap(
            new java.util.LinkedHashMap<String, String>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > DUPLICATE_CACHE_LIMIT;
                }
            }
    );
    private final Deque<String> relayStack = new ArrayDeque<>();

    private volatile String nodeName;
    private volatile byte[] nodeHash;
    private volatile DatagramSocket socket;
    private volatile boolean running;
    private volatile Thread receiverThread;
    private volatile InetSocketAddress selfAddress;

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with N:");
        }
        this.nodeName = nodeName;
        this.nodeHash = HashID.computeHashID(nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        if (nodeName == null) {
            throw new IllegalStateException("Call setNodeName before openPort");
        }
        socket = new DatagramSocket(portNumber);
        selfAddress = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), portNumber);
        knownNodes.put(nodeName, selfAddress);
        running = true;
        receiverThread = new Thread(this::receiveLoop, "crn-receiver-" + portNumber);
        receiverThread.setDaemon(true);
        receiverThread.start();
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) {
            throw new IllegalStateException("Port is not open");
        }
        if (delay == 0) {
            while (running) {
                Thread.sleep(1000);
            }
            return;
        }
        long end = System.currentTimeMillis() + delay;
        while (System.currentTimeMillis() < end) {
            Thread.sleep(Math.min(100, end - System.currentTimeMillis()));
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        InetSocketAddress target = resolveNodeAddress(nodeName);
        if (target == null) {
            return false;
        }
        Message response = sendRequest(target, "G", "", Set.of("H"));
        if (response == null || response.type != 'H' || response.stringFields.isEmpty()) {
            return false;
        }
        rememberNode(response.stringFields.get(0), target);
        return nodeName.equals(response.stringFields.get(0));
    }

    @Override
    public void pushRelay(String nodeName) {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        if (dataStore.containsKey(key)) {
            return true;
        }
        for (NodeCandidate candidate : getClosestKnownNodes(key, 3)) {
            if (candidate.name.equals(this.nodeName)) {
                continue;
            }
            Message response = sendRequest(candidate.address, "E", encodeString(key), Set.of("F"));
            if (response != null && !response.rawFields.isEmpty()) {
                String code = response.rawFields.get(0);
                if ("Y".equals(code)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String read(String key) throws Exception {
        if (dataStore.containsKey(key)) {
            return dataStore.get(key);
        }
        for (NodeCandidate candidate : getClosestKnownNodes(key, 3)) {
            if (candidate.name.equals(this.nodeName)) {
                continue;
            }
            Message response = sendRequest(candidate.address, "R", encodeString(key), Set.of("S"));
            if (response == null || response.rawFields.isEmpty()) {
                continue;
            }
            String code = response.rawFields.get(0);
            if ("Y".equals(code) && !response.stringFields.isEmpty()) {
                return response.stringFields.get(0);
            }
        }
        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        List<NodeCandidate> candidates = getClosestKnownNodes(key, 3);
        boolean success = false;

        for (NodeCandidate candidate : candidates) {
            if (candidate.name.equals(this.nodeName)) {
                String result = storeValueLocally(key, value);
                success = success || "A".equals(result) || "R".equals(result);
            } else {
                String payload = encodeString(key) + encodeString(value);
                Message response = sendRequest(candidate.address, "W", payload, Set.of("X"));
                if (response != null && !response.rawFields.isEmpty()) {
                    String code = response.rawFields.get(0);
                    if ("A".equals(code) || "R".equals(code)) {
                        success = true;
                    }
                }
            }
        }

        return success;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        List<NodeCandidate> candidates = getClosestKnownNodes(key, 3);
        boolean success = false;

        for (NodeCandidate candidate : candidates) {
            if (candidate.name.equals(this.nodeName)) {
                String result = compareAndSwapLocally(key, currentValue, newValue);
                success = success || "A".equals(result) || "R".equals(result);
            } else {
                String payload = encodeString(key) + encodeString(currentValue) + encodeString(newValue);
                Message response = sendRequest(candidate.address, "C", payload, Set.of("D"));
                if (response != null && !response.rawFields.isEmpty()) {
                    String code = response.rawFields.get(0);
                    if ("A".equals(code) || "R".equals(code)) {
                        success = true;
                    }
                }
            }
        }

        return success;
    }

    private void receiveLoop() {
        byte[] buffer = new byte[BUFFER_SIZE];
        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String text = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
                IncomingMessage incoming = new IncomingMessage(packet.getAddress(), packet.getPort(), text);
                handleIncoming(incoming);
            } catch (SocketException e) {
                running = false;
            } catch (SocketTimeoutException ignored) {
            } catch (Exception ignored) {
            }
        }
    }

    private void handleIncoming(IncomingMessage incoming) throws Exception {
        Message message = parseMessage(incoming.text);
        if (message == null) {
            return;
        }

        if (isResponseType(message.type)) {
            RelayContext relayContext = relayContexts.remove(message.txId);
            if (relayContext != null) {
                String rewritten = relayContext.relayTxId + incoming.text.substring(2);
                sendRaw(relayContext.returnAddress, rewritten);
                return;
            }

            BlockingQueue<IncomingMessage> queue = pendingResponses.get(message.txId);
            if (queue != null) {
                queue.offer(incoming);
            }
            return;
        }

        String duplicateKey = duplicateKey(incoming.address, incoming.port, message.txId);
        String cachedResponse = duplicateResponses.get(duplicateKey);
        if (cachedResponse != null) {
            sendRaw(new InetSocketAddress(incoming.address, incoming.port), cachedResponse);
            return;
        }

        String response = switch (message.type) {
            case 'G' -> buildResponse(message.txId, "H", encodeString(nodeName));
            case 'N' -> handleNearestRequest(message);
            case 'E' -> handleExistsRequest(message);
            case 'R' -> handleReadRequest(message);
            case 'W' -> handleWriteRequest(message);
            case 'C' -> handleCasRequest(message);
            case 'V' -> handleRelayRequest(message, incoming);
            case 'I' -> null;
            default -> null;
        };

        if (response != null) {
            duplicateResponses.put(duplicateKey, response);
            sendRaw(new InetSocketAddress(incoming.address, incoming.port), response);
        }
    }

    private String handleNearestRequest(Message message) throws Exception {
        if (message.rawFields.isEmpty()) {
            return null;
        }
        byte[] targetHash = hexToBytes(message.rawFields.get(0));
        List<NodeCandidate> candidates = getClosestKnownNodes(targetHash, 3);
        StringBuilder payload = new StringBuilder();
        for (NodeCandidate candidate : candidates) {
            payload.append(encodeString(candidate.name));
            payload.append(encodeString(candidate.address.getAddress().getHostAddress() + ":" + candidate.address.getPort()));
        }
        return buildResponse(message.txId, "O", payload.toString());
    }

    private String handleExistsRequest(Message message) throws Exception {
        if (message.stringFields.isEmpty()) {
            return null;
        }
        String key = message.stringFields.get(0);
        String code = statusForKey(key, dataStore.containsKey(key));
        return buildResponse(message.txId, "F", code);
    }

    private String handleReadRequest(Message message) throws Exception {
        if (message.stringFields.isEmpty()) {
            return null;
        }
        String key = message.stringFields.get(0);
        boolean stored = dataStore.containsKey(key);
        String status = statusForKey(key, stored);
        if ("Y".equals(status)) {
            return buildResponse(message.txId, "S", "Y " + encodeString(dataStore.get(key)));
        }
        return buildResponse(message.txId, "S", status);
    }

    private String handleWriteRequest(Message message) throws Exception {
        if (message.stringFields.size() < 2) {
            return null;
        }
        String code = storeValueLocally(message.stringFields.get(0), message.stringFields.get(1));
        return buildResponse(message.txId, "X", code);
    }

    private String handleCasRequest(Message message) throws Exception {
        if (message.stringFields.size() < 3) {
            return null;
        }
        String code = compareAndSwapLocally(message.stringFields.get(0), message.stringFields.get(1), message.stringFields.get(2));
        return buildResponse(message.txId, "D", code);
    }

    private String handleRelayRequest(Message message, IncomingMessage incoming) throws Exception {
        if (message.stringFields.isEmpty() || message.embeddedMessage == null) {
            return null;
        }

        String targetNode = message.stringFields.get(0);
        if (targetNode.equals(nodeName)) {
            Message embedded = parseMessage(message.embeddedMessage);
            if (embedded == null || !isRequestType(embedded.type)) {
                return null;
            }

            Message localRequest = new Message(
                    message.txId,
                    embedded.type,
                    embedded.rawFields,
                    embedded.stringFields,
                    embedded.embeddedMessage
            );

            return switch (localRequest.type) {
                case 'G' -> buildResponse(message.txId, "H", encodeString(nodeName));
                case 'N' -> handleNearestRequest(localRequest);
                case 'E' -> handleExistsRequest(localRequest);
                case 'R' -> handleReadRequest(localRequest);
                case 'W' -> handleWriteRequest(localRequest);
                case 'C' -> handleCasRequest(localRequest);
                default -> null;
            };
        }

        InetSocketAddress nextHop = resolveNodeAddress(targetNode);
        if (nextHop == null) {
            return null;
        }

        Message embedded = parseMessage(message.embeddedMessage);
        if (embedded == null || !isRequestType(embedded.type)) {
            return null;
        }

        String forwardedTx = generateTxId();
        relayContexts.put(forwardedTx, new RelayContext(message.txId, new InetSocketAddress(incoming.address, incoming.port)));
        String forwarded = forwardedTx + message.embeddedMessage.substring(2);
        sendRaw(nextHop, forwarded);
        return null;
    }

    private String storeValueLocally(String key, String value) throws Exception {
        if (key.startsWith("N:")) {
            InetSocketAddress address = parseAddress(value);
            if (address == null) {
                return "X";
            }
            if (nodeName.equals(key)) {
                knownNodes.put(key, address);
                selfAddress = address;
                return dataStore.put(key, value) == null ? "A" : "R";
            }

            byte[] keyHash = HashID.computeHashID(key);
            int myDistance = distance(nodeHash, keyHash);
            int sameDistanceCount = 0;
            int strictlyCloser = 0;

            for (String knownNode : knownNodes.keySet()) {
                int d = distance(HashID.computeHashID(knownNode), keyHash);
                if (d < myDistance) {
                    strictlyCloser++;
                }
                if (d == myDistance) {
                    sameDistanceCount++;
                }
            }

            if (strictlyCloser >= 3 || sameDistanceCount >= 3) {
                return "X";
            }

            knownNodes.put(key, address);
            return dataStore.put(key, value) == null ? "A" : "R";
        }

        boolean stored = dataStore.containsKey(key);
        String status = statusForKey(key, stored);
        if ("?".equals(status)) {
            return "X";
        }
        dataStore.put(key, value);
        return stored ? "R" : "A";
    }

    private String compareAndSwapLocally(String key, String currentValue, String newValue) throws Exception {
        boolean stored = dataStore.containsKey(key);
        String status = statusForKey(key, stored);
        if ("?".equals(status)) {
            return "X";
        }

        synchronized (dataStore) {
            if (!dataStore.containsKey(key)) {
                dataStore.put(key, newValue);
                return "A";
            }
            if (Objects.equals(dataStore.get(key), currentValue)) {
                dataStore.put(key, newValue);
                return "R";
            }
            return "N";
        }
    }

    private String statusForKey(String key, boolean storedHere) throws Exception {
        List<NodeCandidate> closest = getClosestKnownNodes(key, 3);
        boolean amClose = false;
        for (NodeCandidate candidate : closest) {
            if (candidate.name.equals(nodeName)) {
                amClose = true;
                break;
            }
        }
        if (storedHere) {
            return "Y";
        }
        return amClose ? "N" : "?";
    }

    private List<NodeCandidate> getClosestKnownNodes(String key, int limit) throws Exception {
        return getClosestKnownNodes(HashID.computeHashID(key), limit);
    }

    private List<NodeCandidate> getClosestKnownNodes(byte[] targetHash, int limit) throws Exception {
        List<NodeCandidate> candidates = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        if (selfAddress != null) {
            candidates.add(new NodeCandidate(nodeName, selfAddress));
            seen.add(nodeName);
        }

        for (Map.Entry<String, InetSocketAddress> entry : knownNodes.entrySet()) {
            if (seen.add(entry.getKey())) {
                candidates.add(new NodeCandidate(entry.getKey(), entry.getValue()));
            }
        }

        candidates.sort(Comparator
                .comparingInt((NodeCandidate candidate) -> {
                    try {
                        return distance(HashID.computeHashID(candidate.name), targetHash);
                    } catch (Exception e) {
                        return Integer.MAX_VALUE;
                    }
                })
                .thenComparing(candidate -> candidate.name)
        );

        if (candidates.size() > limit) {
            return new ArrayList<>(candidates.subList(0, limit));
        }
        return candidates;
    }

    private InetSocketAddress resolveNodeAddress(String targetNode) throws Exception {
        if (targetNode == null) {
            return null;
        }
        if (targetNode.equals(nodeName)) {
            return selfAddress;
        }
        InetSocketAddress known = knownNodes.get(targetNode);
        if (known != null) {
            return known;
        }

        for (NodeCandidate candidate : getClosestKnownNodes(targetNode, 3)) {
            if (candidate.name.equals(this.nodeName)) {
                continue;
            }
            Message response = sendRequest(candidate.address, "N", bytesToHex(HashID.computeHashID(targetNode)), Set.of("O"));
            if (response != null) {
                rememberNearestEntries(response);
            }
        }

        return knownNodes.get(targetNode);
    }

    private void rememberNearestEntries(Message response) {
        for (int i = 0; i + 1 < response.stringFields.size(); i += 2) {
            String key = response.stringFields.get(i);
            String value = response.stringFields.get(i + 1);
            InetSocketAddress address = parseAddress(value);
            if (key.startsWith("N:") && address != null) {
                rememberNode(key, address);
            }
        }
    }

    private void rememberNode(String key, InetSocketAddress address) {
        if (key != null && key.startsWith("N:") && address != null) {
            knownNodes.put(key, address);
        }
    }

    private Message sendRequest(InetSocketAddress destination, String type, String payload, Set<String> expectedTypes) throws Exception {
        String baseTx = generateTxId();
        String request = baseTx + " " + type + (payload.isEmpty() ? "" : " " + payload);

        String wrappedRequest = request;
        InetSocketAddress nextHop = destination;
        if (!relayStack.isEmpty()) {
            for (String relayNode : relayStack) {
                wrappedRequest = generateTxId() + " V " + encodeString(relayNode) + wrappedRequest;
                nextHop = resolveNodeAddress(relayNode);
                if (nextHop == null) {
                    return null;
                }
            }
        }

        String waitTx = wrappedRequest.substring(0, 2);
        BlockingQueue<IncomingMessage> queue = new LinkedBlockingQueue<>();
        pendingResponses.put(waitTx, queue);

        try {
            for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
                sendRaw(nextHop, wrappedRequest);
                IncomingMessage incoming = queue.poll(RESPONSE_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (incoming == null) {
                    continue;
                }
                Message response = parseMessage(incoming.text);
                if (response != null && expectedTypes.contains(String.valueOf(response.type))) {
                    if (response.type == 'H' && !response.stringFields.isEmpty()) {
                        rememberNode(response.stringFields.get(0), destination);
                    }
                    if (response.type == 'O') {
                        rememberNearestEntries(response);
                    }
                    return response;
                }
            }
        } finally {
            pendingResponses.remove(waitTx);
        }

        return null;
    }

    private void sendRaw(InetSocketAddress destination, String text) throws IOException {
        byte[] data = text.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, destination.getAddress(), destination.getPort());
        socket.send(packet);
    }

    private Message parseMessage(String text) throws Exception {
        int firstSpace = text.indexOf(' ');
        if (firstSpace < 2 || text.length() < firstSpace + 2) {
            return null;
        }

        String txId = text.substring(0, firstSpace);
        if (txId.length() != 2 || txId.charAt(0) == ' ' || txId.charAt(1) == ' ') {
            return null;
        }

        int typePos = firstSpace + 1;
        char type = text.charAt(typePos);
        int cursor = typePos + 1;
        if (cursor < text.length() && text.charAt(cursor) == ' ') {
            cursor++;
        }

        List<String> rawFields = new ArrayList<>();
        List<String> stringFields = new ArrayList<>();
        String embedded = null;

        switch (type) {
            case 'G', 'I' -> {
            }
            case 'H', 'E', 'R' -> stringFields.add(parseEncodedString(text, new Cursor(cursor)));
            case 'N' -> rawFields.add(text.substring(cursor).trim());
            case 'F', 'X', 'D' -> rawFields.add(text.substring(cursor).trim());
            case 'S' -> {
                String code = text.substring(cursor, Math.min(cursor + 1, text.length()));
                rawFields.add(code);
                int next = cursor + 1;
                if ("Y".equals(code) && next < text.length() && text.charAt(next) == ' ') {
                    Cursor valueCursor = new Cursor(next + 1);
                    stringFields.add(parseEncodedString(text, valueCursor));
                }
            }
            case 'W' -> {
                Cursor c = new Cursor(cursor);
                stringFields.add(parseEncodedString(text, c));
                stringFields.add(parseEncodedString(text, c));
            }
            case 'C' -> {
                Cursor c = new Cursor(cursor);
                stringFields.add(parseEncodedString(text, c));
                stringFields.add(parseEncodedString(text, c));
                stringFields.add(parseEncodedString(text, c));
            }
            case 'O' -> {
                Cursor c = new Cursor(cursor);
                while (c.position < text.length()) {
                    stringFields.add(parseEncodedString(text, c));
                }
            }
            case 'V' -> {
                Cursor c = new Cursor(cursor);
                stringFields.add(parseEncodedString(text, c));
                embedded = text.substring(c.position);
            }
            default -> {
                return null;
            }
        }

        return new Message(txId, type, rawFields, stringFields, embedded);
    }

    private String parseEncodedString(String text, Cursor cursor) throws Exception {
        int firstSpace = text.indexOf(' ', cursor.position);
        if (firstSpace < 0) {
            throw new IOException("Malformed CRN string");
        }

        int expectedSpaces = Integer.parseInt(text.substring(cursor.position, firstSpace));
        int start = firstSpace + 1;
        int spacesSeen = 0;
        int i = start;
        while (i < text.length()) {
            if (text.charAt(i) == ' ') {
                spacesSeen++;
                if (spacesSeen == expectedSpaces + 1) {
                    String value = text.substring(start, i);
                    cursor.position = i + 1;
                    return value;
                }
            }
            i++;
        }
        throw new IOException("Malformed CRN string");
    }

    private String encodeString(String value) {
        long spaces = value.chars().filter(ch -> ch == ' ').count();
        return spaces + " " + value + " ";
    }

    private String buildResponse(String txId, String type, String payload) {
        return txId + " " + type + (payload.isEmpty() ? "" : " " + payload);
    }

    private static String duplicateKey(InetAddress address, int port, String txId) {
        return address.getHostAddress() + ":" + port + ":" + txId;
    }

    private static boolean isResponseType(char type) {
        return type == 'H' || type == 'O' || type == 'F' || type == 'S' || type == 'X' || type == 'D';
    }

    private static boolean isRequestType(char type) {
        return type == 'G' || type == 'N' || type == 'E' || type == 'R' || type == 'W' || type == 'C' || type == 'V';
    }

    private String generateTxId() {
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        while (true) {
            int a = ThreadLocalRandom.current().nextInt(alphabet.length());
            int b = ThreadLocalRandom.current().nextInt(alphabet.length());
            String tx = "" + alphabet.charAt(a) + alphabet.charAt(b);
            if (!pendingResponses.containsKey(tx) && !relayContexts.containsKey(tx)) {
                return tx;
            }
        }
    }

    private static InetSocketAddress parseAddress(String value) {
        int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon == value.length() - 1) {
            return null;
        }
        try {
            String host = value.substring(0, colon);
            int port = Integer.parseInt(value.substring(colon + 1));
            return new InetSocketAddress(InetAddress.getByName(host), port);
        } catch (Exception e) {
            return null;
        }
    }

    private static int distance(byte[] a, byte[] b) {
        for (int i = 0; i < a.length; i++) {
            int xor = (a[i] ^ b[i]) & 0xff;
            if (xor != 0) {
                return (i * 8) + Integer.numberOfLeadingZeros(xor) - 24;
            }
        }
        return 0;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            sb.append(String.format("%02x", value));
        }
        return sb.toString();
    }

    private static byte[] hexToBytes(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int index = i * 2;
            bytes[i] = (byte) Integer.parseInt(hex.substring(index, index + 2), 16);
        }
        return bytes;
    }

    private static final class Cursor {
        private int position;

        private Cursor(int position) {
            this.position = position;
        }
    }

    private static final class Message {
        private final String txId;
        private final char type;
        private final List<String> rawFields;
        private final List<String> stringFields;
        private final String embeddedMessage;

        private Message(String txId, char type, List<String> rawFields, List<String> stringFields, String embeddedMessage) {
            this.txId = txId;
            this.type = type;
            this.rawFields = rawFields;
            this.stringFields = stringFields;
            this.embeddedMessage = embeddedMessage;
        }
    }

    private static final class IncomingMessage {
        private final InetAddress address;
        private final int port;
        private final String text;

        private IncomingMessage(InetAddress address, int port, String text) {
            this.address = address;
            this.port = port;
            this.text = text;
        }
    }

    private static final class RelayContext {
        private final String relayTxId;
        private final InetSocketAddress returnAddress;

        private RelayContext(String relayTxId, InetSocketAddress returnAddress) {
            this.relayTxId = relayTxId;
            this.returnAddress = returnAddress;
        }
    }

    private static final class NodeCandidate {
        private final String name;
        private final InetSocketAddress address;

        private NodeCandidate(String name, InetSocketAddress address) {
            this.name = name;
            this.address = address;
        }
    }
}
