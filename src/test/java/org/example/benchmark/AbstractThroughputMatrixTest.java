// SPDX-License-Identifier: Apache-2.0
package org.example.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import org.example.client.HelidonThroughputClient;
import org.example.client.NettyThroughputClient;
import org.example.client.ThroughputClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.DisplayName.class)
abstract class AbstractThroughputMatrixTest {
    protected enum Impl { NETTY, HELIDON }

    protected record Result(Impl server, Impl client, long messages, int payloadBytes,
                            double seconds, double mbps) { }

    protected static final long MESSAGE_COUNT = 1_000;
    protected static final int SIZE_5KB = 5 * 1024;
    protected static final int SIZE_50KB = 50 * 1024;
    protected static final int SIZE_500KB = 500 * 1024;
    protected static final int SIZE_1MB = 1024 * 1024;

    private final List<Result> results = Collections.synchronizedList(new ArrayList<>());

    protected abstract ServerHandle startServer(Impl impl) throws Exception;

    protected ThroughputClient createClient(Impl impl, String host, int port) {
        return switch (impl) {
            case NETTY -> new NettyThroughputClient(host, port);
            case HELIDON -> new HelidonThroughputClient("http://" + host + ":" + port);
        };
    }

    protected static Stream<Arguments> combinations() {
        return Stream.of(Impl.NETTY, Impl.HELIDON)
                .flatMap(server -> Stream.of(Impl.NETTY, Impl.HELIDON)
                        .flatMap(client -> Stream.of(SIZE_5KB, SIZE_50KB, SIZE_500KB, SIZE_1MB)
                                .map(size -> Arguments.of(server, client, size))));
    }

    @ParameterizedTest(name = "{0} server ↔ {1} client [{2} bytes]")
    @MethodSource("combinations")
    void clientServerMatrix(Impl serverImpl, Impl clientImpl, int payloadBytes) throws Exception {
        try (ServerHandle handle = startServer(serverImpl)) {
            assertTrue(handle.port() > 0, "Server port should be assigned");

            ThroughputClient client = createClient(clientImpl, handle.host(), handle.port());
            long started = System.nanoTime();
            client.run(MESSAGE_COUNT, payloadBytes);
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            double totalBytes = MESSAGE_COUNT * (double) payloadBytes;
            double mbps = seconds > 0 ? (totalBytes / (1024.0 * 1024.0)) / seconds : 0.0;

            results.add(new Result(serverImpl, clientImpl, MESSAGE_COUNT, payloadBytes, seconds, mbps));
        }
    }

    @AfterAll
    void printSummary() {
        if (results.isEmpty()) {
            return;
        }
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.printf(" Integration test throughput summary (%s)%n", summaryLabel());
        System.out.println("   server   client   payloadKB   messages   duration(s)   MB/s");
        System.out.println("--------------------------------------------------------------");
        results.stream()
                .sorted(Comparator
                        .comparing(Result::server)
                        .thenComparing(Result::client)
                        .thenComparing(Result::payloadBytes))
                .forEach(r -> System.out.printf(Locale.ROOT,
                        " %7s %8s %10.1f %10d %12.3f %8.2f%n",
                        r.server(),
                        r.client(),
                        r.payloadBytes() / 1024.0,
                        r.messages(),
                        r.seconds(),
                        r.mbps()));
        System.out.println("══════════════════════════════════════════════════════════════");
    }

    protected String summaryLabel() {
        return getClass().getSimpleName();
    }

    protected static final class ServerHandle implements AutoCloseable {
        private final String host;
        private final int port;
        private final AutoCloseable closer;

        ServerHandle(String host, int port, AutoCloseable closer) {
            this.host = host;
            this.port = port;
            this.closer = closer;
        }

        String host() {
            return host;
        }

        int port() {
            return port;
        }

        @Override
        public void close() throws Exception {
            if (closer != null) {
                closer.close();
            }
        }
    }
}
