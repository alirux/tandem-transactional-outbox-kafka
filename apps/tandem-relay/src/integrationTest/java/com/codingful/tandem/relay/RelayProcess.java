package com.codingful.tandem.relay;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The application under test, started the way an operator starts it: {@code java -jar} on the built
 * jar, configured through its environment and nothing else (LLD-relay §8.2). The jar and the JVM come
 * from the build, which hands over the module's own Java launcher and one of its two jars.
 */
final class RelayProcess implements AutoCloseable {

    static final String READINESS_PATH = "/actuator/health/readiness";

    private static final String JAR = System.getProperty("tandem.relay.jar");
    private static final String JAVA = System.getProperty("tandem.relay.java");
    private static final Duration STARTUP = Duration.ofSeconds(90);
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private final Process process;
    private final Path output;
    private final int applicationPort;
    private final int managementPort;

    private RelayProcess(Process process, Path output, int applicationPort, int managementPort) {
        this.process = process;
        this.output = output;
        this.applicationPort = applicationPort;
        this.managementPort = managementPort;
    }

    /** Starts the application with exactly this environment, plus the two ports it listens on. */
    static RelayProcess start(Map<String, String> environment) {
        int applicationPort = freePort();
        int managementPort = freePort();
        try {
            Path output = Files.createTempFile("tandem-relay-it", ".log");
            ProcessBuilder builder = new ProcessBuilder(List.of(JAVA, "-jar", JAR));
            // Nothing inherited: whatever the test does not set, the application must not need.
            builder.environment().clear();
            builder.environment().putAll(environment);
            builder.environment().put("SERVER_PORT", Integer.toString(applicationPort));
            builder.environment().put("MANAGEMENT_SERVER_PORT", Integer.toString(managementPort));
            builder.redirectErrorStream(true).redirectOutput(output.toFile());
            return new RelayProcess(builder.start(), output, applicationPort, managementPort);
        } catch (IOException e) {
            throw new UncheckedIOException("could not start " + JAR, e);
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Blocks until the readiness probe answers 200, and fails with the application's output if it never does. */
    RelayProcess awaitReady() {
        long deadline = System.nanoTime() + STARTUP.toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new AssertionError("the application exited before becoming ready:\n" + output());
            }
            try {
                if (management(READINESS_PATH).statusCode() == 200) {
                    return this;
                }
            } catch (UncheckedIOException notListeningYet) {
                // The management port opens partway through startup.
            }
            pause();
        }
        throw new AssertionError("the application was not ready within " + STARTUP + ":\n" + output());
    }

    /** Waits for the process to end by itself and returns its exit code. */
    int awaitExit(Duration within) {
        try {
            if (!process.waitFor(within.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("the application was still running after " + within + ":\n" + output());
            }
            return process.exitValue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Asks the process to stop the way a container runtime does, with SIGTERM. */
    void requestStop() {
        process.destroy();
    }

    HttpResponse<String> application(String path) {
        return get(applicationPort, path);
    }

    HttpResponse<String> management(String path) {
        return get(managementPort, path);
    }

    private static HttpResponse<String> get(int port, String path) {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10)).GET().build();
        try {
            return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Everything the application wrote to its console so far. */
    String output() {
        try {
            return Files.readString(output);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void pause() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }
}
