package com.codingful.tandem.benchmark;

import org.testcontainers.DockerClientFactory;

/**
 * Freezes and thaws a running container (LLD-benchmark §3.1). {@code docker pause} sends the
 * container's processes {@code SIGSTOP}: the host keeps the ports open and the TCP connections
 * established, and nothing on the other side answers: a hung broker rather than a closed socket,
 * which is the harder of the two for a client to notice and therefore the one worth injecting.
 *
 * <p>Deliberately not a container stop: Testcontainers maps a fresh random host port on restart, so a
 * stopped broker is one no client could reconnect to, whatever its recovery logic does.
 */
final class DockerPause {

    private DockerPause() {
    }

    static void pause(String containerId) {
        DockerClientFactory.instance().client().pauseContainerCmd(containerId).exec();
    }

    static void unpause(String containerId) {
        DockerClientFactory.instance().client().unpauseContainerCmd(containerId).exec();
    }
}
