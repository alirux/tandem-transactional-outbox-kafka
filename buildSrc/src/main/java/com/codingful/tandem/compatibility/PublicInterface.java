package com.codingful.tandem.compatibility;

/**
 * A public interface of a module's API (HLD-api-stability §3), with the kind its javadoc states.
 *
 * @param binaryName the interface's binary name, {@code Outer$Nested} for a nested one
 * @param kind       what its own javadoc states
 */
public record PublicInterface(String binaryName, StatedKind kind) {
}
