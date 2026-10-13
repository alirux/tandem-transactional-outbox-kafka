package com.codingful.tandem.compatibility;

/**
 * One run of oasdiff, as {@code checkOpenApi} judges it (LLD-api-stability §6).
 *
 * @param command  the command, as the gate's lines name it: {@code validate}, {@code breaking}, ...
 * @param exitCode the container's exit code: oasdiff's own, or Docker's when the container did not run
 * @param stdout   what oasdiff printed on its standard output, the JSON the gate reads
 * @param stderr   what it printed on its standard error, for the failure message
 */
public record OasdiffRun(String command, int exitCode, String stdout, String stderr) {
}
