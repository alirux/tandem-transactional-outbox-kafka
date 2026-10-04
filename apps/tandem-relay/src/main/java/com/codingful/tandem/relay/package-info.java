/**
 * The standalone relay application (LLD-relay): a Spring Boot application whose whole job is to host
 * the relay that {@code tandem-spring-relay} wires and, optionally, the Admin API. It adds the three
 * things a library has no business deciding: a health verdict, deployment defaults, and a refusal to
 * start in a configuration that would do nothing.
 */
package com.codingful.tandem.relay;
