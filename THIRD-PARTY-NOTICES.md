# Third-Party Notices

Tandem is licensed under the Apache License, Version 2.0 (see [LICENSE](LICENSE)).

Tandem publishes **standard, non-shaded JARs**: third-party libraries are *not*
bundled into Tandem's artifacts. Consumers resolve them separately from Maven
Central, each under its own license. This file lists the third-party libraries
that reach a consumer's **compile / runtime classpath** when depending on a
Tandem module — it is provided as a convenience, for informational purposes only.

Two distributions are the exception, and each has a section of its own below:
the `tandem-cli` binary, which links its dependencies statically, and the
`tandem-relay` container image and executable jar, which bundle their whole
runtime classpath.

Dependencies used only to build and test Tandem itself (e.g. JUnit, AssertJ,
HikariCP, HdrHistogram, slf4j-simple) are **not** listed: they never reach a
consumer. Note that `tandem-test` is the one published module whose *purpose* is
testing, so the libraries it needs — Testcontainers and the PostgreSQL JDBC
driver among them — do reach whoever declares it, and are listed below.

## Runtime footprint by module

| Module         | Redistributed runtime dependencies                  |
|----------------|-----------------------------------------------------|
| `tandem-core`  | none — JDK only                                     |
| `tandem-jdbc`  | none beyond `tandem-core`: the PostgreSQL JDBC driver is test-only, and `compileOnly` for the `pg_notify` wakeup adapter, so neither reaches a consumer |
| `tandem-cloudevents` | `cloudevents-core` |
| `tandem-kafka` | `kafka-clients`, `cloudevents-kafka`, `cloudevents-core` (via `tandem-cloudevents`), `slf4j-api` |
| `tandem-rabbitmq` | `amqp-client`, `cloudevents-core` (via `tandem-cloudevents`), `slf4j-api` |
| `tandem-spring-producer` | none beyond `tandem-jdbc` — **Spring and Jackson are `compileOnly`**, so the application's own versions are used and none is dragged in |
| `tandem-spring-relay` | none beyond `tandem-jdbc` — Spring is `compileOnly`, as above, and so are `tandem-kafka` and `tandem-micrometer`: the transport is a port, so the application declares the publish adapter it uses and inherits no other one |
| `tandem-micrometer` | `micrometer-core` |
| `tandem-tracing-otel` | `opentelemetry-api` |
| `tandem-admin` | none beyond `tandem-jdbc` — Spring is `compileOnly`, as above. Of Jackson it compiles against the **annotations only** (`jackson-annotations`, also `compileOnly`), never a JSON binding: Boot 3 supplies Jackson 2 and Boot 4 supplies Jackson 3, and the annotations are what both carry |
| `tandem-test`  | `testcontainers-postgresql`, `testcontainers-kafka`, `kafka-clients`, and the `postgresql` JDBC driver (runtime). Meant for a consumer's **test** scope, but its POM declares them at compile/runtime scope, so they are redistributed like any other dependency. Testcontainers itself brings a substantial transitive tree (docker-java, Jackson, commons-compress, …) not enumerated here |
| `tandem-bom`   | none — a POM with no code, publishing only version constraints |
| `tandem-relay` | not a Maven artifact: an application whose image and jar **bundle** every library they run on, listed in full in [its own section](#tandem-relay-container-image-and-executable-jar) |

## Dependencies

The complete set of third-party libraries a consumer may pull onto the
compile / runtime classpath is:

| Library                                 | Version | License      |
|-----------------------------------------|---------|--------------|
| org.apache.kafka:kafka-clients          | 3.9.2   | Apache-2.0   |
| io.cloudevents:cloudevents-kafka        | 4.1.1   | Apache-2.0   |
| io.cloudevents:cloudevents-core         | 4.1.1   | Apache-2.0   |
| com.rabbitmq:amqp-client                | 5.25.0  | Apache-2.0   |
| org.slf4j:slf4j-api                     | 2.0.16  | MIT          |
| org.testcontainers:postgresql           | 1.21.4  | MIT          |
| org.testcontainers:kafka                | 1.21.4  | MIT          |
| org.postgresql:postgresql               | 42.7.12 | BSD-2-Clause |
| io.micrometer:micrometer-core            | 1.13.6  | Apache-2.0   |
| io.opentelemetry:opentelemetry-api       | 1.38.0  | Apache-2.0   |

`cloudevents-core` is declared directly by `tandem-cloudevents`, and `cloudevents-kafka`
pulls it in transitively as well. The three
entries below `slf4j-api` reach only consumers of `tandem-test`; each Testcontainers
module additionally pulls `org.testcontainers:testcontainers` and its own transitive
dependencies, which are not enumerated here.

## `tandem-cli` (Go binary)

`tandem-cli` is not a JVM module — it is a separately versioned Go binary
(`tandem-cli/`, its own `go.mod`, LLD-cli.md §2/§9.1), so the "reaches a consumer's
classpath" framing above does not apply to it. The equivalent inheritance event is
**static linking**: every dependency below is compiled directly into the distributed
`tandem-cli` binary (Go has no dynamic linking for these), so all of them, not just
direct requires, reach whoever downloads a release. The list is the actual build-info
dependency set of a compiled binary (`go version -m`), not `go.mod`'s full module graph,
which also lists modules only `go generate`'s code-generator tool needs and that never
compile into `tandem-cli` itself.

| Library                                   | Version | License      |
|--------------------------------------------|---------|--------------|
| github.com/spf13/cobra                     | 1.10.2  | Apache-2.0   |
| github.com/spf13/pflag                     | 1.0.9   | BSD-3-Clause |
| github.com/inconshreveable/mousetrap       | 1.1.0   | Apache-2.0   |
| github.com/oapi-codegen/runtime            | 1.6.0   | Apache-2.0   |
| github.com/google/uuid                     | 1.6.0   | BSD-3-Clause |
| github.com/apapsch/go-jsonmerge/v2         | 2.0.0   | MIT          |

`mousetrap` links in only on Windows builds (it is cobra's own conditional dependency
for detecting a console-less launch there); the other five link in on every platform
`goreleaser` cross-compiles for (darwin/linux/windows × amd64/arm64, LLD-cli.md §9).
`oapi-codegen` itself (the code generator) does **not** appear here: it runs at
`go generate` time only, from a separate tools module (`tandem-cli/tools/`) kept apart
specifically so its own, larger dependency tree — and its higher minimum Go version —
never reaches the shipped binary or this table.

## `tandem-relay` (container image and executable jar)

`tandem-relay` is not a library either: it is an application, released as a container
image and an executable jar on its own `relay-v*` tags (LLD-relay §7), and both
**bundle** their whole runtime classpath. Every library below sits inside the jar's
`BOOT-INF/lib` and inside the image, so all of them, not only direct dependencies,
reach whoever pulls the image or downloads the jar. The list is generated from the
`BOOT-INF/lib` of the jar the image is built from, never kept by hand:
`./gradlew :tandem-relay:updateThirdPartyNotices` rewrites it, and `check` fails when
it no longer matches the jar. Licenses are read from each library's POM; where a POM
names several, they are alternatives, shown joined by `OR`.

Not listed: Tandem's own modules (`com.codingful:tandem-*`, Apache-2.0) at the release
named above the table, and what the base image brings (Eclipse Temurin's Java runtime
on Ubuntu), which carries its own notices inside the image. This file and
[LICENSE](LICENSE) are copied into the jar's `META-INF` and into the image's
`/licenses`.

<!-- BEGIN tandem-relay libraries: generated by ./gradlew :tandem-relay:updateThirdPartyNotices, do not edit by hand -->

Built on Tandem 0.11.1. 74 third-party libraries:

| Library                                                         | Version  | License                                              |
|-----------------------------------------------------------------|----------|------------------------------------------------------|
| at.yawk.lz4:lz4-java                                            | 1.11.1   | Apache-2.0                                           |
| ch.qos.logback:logback-classic                                  | 1.5.38   | EPL-2.0 OR LGPL-2.1-only                             |
| ch.qos.logback:logback-core                                     | 1.5.38   | EPL-2.0 OR LGPL-2.1-only                             |
| com.fasterxml.jackson.core:jackson-annotations                  | 2.21     | Apache-2.0                                           |
| com.github.luben:zstd-jni                                       | 1.5.6-10 | BSD-2-Clause                                         |
| com.zaxxer:HikariCP                                             | 7.0.2    | Apache-2.0                                           |
| commons-logging:commons-logging                                 | 1.3.6    | Apache-2.0                                           |
| io.cloudevents:cloudevents-api                                  | 4.1.1    | Apache-2.0                                           |
| io.cloudevents:cloudevents-core                                 | 4.1.1    | Apache-2.0                                           |
| io.cloudevents:cloudevents-kafka                                | 4.1.1    | Apache-2.0                                           |
| io.micrometer:micrometer-commons                                | 1.17.1   | Apache-2.0                                           |
| io.micrometer:micrometer-core                                   | 1.17.1   | Apache-2.0                                           |
| io.micrometer:micrometer-jakarta9                               | 1.17.1   | Apache-2.0                                           |
| io.micrometer:micrometer-observation                            | 1.17.1   | Apache-2.0                                           |
| io.micrometer:micrometer-registry-prometheus                    | 1.17.1   | Apache-2.0                                           |
| io.prometheus:prometheus-metrics-config                         | 1.7.0    | Apache-2.0                                           |
| io.prometheus:prometheus-metrics-core                           | 1.7.0    | Apache-2.0                                           |
| io.prometheus:prometheus-metrics-exposition-formats             | 1.7.0    | Apache-2.0                                           |
| io.prometheus:prometheus-metrics-exposition-textformats         | 1.7.0    | Apache-2.0                                           |
| io.prometheus:prometheus-metrics-model                          | 1.7.0    | Apache-2.0                                           |
| io.prometheus:prometheus-metrics-tracer-common                  | 1.7.0    | Apache-2.0                                           |
| jakarta.annotation:jakarta.annotation-api                       | 3.0.0    | EPL-2.0 OR GPL-2.0-only WITH Classpath-exception-2.0 |
| org.apache.kafka:kafka-clients                                  | 4.2.1    | Apache-2.0                                           |
| org.apache.logging.log4j:log4j-api                              | 2.25.5   | Apache-2.0                                           |
| org.apache.logging.log4j:log4j-to-slf4j                         | 2.25.5   | Apache-2.0                                           |
| org.apache.tomcat.embed:tomcat-embed-core                       | 11.0.24  | Apache-2.0                                           |
| org.apache.tomcat.embed:tomcat-embed-el                         | 11.0.24  | Apache-2.0                                           |
| org.apache.tomcat.embed:tomcat-embed-websocket                  | 11.0.24  | Apache-2.0                                           |
| org.checkerframework:checker-qual                               | 3.55.1   | MIT                                                  |
| org.hdrhistogram:HdrHistogram                                   | 2.2.2    | CC0-1.0 OR BSD-2-Clause                              |
| org.jspecify:jspecify                                           | 1.0.1    | Apache-2.0                                           |
| org.postgresql:postgresql                                       | 42.7.13  | BSD-2-Clause                                         |
| org.slf4j:jul-to-slf4j                                          | 2.0.18   | MIT                                                  |
| org.slf4j:slf4j-api                                             | 2.0.18   | MIT                                                  |
| org.springframework.boot:spring-boot                            | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-actuator                   | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-actuator-autoconfigure     | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-autoconfigure              | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-health                     | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-http-converter             | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-jackson                    | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-jarmode-tools              | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-jdbc                       | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-micrometer-metrics         | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-micrometer-observation     | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-persistence                | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-servlet                    | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-sql                        | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter                    | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-actuator           | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-jackson            | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-jdbc               | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-logging            | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-micrometer-metrics | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-tomcat             | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-tomcat-runtime     | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-starter-webmvc             | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-tomcat                     | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-transaction                | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-web-server                 | 4.1.1    | Apache-2.0                                           |
| org.springframework.boot:spring-boot-webmvc                     | 4.1.1    | Apache-2.0                                           |
| org.springframework:spring-aop                                  | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-beans                                | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-context                              | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-core                                 | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-expression                           | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-jdbc                                 | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-tx                                   | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-web                                  | 7.0.9    | Apache-2.0                                           |
| org.springframework:spring-webmvc                               | 7.0.9    | Apache-2.0                                           |
| org.xerial.snappy:snappy-java                                   | 1.1.10.7 | Apache-2.0                                           |
| org.yaml:snakeyaml                                              | 2.6      | Apache-2.0                                           |
| tools.jackson.core:jackson-core                                 | 3.1.5    | Apache-2.0                                           |
| tools.jackson.core:jackson-databind                             | 3.1.5    | Apache-2.0                                           |

<!-- END tandem-relay libraries -->

## License texts

### Apache License 2.0

Applies to: `kafka-clients`, `cloudevents-kafka`, `cloudevents-core` — and, in
`tandem-cli`'s statically-linked binary, `cobra`, `mousetrap`, `oapi-codegen/runtime`,
and, in `tandem-relay`, every library its list marks `Apache-2.0`.

The full text of the Apache License, Version 2.0 is available in [LICENSE](LICENSE)
and at https://www.apache.org/licenses/LICENSE-2.0.

### BSD 3-Clause License

Applies to, in `tandem-cli`'s statically-linked binary: `pflag`, `google/uuid`.

```
Copyright (c) 2012 Alex Ogier. All rights reserved.
Copyright (c) 2012 The Go Authors. All rights reserved.
Copyright (c) 2009,2014 Google Inc. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

Each project's copyright line above is its own; the terms are identical BSD-3-Clause
boilerplate in both.

### MIT License

Applies to: `slf4j-api` and, in `tandem-cli`'s statically-linked binary,
`go-jsonmerge/v2` (© 2016-2019 Artur Kraev — the same MIT terms below, different
copyright holder). In `tandem-relay`, every library its list marks `MIT`, under
the same terms and each with its own copyright holder.

```
Copyright (c) 2004-2023 QOS.ch Sarl (Switzerland)
All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

### BSD 2-Clause License

Applies to `org.postgresql:postgresql` and, in `tandem-relay`, every library its
list marks `BSD-2-Clause`, under the same terms and each with its own copyright
holder. `HdrHistogram` is offered under this license or dedicated to the public
domain under CC0-1.0 (https://creativecommons.org/publicdomain/zero/1.0/), which
asks nothing of a redistributor.

```
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
```

### Eclipse Public License 2.0

Applies to, in `tandem-relay`: every library its list marks `EPL-2.0`, each of them
offered under a choice of licenses. `tandem-relay` redistributes them under
EPL-2.0: `logback-classic` and `logback-core` (also offered under LGPL-2.1-only),
and `jakarta.annotation-api` (also offered under GPL-2.0-only with the Classpath
Exception).

The full text of the Eclipse Public License 2.0 is available at
https://www.eclipse.org/legal/epl-2.0/. The source code of these libraries is
available from their projects: https://github.com/qos-ch/logback and
https://github.com/jakartaee/common-annotations-api.
