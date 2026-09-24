## What this changes

<!-- What the change does and why it is needed. The diff already shows how. -->

## Checklist

- [ ] `./gradlew check` passes locally. Not `test` alone: `check` adds the integration tests and,
      for the Spring modules, the run against Spring Boot 4.x, and a green `test` can hide a
      failure in either.
- [ ] Tests cover the new behaviour, or reproduce the bug this fixes.
- [ ] Any contract this touches (REST API, DB schema, published Kafka message) evolves additively,
      and its HLD/LLD is updated in the same change. Tick this if the change touches none.
- [ ] A DB schema change is a new changeset under `schema/postgres/changelog/`, never an edit to a
      shipped one or to the generated baseline. Tick this if the change touches no schema.

<!-- Leaving a box unchecked with a line saying why is more useful than ticking it untested. -->
