// Commit subject rules (AGENTS.md "Commit messages", CONTRIBUTING.md): Conventional Commits with a
// mandatory scope from a closed list, letter case as config-conventional, at most 100 characters
// (config-conventional's own default, stated here so an upstream change cannot move it).
// Run by .github/workflows/commitlint.yml; locally:
//   npx --yes -p @commitlint/cli@21.2.3 -p @commitlint/config-conventional@21.2.3 commitlint --from origin/main
export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'type-enum': [2, 'always', ['feat', 'fix', 'docs', 'test', 'refactor', 'perf', 'build', 'ci']],
    'scope-empty': [2, 'never'],
    'scope-enum': [2, 'always', [
      // Modules, without their tandem- prefix.
      'core', 'jdbc', 'kafka', 'cloudevents', 'admin', 'micrometer', 'tracing-otel', 'rabbitmq',
      'spring-producer', 'spring-relay', 'test', 'bom', 'cli', 'relay', 'sample', 'sample-spring',
      'benchmark', 'coverage',
      // What lives outside a module.
      'schema', 'site', 'guide', 'deps', 'repo',
    ]],
    'header-max-length': [2, 'always', 100],
  },
  // A squash merge adds " (#NN)" to the pull request title, which the workflow has already checked
  // as a title; checking it again here would count the suffix against the length limit. The subjects
  // written before the rule (plain imperative sentences) are never in a checked range.
  ignores: [(message) => /\(#\d+\)$/.test(message.split('\n')[0])],
};
