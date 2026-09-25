package com.codingful.tandem.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

class ProblemTypePagesTest {

    private static final String PROBLEM_TYPE_PREFIX = "https://tandem.codingful.com/problems/";
    private static final Pattern PROBLEM_TYPE = Pattern.compile(
            "^\\s+type:\\s+[\"']?" + Pattern.quote(PROBLEM_TYPE_PREFIX)
                + "([a-z0-9-]+)[\"']?\\s*$",
            Pattern.MULTILINE);

    @Test
    void GIVEN_problem_types_in_the_contract_WHEN_the_site_is_checked_THEN_every_type_has_a_page_and_index_link()
            throws IOException {
        Path repository = repositoryRoot();
        String specification = Files.readString(repository.resolve("docs/admin-api.openapi.yaml"));
        String problemIndex = Files.readString(repository.resolve("site/problems/index.html"));
        Set<String> slugs = new TreeSet<>();
        Set<String> missingPages = new TreeSet<>();
        Set<String> missingLinks = new TreeSet<>();

        Matcher matcher = PROBLEM_TYPE.matcher(specification);
        while (matcher.find()) {
            String slug = matcher.group(1);
            slugs.add(slug);
            if (!Files.isRegularFile(repository.resolve("site/problems/" + slug + "/index.html"))) {
                missingPages.add(slug);
            }
            if (!problemIndex.contains("href=\"" + slug + "/\"")) {
                missingLinks.add(slug);
            }
        }

        assertThat(slugs).as("problem type slugs found in the OpenAPI contract").isNotEmpty();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(missingPages)
                .as("problem type slugs missing a site page")
                .isEmpty();
        softly.assertThat(missingLinks)
                .as("problem type slugs missing an index link")
                .isEmpty();
        softly.assertAll();
    }

    private static Path repositoryRoot() {
        Path directory = Path.of("").toAbsolutePath();
        while (directory != null) {
            if (Files.exists(directory.resolve("docs/admin-api.openapi.yaml"))) {
                return directory;
            }
            directory = directory.getParent();
        }
        throw new IllegalStateException("could not locate repository root");
    }
}