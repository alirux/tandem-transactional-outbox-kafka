package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The IDE shows a property's description from the generated metadata, which the annotation processor cannot
 * fill from a record's {@code @param} tags. The hand-written additional metadata supplies them, and this
 * keeps every bound key covered as the contract grows.
 */
class ConfigurationMetadataTest {

    @Test
    void GIVEN_the_bound_property_contract_WHEN_the_ide_metadata_is_read_THEN_every_key_has_a_description()
            throws IOException {
        try (InputStream metadata = getClass().getResourceAsStream("/META-INF/spring-configuration-metadata.json")) {
            List<String> undocumented = new ArrayList<>();
            List<String> keys = new ArrayList<>();
            for (JsonNode property : new ObjectMapper().readTree(metadata).get("properties")) {
                keys.add(property.get("name").asText());
                if (!property.hasNonNull("description") || property.get("description").asText().isBlank()) {
                    undocumented.add(property.get("name").asText());
                }
            }
            assertThat(keys).isNotEmpty();
            assertThat(undocumented).as("keys without a description").isEmpty();
        }
    }
}
