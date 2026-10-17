package com.codingful.tandem.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

/** A process must have something to do (LLD-relay §4): either role is enough, neither is refused. */
class RoleCheckTest {

    @Test
    void GIVEN_neither_the_relay_nor_the_admin_api_enabled_WHEN_the_roles_are_checked_THEN_startup_is_refused_naming_both_settings() {
        assertThatThrownBy(() -> RoleCheck.requireARole(false, false))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining(RoleCheck.RELAY_ENABLED_KEY)
                .hasMessageContaining(RoleCheck.ADMIN_ENABLED_KEY);
    }

    @Test
    void GIVEN_only_the_relay_enabled_WHEN_the_roles_are_checked_THEN_startup_proceeds() {
        assertThatCode(() -> RoleCheck.requireARole(true, false)).doesNotThrowAnyException();
    }

    @Test
    void GIVEN_only_the_admin_api_enabled_WHEN_the_roles_are_checked_THEN_startup_proceeds() {
        assertThatCode(() -> RoleCheck.requireARole(false, true)).doesNotThrowAnyException();
    }

    @Test
    void GIVEN_both_roles_switched_off_with_values_other_than_false_WHEN_the_roles_are_read_THEN_they_match_what_the_autoconfigurations_run() {
        StandardEnvironment environment = environmentWith(Map.of(
                RoleCheck.RELAY_ENABLED_KEY, "no", RoleCheck.ADMIN_ENABLED_KEY, "off"));

        // The relay's autoconfiguration runs on anything but "false"; the Admin API's only on "true".
        assertThat(RoleCheck.relayEnabled(environment)).isTrue();
        assertThat(RoleCheck.adminEnabled(environment)).isFalse();
    }

    @Test
    void GIVEN_neither_role_configured_WHEN_the_roles_are_read_THEN_the_relay_runs_and_the_admin_api_does_not() {
        StandardEnvironment environment = environmentWith(Map.of());

        assertThat(RoleCheck.relayEnabled(environment)).isTrue();
        assertThat(RoleCheck.adminEnabled(environment)).isFalse();
    }

    @Test
    void GIVEN_the_relay_off_and_the_admin_api_on_WHEN_the_roles_are_read_THEN_only_the_admin_api_runs() {
        StandardEnvironment environment = environmentWith(Map.of(
                RoleCheck.RELAY_ENABLED_KEY, "FALSE", RoleCheck.ADMIN_ENABLED_KEY, "TRUE"));

        assertThat(RoleCheck.relayEnabled(environment)).isFalse();
        assertThat(RoleCheck.adminEnabled(environment)).isTrue();
    }

    private static StandardEnvironment environmentWith(Map<String, Object> properties) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("test", properties));
        return environment;
    }
}
