package com.codingful.tandem.relay;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import org.junit.jupiter.api.Test;

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
}
