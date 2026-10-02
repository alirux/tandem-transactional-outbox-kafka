package com.codingful.tandem.relay;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.info.BuildProperties;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Looks at which roles this process was asked to run, once, at startup (LLD-relay §4).
 *
 * <ul>
 *   <li><b>Neither role</b>: refused. The application would start, report ready and do nothing, which
 *       is the kind of misconfiguration nobody notices.</li>
 *   <li><b>The Admin API</b>: allowed, with a warning. It is an unauthenticated management surface
 *       (HLD-admin-api §3), and this application is the one place where it can be switched on with
 *       nothing in front of it. An application embedding {@code tandem-admin} chose its own security,
 *       which is why the warning lives here and not in that module.</li>
 * </ul>
 *
 * It also states what is running: the application's version does not imply the library release it
 * contains, so the startup line names both.
 */
@Component
class RoleCheck {

    static final String RELAY_ENABLED_KEY = "tandem.relay.enabled";
    static final String ADMIN_ENABLED_KEY = "tandem.admin.enabled";
    static final String LIBRARY_VERSION_BUILD_PROPERTY = "tandem.library";

    private static final Logger LOG = LoggerFactory.getLogger(RoleCheck.class);
    private static final String UNKNOWN = "unknown";

    RoleCheck(Environment environment, ObjectProvider<BuildProperties> build) {
        // The library's own defaults: the relay runs unless switched off, the Admin API only when
        // switched on.
        boolean relay = environment.getProperty(RELAY_ENABLED_KEY, Boolean.class, true);
        boolean admin = environment.getProperty(ADMIN_ENABLED_KEY, Boolean.class, false);
        requireARole(relay, admin);

        BuildProperties buildProperties = build.getIfAvailable();
        LOG.info("Tandem relay application starting version:{}, tandemLibrary:{}, relayRole:{}, adminRole:{}",
                buildProperties == null ? UNKNOWN : buildProperties.getVersion(),
                buildProperties == null ? UNKNOWN : buildProperties.get(LIBRARY_VERSION_BUILD_PROPERTY),
                relay, admin);
        if (admin) {
            LOG.warn("Admin API enabled with no authentication in front of it;"
                    + " keep its port on an internal network serverPort:{}",
                    environment.getProperty("server.port"));
        }
    }

    /**
     * @throws TandemConfigurationException if the process was configured with no role at all
     */
    static void requireARole(boolean relay, boolean admin) {
        if (!relay && !admin) {
            throw new TandemConfigurationException("No role to run: " + RELAY_ENABLED_KEY + " is false and "
                    + ADMIN_ENABLED_KEY + " is not true. Enable the relay, the Admin API, or both");
        }
    }
}
