package com.codingful.tandem.admin;

import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** Shared standalone MVC setup for the Admin API's feature controller tests. */
public final class AdminMockMvc {

    private AdminMockMvc() {
    }

    public static MockMvc create(Object controller, Object featureExceptionHandler) {
        return MockMvcBuilders.standaloneSetup(controller)
                // Standalone setup does not honour @Order across manually supplied advice instances.
                // Keep the generic catch-all last so it cannot shadow feature-specific mappings.
                .setControllerAdvice(featureExceptionHandler, new TandemAdminExceptionHandler())
                .build();
    }
}
