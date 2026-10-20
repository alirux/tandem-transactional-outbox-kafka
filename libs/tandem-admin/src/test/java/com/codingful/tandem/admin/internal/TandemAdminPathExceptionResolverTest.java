package com.codingful.tandem.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.servlet.NoHandlerFoundException;

/**
 * The path scoping and the rendering, on both Spring generations; {@code TandemAdminEndToEndTest} covers
 * the dispatch through a real context on the Boot 3 line.
 */
class TandemAdminPathExceptionResolverTest {

    private static final String BASE_PATH = "/ops/tandem";
    private static final String PROBLEMS = "https://tandem.codingful.com/problems/";

    private final TandemAdminPathExceptionResolver resolver = new TandemAdminPathExceptionResolver(BASE_PATH);

    private static MockHttpServletRequest request(String contextPath, String path) {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", contextPath + path);
        request.setContextPath(contextPath);
        return request;
    }

    /** {@code NoResourceFoundException}, its sibling, changed constructor between Spring 6 and 7. */
    private static NoHandlerFoundException noResource(String path) {
        return new NoHandlerFoundException("GET", path, new HttpHeaders());
    }

    @Test
    void GIVEN_a_missing_path_under_the_configured_base_path_WHEN_it_fails_THEN_a_not_found_problem_is_written()
            throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThat(resolver.resolveException(request("", BASE_PATH + "/v1/nope"), response, null, noResource("nope")))
                .isNotNull();

        assertThat(response.getStatus()).isEqualTo(404);
        assertThat(response.getContentType()).isEqualTo("application/problem+json");
        assertThat(response.getContentAsString())
                .contains("\"type\":\"" + PROBLEMS + "not-found\"")
                .contains("\"status\":404");
    }

    @Test
    void GIVEN_a_method_the_path_does_not_define_WHEN_it_fails_THEN_the_problem_lists_the_accepted_methods()
            throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        HttpRequestMethodNotSupportedException wrongMethod = new HttpRequestMethodNotSupportedException("GET", List.of("POST"));

        resolver.resolveException(request("", BASE_PATH + "/v1/outbox/replay"), response, null, wrongMethod);

        assertThat(response.getStatus()).isEqualTo(405);
        assertThat(response.getHeader(HttpHeaders.ALLOW)).isEqualTo("POST");
        assertThat(response.getContentAsString()).contains("\"type\":\"" + PROBLEMS + "method-not-allowed\"");
    }

    @Test
    void GIVEN_the_application_runs_under_a_context_path_WHEN_an_admin_path_fails_THEN_it_is_still_recognised() {
        MockHttpServletResponse response = new MockHttpServletResponse();

        resolver.resolveException(request("/app", BASE_PATH + "/v1/nope"), response, null, noResource("nope"));

        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void GIVEN_the_bare_API_root_WHEN_it_fails_THEN_it_is_an_admin_path() {
        MockHttpServletResponse response = new MockHttpServletResponse();

        resolver.resolveException(request("", BASE_PATH + "/v1"), response, null, noResource("v1"));

        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void GIVEN_a_path_of_the_host_WHEN_it_fails_THEN_it_is_left_to_the_host() {
        for (String path : new String[] {"/somewhere/else", BASE_PATH + "/v1extra/nope", BASE_PATH + "/v2/nope"}) {
            MockHttpServletResponse response = new MockHttpServletResponse();

            assertThat(resolver.resolveException(request("", path), response, null, noResource(path)))
                    .as(path).isNull();
            assertThat(response.isCommitted()).as(path).isFalse();
        }
    }

    @Test
    void GIVEN_an_admin_path_WHEN_a_failure_other_than_a_missing_route_reaches_it_THEN_it_is_left_to_the_other_handlers() {
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThat(resolver.resolveException(request("", BASE_PATH + "/v1/outbox/summary"), response, null,
                new IllegalStateException("boom"))).isNull();
    }
}
