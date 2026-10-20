package com.codingful.tandem.admin.internal;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.ErrorResponse;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Renders as RFC 9457 problems the errors no Tandem controller sees (HLD-admin-api §3): a path under the
 * Admin API that matches no operation ({@code 404 not-found}) and a method a path does not define
 * ({@code 405 method-not-allowed}, with its {@code Allow} header). Spring raises both before choosing a
 * handler, so the advices, scoped to Tandem's packages, never apply. This resolver is scoped by path
 * instead, and leaves every request outside {@code {base-path}/v1} to the host.
 *
 * <p>The body holds fixed text only, written without a JSON binding (LLD-spring-config §1.3), and echoes
 * nothing of the request.
 */
public final class TandemAdminPathExceptionResolver implements HandlerExceptionResolver, Ordered {

    private static final Logger LOG = LoggerFactory.getLogger(TandemAdminPathExceptionResolver.class);

    private final String apiRoot;

    /**
     * @param basePath the configured {@code tandem.admin.base-path}, before the {@code /v1} segment
     * @throws NullPointerException if {@code basePath} is {@code null}
     */
    public TandemAdminPathExceptionResolver(String basePath) {
        this.apiRoot = Objects.requireNonNull(basePath, "basePath") + "/v1";
    }

    @Override
    public ModelAndView resolveException(HttpServletRequest request, HttpServletResponse response, Object handler,
            Exception exception) {
        if (!underApiRoot(request)) {
            return null;
        }
        if (exception instanceof HttpRequestMethodNotSupportedException methodNotSupported) {
            return write(response, methodNotSupported, HttpStatus.METHOD_NOT_ALLOWED, "method-not-allowed",
                    "Method not allowed",
                    "This path does not accept the request method; the Allow header lists the ones it does");
        }
        if (exception instanceof NoHandlerFoundException || exception instanceof NoResourceFoundException) {
            return write(response, (ErrorResponse) exception, HttpStatus.NOT_FOUND, "not-found", "Resource not found",
                    "No operation of the Admin API answers this path");
        }
        return null;
    }

    private boolean underApiRoot(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        return path.equals(apiRoot) || path.startsWith(apiRoot + "/");
    }

    private static ModelAndView write(HttpServletResponse response, ErrorResponse error, HttpStatus status,
            String slug, String title, String detail) {
        try {
            // The exception's own headers carry what the status calls for, the Allow list of a 405.
            error.getHeaders().forEach((name, values) -> values.forEach(value -> response.addHeader(name, value)));
            response.setStatus(status.value());
            // Bytes, not the writer: the content type stays bare, like every other problem this API sends.
            response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
            response.getOutputStream().write(("{\"type\":\"" + ProblemDetails.type(slug) + "\",\"title\":\""
                    + title + "\",\"status\":" + status.value() + ",\"detail\":\"" + detail + "\"}")
                    .getBytes(StandardCharsets.UTF_8));
            return new ModelAndView();
        } catch (IOException writeFailure) {
            LOG.warn("Writing an Admin API problem response failed status:{}, slug:{}", status.value(), slug,
                    writeFailure);
            return null;
        }
    }

    /** Ahead of Spring MVC's own resolvers, which would answer with the host's error page. */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }
}
