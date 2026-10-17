package com.codingful.tandem.spring.producer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.core.annotation.AliasFor;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The annotation tier (HLD §3.1, LLD-spring-producer §4): mark a method whose returned aggregate carries
 * pending outbox messages. It is a <em>composed</em> {@link Transactional} annotation — a transaction is
 * always present, so the method never needs both annotations — and it exposes every {@code @Transactional}
 * attribute via {@link AliasFor}, keeping full transaction control.
 *
 * <p>After the method returns, an aspect reads the {@code TandemAggregate} it returned (a single one or an
 * {@link Iterable} of them), takes its {@code pendingOutboxMessages()}, and inserts them within the same
 * transaction. The {@code seq} of each message must come from the aggregate's {@code version} (HLD §4.2);
 * Tandem never invents it.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Transactional
public @interface TransactionalOutbox {

    /**
     * Optional guard: when set, every extracted message must carry this {@code aggregateType} or the
     * insert fails fast. Left empty, extraction is unconstrained — the messages carry their own type.
     */
    String aggregateType() default "";

    /** @see Transactional#value() */
    @AliasFor(annotation = Transactional.class, attribute = "transactionManager")
    String value() default "";

    /**
     * The transaction manager to run the method under, as {@link Transactional#transactionManager()}
     * names it: the one bound to the {@code DataSource} Tandem writes to when the application has
     * several.
     */
    @AliasFor(annotation = Transactional.class, attribute = "transactionManager")
    String transactionManager() default "";

    /** @see Transactional#label() */
    @AliasFor(annotation = Transactional.class, attribute = "label")
    String[] label() default {};

    /** @see Transactional#propagation() */
    @AliasFor(annotation = Transactional.class, attribute = "propagation")
    Propagation propagation() default Propagation.REQUIRED;

    /** @see Transactional#isolation() */
    @AliasFor(annotation = Transactional.class, attribute = "isolation")
    Isolation isolation() default Isolation.DEFAULT;

    /** @see Transactional#timeout() */
    @AliasFor(annotation = Transactional.class, attribute = "timeout")
    int timeout() default -1;

    /** @see Transactional#timeoutString() */
    @AliasFor(annotation = Transactional.class, attribute = "timeoutString")
    String timeoutString() default "";

    /** @see Transactional#readOnly() */
    @AliasFor(annotation = Transactional.class, attribute = "readOnly")
    boolean readOnly() default false;

    /** @see Transactional#rollbackFor() */
    @AliasFor(annotation = Transactional.class, attribute = "rollbackFor")
    Class<? extends Throwable>[] rollbackFor() default {};

    /** @see Transactional#rollbackForClassName() */
    @AliasFor(annotation = Transactional.class, attribute = "rollbackForClassName")
    String[] rollbackForClassName() default {};

    /** @see Transactional#noRollbackFor() */
    @AliasFor(annotation = Transactional.class, attribute = "noRollbackFor")
    Class<? extends Throwable>[] noRollbackFor() default {};

    /** @see Transactional#noRollbackForClassName() */
    @AliasFor(annotation = Transactional.class, attribute = "noRollbackForClassName")
    String[] noRollbackForClassName() default {};
}
