package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.TandemAggregate;
import com.codingful.tandem.test.InMemoryOutbox;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Runtime behaviour of the write-side tiers inside a live Spring context, with no database: the aspect must
 * actually intercept an annotated call and the template must actually insert. Unlike the wiring tests, which
 * only assert that beans exist, this exercises the machinery — AspectJ proxying, the composed
 * {@code @Transactional} of {@link TransactionalOutbox}, and {@code TransactionTemplate} — and being
 * Docker-free it runs on <b>both</b> Spring generations (the {@code bootFourTest} matrix, LLD-spring-config
 * §1.2), which is where a major-version change to the AOP or transaction model would show up.
 *
 * <p>Atomicity itself (a rollback discarding outbox rows) needs a real transactional resource and stays in
 * the Testcontainers integration tests.
 */
class TandemProducerRuntimeTest {

    private static final String AGGREGATE_TYPE = "Order";
    private static final String LINE_ADDED = "com.acme.order.line-added";
    private static final String ORDER_CONFIRMED = "com.acme.order.confirmed";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemProducerAutoConfiguration.class))
            .withBean(TransactionBindingGuard.class, TransactionBindingGuard::disabled) // the stub transaction binds nothing
            .withBean(DataSource.class, NoopDataSource::new)
            .withBean(OutboxRepository.class, InMemoryOutbox::new)
            .withBean(PlatformTransactionManager.class, StubTransactionManager::new)
            .withUserConfiguration(RuntimeConfig.class);

    @Test
    void GIVEN_an_annotated_method_WHEN_it_is_called_THEN_the_aggregate_events_reach_the_outbox() {
        String aggregateId = "order-annotated";
        runner.run(context -> {
            context.getBean(OrderService.class).place(aggregateId);

            assertThat(outboxOf(context.getBean(OutboxRepository.class)))
                    .singleElement()
                    .satisfies(record -> assertThat(record.aggregateId().value()).isEqualTo(aggregateId));
        });
    }

    @Test
    void GIVEN_an_annotated_method_calling_another_on_the_same_aggregate_WHEN_both_return_it_THEN_each_change_is_one_event() {
        // Two changes in one transaction are two events. The inner method's event is still pending on the
        // aggregate when the outer method returns it, and must not be written a second time.
        runner.run(context -> {
            context.getBean(CheckoutService.class).checkout("order-checkout");

            assertThat(outboxOf(context.getBean(OutboxRepository.class)))
                    .extracting(OutboxRecord::type)
                    .containsExactly(LINE_ADDED, ORDER_CONFIRMED);
        });
    }

    @Test
    void GIVEN_an_annotated_method_returning_an_aggregate_it_did_not_change_WHEN_it_returns_THEN_no_event_is_added() {
        runner.run(context -> {
            context.getBean(CheckoutService.class).addLineOnly("order-unchanged");

            assertThat(outboxOf(context.getBean(OutboxRepository.class)))
                    .extracting(OutboxRecord::type)
                    .containsExactly(LINE_ADDED);
        });
    }

    @Test
    void GIVEN_two_identical_changes_in_one_transaction_WHEN_the_method_returns_THEN_both_are_events() {
        runner.run(context -> {
            context.getBean(CartService.class).addTwice("order-twice");

            assertThat(outboxOf(context.getBean(OutboxRepository.class)))
                    .extracting(OutboxRecord::type)
                    .containsExactly(LINE_ADDED, LINE_ADDED);
        });
    }

    @Test
    void GIVEN_the_template_WHEN_work_records_an_object_payload_THEN_it_reaches_the_outbox_serialized() {
        String aggregateId = "order-template";
        runner.run(context -> {
            context.getBean(TransactionalOutboxTemplate.class).executeWithoutResult(outbox ->
                    outbox.record(AGGREGATE_TYPE, aggregateId, 1L, new OrderPayload(aggregateId)));

            assertThat(outboxOf(context.getBean(OutboxRepository.class)))
                    .singleElement()
                    .satisfies(record -> {
                        assertThat(record.aggregateId().value()).isEqualTo(aggregateId);
                        assertThat(record.payload()).isNotEmpty();
                    });
        });
    }

    private static List<OutboxRecord> outboxOf(OutboxRepository repository) {
        return ((InMemoryOutbox) repository).all();
    }

    private record OrderPayload(String order) {
    }

    /** An aggregate that exposes the event its mutation produced, as the annotation tier requires. */
    static final class Order implements TandemAggregate {
        private final String id;

        Order(String id) {
            this.id = id;
        }

        @Override
        public Collection<OutboxMessage> pendingOutboxMessages() {
            return List.of(OutboxMessage.builder()
                    .aggregateType(AGGREGATE_TYPE).aggregateId(id).seq(1L).payload(new byte[] {1}).build());
        }
    }

    /** An aggregate that records each change as one more pending event, the usual shape of the tier. */
    static final class Cart implements TandemAggregate {
        private final String id;
        private final List<OutboxMessage> pending = new ArrayList<>();

        Cart(String id) {
            this.id = id;
        }

        Cart record(String type) {
            pending.add(OutboxMessage.builder().aggregateType(AGGREGATE_TYPE).aggregateId(id).type(type)
                    .unsequenced().payload(new byte[] {1}).build());
            return this;
        }

        @Override
        public Collection<OutboxMessage> pendingOutboxMessages() {
            return List.copyOf(pending);
        }
    }

    public static class CartService {

        @TransactionalOutbox
        public Cart addLine(Cart cart) {
            return cart.record(LINE_ADDED);
        }

        @TransactionalOutbox
        public Cart addTwice(String id) {
            return new Cart(id).record(LINE_ADDED).record(LINE_ADDED);
        }
    }

    public static class CheckoutService {
        private final CartService cartService;

        public CheckoutService(CartService cartService) {
            this.cartService = cartService;
        }

        @TransactionalOutbox
        public Cart checkout(String id) {
            Cart cart = cartService.addLine(new Cart(id));   // through the proxy: its own advice runs
            return cart.record(ORDER_CONFIRMED);
        }

        @TransactionalOutbox
        public Cart addLineOnly(String id) {
            return cartService.addLine(new Cart(id));   // returned as the inner method left it
        }
    }

    /** The application service under test — only {@code @TransactionalOutbox}, no explicit outbox call. */
    public static class OrderService {

        @TransactionalOutbox
        public Order place(String id) {
            return new Order(id);
        }
    }

    @Configuration
    @EnableAspectJAutoProxy
    @EnableTransactionManagement
    static class RuntimeConfig {

        @Bean
        OrderService orderService() {
            return new OrderService();
        }

        @Bean
        CartService cartService() {
            return new CartService();
        }

        @Bean
        CheckoutService checkoutService(CartService cartService) {
            return new CheckoutService(cartService);
        }
    }
}
