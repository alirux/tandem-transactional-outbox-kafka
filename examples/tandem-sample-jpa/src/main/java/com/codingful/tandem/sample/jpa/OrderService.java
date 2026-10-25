package com.codingful.tandem.sample.jpa;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.port.OutboxRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceContext;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {
    @PersistenceContext
    private EntityManager entities;
    private final OutboxRepository outbox;

    OrderService(OutboxRepository outbox) { this.outbox = outbox; }

    @Transactional
    public void create(String id) {
        entities.persist(new PurchaseOrder(id));
    }

    /** One command may append several notes, including several on the same aggregate. */
    @Transactional
    public void append(List<String> ids) {
        var orders = new LinkedHashMap<String, PurchaseOrder>();
        // Acquire the COMPLETE lock set before any mutation or outbox insert. All writers use
        // the same ordering, even when commands mention the aggregates in reverse order.
        ids.stream().distinct().sorted().forEach(id -> orders.put(id, lock(id)));
        for (String id : ids) {
            entities.persist(new OrderNote(id));
            // No flush: the lock is already held, and the revision is read from memory, not from
            // anything Hibernate computes at flush. Everything becomes visible together at commit.
            outbox.insert(message(id, orders.get(id).nextEventSequence(), "OrderNoteAdded"));
        }
    }

    /** Separate stream: exactly one state-changing event, with seq taken from JPA's version. */
    @Transactional
    public void confirmWithVersion(String id) {
        PurchaseOrder order = lock(id);
        long before = order.version();
        order.confirm();
        // Hibernate advances @Version only at flush, and a JDBC outbox write does not trigger one.
        entities.flush();
        // A no-op produces no event. Reusing the unchanged version would duplicate seq.
        if (order.version() != before) {
            outbox.insert(message(id, order.version(), "OrderConfirmed"));
        }
    }

    private PurchaseOrder lock(String id) {
        PurchaseOrder order = entities.find(PurchaseOrder.class, id, LockModeType.PESSIMISTIC_WRITE);
        if (order == null) {
            throw new IllegalArgumentException("Unknown aggregate id:" + id);
        }
        return order;
    }

    private static OutboxMessage message(String id, long seq, String type) {
        return OutboxMessage.builder().aggregateType("Order").aggregateId(id)
                .type(type).seq(seq).payload("{}".getBytes(StandardCharsets.UTF_8)).build();
    }
}
