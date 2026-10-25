package com.codingful.tandem.sample.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "sample_order_note")
public class OrderNote {
    @Id
    private String id;
    // No @OneToMany back from the root: appending a note never loads the order's notes.
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id")
    private PurchaseOrder order;

    protected OrderNote() {}

    OrderNote(PurchaseOrder order) {
        this.id = UUID.randomUUID().toString();
        this.order = order;
    }
}
