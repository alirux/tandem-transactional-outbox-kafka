package com.codingful.tandem.sample.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "sample_order_note")
public class OrderNote {
    @Id
    private String id;
    private String orderId;

    protected OrderNote() {}

    OrderNote(String orderId) {
        this.id = UUID.randomUUID().toString();
        this.orderId = orderId;
    }
}
