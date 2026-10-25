package com.codingful.tandem.sample.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "sample_order")
public class PurchaseOrder {
    @Id
    private String id;
    @Version
    private long version;
    // Domain stream revision, incremented for every event, including child-only changes.
    private long eventSequence;
    private String status;

    protected PurchaseOrder() {}

    PurchaseOrder(String id) {
        this.id = id;
        this.status = "new";
    }

    long nextEventSequence() { return eventSequence = Math.incrementExact(eventSequence); }
    long version() { return version; }
    void confirm() { status = "confirmed"; }
}
