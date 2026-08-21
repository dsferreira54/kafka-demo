package com.redhat.demo.kafka;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import io.smallrye.reactive.messaging.kafka.Record;

@ApplicationScoped
public class OrderConsumer {

    private static final Logger LOG = Logger.getLogger(OrderConsumer.class);

    private final List<Order> orders = new CopyOnWriteArrayList<>();

    @Incoming("orders-in")
    public void consume(Record<String, Order> record) {
        Order order = record.value();
        orders.add(order);
        LOG.infof("Consumed order: %s (key=%s)", order.orderId, record.key());
    }

    public List<Order> getOrders() {
        return orders;
    }

    public int getCount() {
        return orders.size();
    }
}
