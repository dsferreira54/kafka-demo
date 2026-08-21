package com.redhat.demo.kafka;

import java.time.Instant;
import java.util.UUID;

public class Order {

    public String orderId;
    public String customerName;
    public String product;
    public int quantity;
    public double price;
    public String timestamp;

    public static Order create(String customerName, String product, int quantity, double price) {
        Order order = new Order();
        order.orderId = UUID.randomUUID().toString().substring(0, 8);
        order.customerName = customerName;
        order.product = product;
        order.quantity = quantity;
        order.price = price;
        order.timestamp = Instant.now().toString();
        return order;
    }
}
