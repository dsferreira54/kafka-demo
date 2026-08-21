package com.redhat.demo.kafka;

import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/api/orders")
@Produces(MediaType.APPLICATION_JSON)
public class OrderResource {

    @Inject
    OrderConsumer consumer;

    @GET
    public List<Order> listOrders() {
        return consumer.getOrders();
    }

    @GET
    @Path("/count")
    public Map<String, Integer> count() {
        return Map.of("count", consumer.getCount());
    }
}
