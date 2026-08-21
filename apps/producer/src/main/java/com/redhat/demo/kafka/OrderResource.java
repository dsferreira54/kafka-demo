package com.redhat.demo.kafka;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import io.smallrye.reactive.messaging.kafka.Record;

@Path("/api/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderResource {

    @Inject
    @Channel("orders-out")
    Emitter<Record<String, Order>> emitter;

    @POST
    public Response createOrder(OrderRequest request) {
        Order order = Order.create(
                request.customerName,
                request.product,
                request.quantity,
                request.price);

        emitter.send(Record.of(order.orderId, order));

        return Response.status(Response.Status.CREATED).entity(order).build();
    }

    public static class OrderRequest {
        public String customerName;
        public String product;
        public int quantity;
        public double price;
    }
}
