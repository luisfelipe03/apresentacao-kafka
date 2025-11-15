package org.example.controller;

import org.example.entity.Order;
import org.example.producer.KafkaOrderProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class OrderController {

    private final KafkaOrderProducer orderProducer;

    @PostMapping("/orders")
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        log.info("Recebendo novo pedido");

        // Gerar ID e configurar pedido
        order.setId(UUID.randomUUID().toString());
        order.setStatus("PENDING");
        order.setTimestamp(new Date());

        // Calcular total se não fornecido
        if (order.getTotal() == null && order.getItems() != null) {
            double total = order.getItems().stream()
                    .mapToDouble(item -> item.getPrice() * item.getQuantity())
                    .sum();
            order.setTotal(total);
        }

        // Publicar no Kafka
        orderProducer.sendOrder(order);

        log.info("Pedido criado: {}", order.getId());
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Order Service is running!");
    }
}