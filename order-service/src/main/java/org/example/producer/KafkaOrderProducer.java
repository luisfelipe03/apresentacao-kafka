package org.example.producer;


import org.example.entity.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaOrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    private static final String TOPIC = "orders";

    public void sendOrder(Order order) {
        log.info("Enviando pedido para o Kafka: {}", order.getId());

        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send(TOPIC, order.getId(), order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Pedido publicado com sucesso: {} | Offset: {}",
                        order.getId(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Erro ao publicar pedido: {}", order.getId(), ex);
            }
        });
    }
}