// consumer-producer.js
const { Kafka } = require('kafkajs');
const { randomUUID: uuidv4 } = require('crypto');

const kafka = new Kafka({
  clientId: 'order-processor',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'order-processing-group' });
const producer = kafka.producer();

async function run() {
  // Conectar
  await consumer.connect();
  await producer.connect();

  // Subscrever no tópico de pedidos
  await consumer.subscribe({ topic: 'orders', fromBeginning: true });

  // Processar mensagens
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      console.log('Processando pedido:', order.id);

      // Simular processamento
      await processOrder(order);

      // Publicar notificação
      const notification = {
        id: uuidv4(),
        orderId: order.id,
        customerId: order.customerId,
        type: 'ORDER_PROCESSED',
        message: `Seu pedido ${order.id} foi processado com sucesso!`,
        timestamp: new Date(),
      };

      await producer.send({
        topic: 'notifications',
        messages: [{ value: JSON.stringify(notification) }],
      });

      console.log('Notificação publicada para pedido:', order.id);
    },
  });
}

async function processOrder(order) {
  // Simular processamento
  await new Promise((resolve) => setTimeout(resolve, 2000));
  order.status = 'PROCESSED';
}

run().catch(console.error);
