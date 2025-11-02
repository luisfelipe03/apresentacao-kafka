// send-order.js
const { Kafka, Partitioners } = require('kafkajs');
const { randomUUID: uuidv4 } = require('crypto');

const kafka = new Kafka({
  clientId: 'order-sender',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

async function sendOrder() {
  await producer.connect();

  const order = {
    id: uuidv4(),
    customerId: `customer-${Math.floor(Math.random() * 1000)}`,
    items: [
      { product: 'Product A', quantity: 2, price: 29.99 },
      { product: 'Product B', quantity: 1, price: 49.99 },
    ],
    total: 109.97,
    status: 'PENDING',
    timestamp: new Date(),
  };

  console.log('Enviando pedido:', order.id);

  await producer.send({
    topic: 'orders',
    messages: [{ value: JSON.stringify(order) }],
  });

  console.log('Pedido enviado com sucesso!');
  await producer.disconnect();
}

sendOrder().catch(console.error);
