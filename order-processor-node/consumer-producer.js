// consumer-producer.js
const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { randomUUID: uuidv4 } = require('crypto');

// Silenciar warning do partitioner
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

const kafka = new Kafka({
  clientId: 'order-processor',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 100,
    retries: 8,
  },
});

const consumer = kafka.consumer({
  groupId: 'order-processing-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

async function run() {
  // Conectar
  await consumer.connect();
  await producer.connect();

  console.log('Conectado ao Kafka');

  // Criar os tópicos se não existirem
  const admin = kafka.admin();
  await admin.connect();

  const topics = await admin.listTopics();

  if (!topics.includes('orders')) {
    await admin.createTopics({
      topics: [{ topic: 'orders', numPartitions: 1, replicationFactor: 1 }],
    });
    console.log('Tópico "orders" criado');
  }

  if (!topics.includes('notifications')) {
    await admin.createTopics({
      topics: [
        { topic: 'notifications', numPartitions: 1, replicationFactor: 1 },
      ],
    });
    console.log('Tópico "notifications" criado');
  }

  await admin.disconnect();

  // Subscrever no tópico de pedidos
  await consumer.subscribe({ topic: 'orders', fromBeginning: true });

  console.log('Aguardando mensagens no tópico "orders"...');

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

// Tratamento de erros e graceful shutdown
const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await consumer.disconnect();
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      console.log(`\nDesconectando do Kafka...`);
      await consumer.disconnect();
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});

run().catch(console.error);
