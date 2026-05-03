import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: [process.env.KAFKA_BROKER!],
});

export const producer = kafka.producer();

export const connectProducer = async () => {
  await producer.connect();
};