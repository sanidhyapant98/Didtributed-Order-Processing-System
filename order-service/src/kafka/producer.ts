import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

export const producer = kafka.producer();

export const connectProducer = async()=>{
  await producer.connect();
};
console.log("Kafka broker:", process.env.KAFKA_BROKER);