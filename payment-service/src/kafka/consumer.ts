import { Kafka } from "kafkajs";
import { handlePayment } from "../handlers/payment.handler";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "payment-group" });

export const startConsumer = async () => {
  await consumer.connect();

  await consumer.subscribe({
    topic: "order-events",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value!.toString());

      console.log("📥 Payment Service received:", data);

      if (data.type === "ORDER_CREATED") {
        await handlePayment(data);
      }
    },
  });
};