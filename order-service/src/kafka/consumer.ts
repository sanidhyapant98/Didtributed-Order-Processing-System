import { Kafka } from "kafkajs";
import { prisma } from "../prisma";

const kafka = new Kafka({
  clientId: "order-service-consumer",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "order-service-group" });

export const startOrderConsumer = async () => {
  try {
    await consumer.connect();
    console.log("📡 Order Consumer connecting to Kafka...");

    await consumer.subscribe({
      topic: "payment-events",
      fromBeginning: false,
    });

    console.log("👂 Listening on topic: payment-events");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const data = JSON.parse(message.value!.toString());
          console.log("\n📥 Order Service received payment event:", data);

          if (data.type === "PAYMENT_SUCCESS") {
            // Update order status to COMPLETED
            const updatedOrder = await prisma.order.update({
              where: { id: data.orderId },
              data: { status: "COMPLETED" }
            });
            console.log(`✅ Order ${data.orderId} marked as COMPLETED`);
            console.log(`   Order Details:`, updatedOrder);
          } 
          else if (data.type === "PAYMENT_FAILED") {
            // Update order status to FAILED
            const updatedOrder = await prisma.order.update({
              where: { id: data.orderId },
              data: { status: "FAILED" }
            });
            console.log(`❌ Order ${data.orderId} marked as FAILED`);
            console.log(`   Reason:`, data.reason || "Payment declined");
            console.log(`   Order Details:`, updatedOrder);
          }
        } catch (err) {
          console.error("⚠️  Error processing payment event:", err);
        }
      },
    });
  } catch (err) {
    console.error("❌ Order consumer error:", err);
    process.exit(1);
  }
};

export const disconnectOrderConsumer = async () => {
  try {
    await consumer.disconnect();
    console.log("🔌 Order consumer disconnected");
  } catch (err) {
    console.error("Error disconnecting order consumer:", err);
  }
};