import dotenv from 'dotenv';
dotenv.config();
import { connectProducer } from "./kafka/producer";
import { startConsumer } from "./kafka/consumer";

const start = async () => {
  await connectProducer();
  await startConsumer();

  console.log("💰 Payment Service running...");
};

start();