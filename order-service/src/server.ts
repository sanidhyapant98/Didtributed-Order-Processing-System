import 'dotenv/config';
import app from "./app";
import { connectProducer } from "./kafka/producer";

const PORT = process.env.PORT || 5000;

const startServer = async ()=>{
  await connectProducer();

  app.listen(PORT, ()=>{
    console.log(`Order Service running on port ${PORT}`);
  });
};

startServer();