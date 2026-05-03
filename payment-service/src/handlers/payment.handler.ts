import { producer } from "../kafka/producer";

export const handlePayment = async (event: any) => {
  const { orderId } = event;

  console.log("💳 Processing payment for order:", orderId);

  // Simulate payment (random success/failure)
  const isSuccess = Math.random() > 0.3;

  if (isSuccess) {
    console.log("✅ Payment Success:", orderId);

    await producer.send({
      topic: "payment-events",
      messages: [
        {
          value: JSON.stringify({
            type: "PAYMENT_SUCCESS",
            orderId,
          }),
        },
      ],
    });
  } else {
    console.log("❌ Payment Failed:", orderId);

    await producer.send({
      topic: "payment-events",
      messages: [
        {
          value: JSON.stringify({
            type: "PAYMENT_FAILED",
            orderId,
          }),
        },
      ],
    });
  }
};