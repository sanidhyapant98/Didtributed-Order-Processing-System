import { producer } from "../kafka/producer";

export const handlePayment = async (event: any) => {
  const { orderId, userId, productId } = event;

  try {
    console.log("💳 Processing payment for order:", orderId);

    // Simulate payment (random success/failure)
    const isSuccess = Math.random() > 0.3;

    if (isSuccess) {
      console.log("✅ Payment Success:", orderId);

      await producer.send({
        topic: "payment-events",
        messages: [
          {
            key: orderId, // Use orderId as key for ordering
            value: JSON.stringify({
              type: "PAYMENT_SUCCESS",
              orderId,
              userId,
              productId,
              timestamp: new Date().toISOString(),
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
            key: orderId,
            value: JSON.stringify({
              type: "PAYMENT_FAILED",
              orderId,
              userId,
              productId,
              timestamp: new Date().toISOString(),
              reason: "Payment declined"
            }),
          },
        ],
      });
    }
  } catch (err) {
    console.error("Error handling payment:", err);
    
    // Send failure event if something goes wrong
    try {
      await producer.send({
        topic: "payment-events",
        messages: [
          {
            key: orderId,
            value: JSON.stringify({
              type: "PAYMENT_FAILED",
              orderId,
              userId,
              productId,
              timestamp: new Date().toISOString(),
              reason: err instanceof Error ? err.message : "Unknown error"
            }),
          },
        ],
      });
    } catch (sendErr) {
      console.error("Failed to send payment failure event:", sendErr);
    }
  }
};