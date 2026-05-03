import { producer } from "../kafka/producer";

export const handlePayment = async (event: any) => {
  const { orderId, userId, productId } = event;

  try {
    console.log(`\n💳 Processing payment for order: ${orderId}`);
    console.log(`   User: ${userId}, Product: ${productId}`);

    // Simulate payment processing (random success/failure - 70% success rate)
    const isSuccess = Math.random() > 0.3;
    
    // Add a small delay to simulate processing
    await new Promise(resolve => setTimeout(resolve, 500));

    if (isSuccess) {
      console.log(`✅ Payment Success for order: ${orderId}`);

      await producer.send({
        topic: "payment-events",
        messages: [
          {
            key: orderId,
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
      
      console.log(`📤 Published PAYMENT_SUCCESS event to Kafka\n`);
    } else {
      console.log(`❌ Payment Failed for order: ${orderId}`);

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
              reason: "Payment declined - insufficient funds"
            }),
          },
        ],
      });
      
      console.log(`📤 Published PAYMENT_FAILED event to Kafka\n`);
    }
  } catch (err) {
    console.error("❌ Error handling payment:", err);
    
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