import { prisma } from "../prisma";

export const handleInventory = async (event: any) => {
  const { orderId, productId } = event;

  console.log(`\n📦 Inventory processing for order: ${orderId}`);
  console.log(`   Product ID: ${productId}`);

  try {
    const product = await prisma.product.findUnique({
      where: { id: productId },
    });

    if (!product) {
      console.log(`❌ Product not found: ${productId}`);
      return;
    }

    console.log(`📊 Current stock: ${product.stock}`);

    if (product.stock <= 0) {
      console.log(`⚠️ Out of stock for product: ${productId}`);
      return;
    }

    const updatedProduct = await prisma.product.update({
      where: { id: productId },
      data: {
        stock: product.stock - 1,
      },
    });

    console.log(`✅ Stock updated successfully`);
    console.log(`📉 New stock: ${updatedProduct.stock}\n`);

  } catch (err) {
    console.error("❌ Error updating inventory:", err);
  }
};