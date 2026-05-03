import { Request, Response } from "express";
import { producer } from "../kafka/producer";
import { prisma } from "../prisma";

export const createOrder = async (req: Request, res: Response)=>{
    try{
        const { userId, productId } = req.body
        
        if (!userId || !productId) {
            res.status(400).json({ error: "userId and productId are required" });
            return;
        }

        const order = await prisma.order.create({
            data: {
                userId,
                productId,
                status: "PENDING",
            }
        })
        
        await producer.send({
            topic: "order-events",
            messages: [{
                value: JSON.stringify({
                    type: "ORDER_CREATED",
                    orderId: order.id,
                    userId,
                    productId,
                })
            }]
        })

        res.status(201).json({ 
            success: true,
            message: "Order created successfully",
            order: {
                id: order.id,
                userId: order.userId,
                productId: order.productId,
                status: order.status,
                createdAt: order.createdAt
            }
        });
    }catch(err){
        console.error("Order creation error:", err);
        res.status(500).json({ 
            error: "Failed to create order",
            message: err instanceof Error ? err.message : "Unknown error"
        });
    }
}