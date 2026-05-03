import { Request, Response } from "express";
import { PrismaClient } from "@prisma/client";
import { producer } from "../kafka/producer";
import { prisma } from "../prisma";

export const createOrder = async (req: Request, res: Response)=>{
    try{
        const { userId, productId } = req.body
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
    }catch(err){
        console.error(err);
        res.status(500).json({ error: "Failed to create order" });
    }
}