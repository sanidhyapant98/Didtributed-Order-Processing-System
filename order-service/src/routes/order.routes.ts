import { Router } from "express";
import { createOrder, getOrder } from "../controllers/order.controller";

const router = Router();

// Create a new order
router.post("/orders", createOrder);

// Get order status
router.get("/orders/:orderId", getOrder);

export default router;