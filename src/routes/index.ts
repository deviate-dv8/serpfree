import { Router } from "express";
import serpController from "../controllers/serp-controller";
const router = Router();

router.use("/serp", serpController);

export default router;
