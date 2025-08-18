const express = require("express");
const router = express.Router();

const {
  ensureRefs,
  importCashInStructured,
  importCashOutStructured,
  importInventoryStructured,
} = require("../controllers/reportsController");

// keep your existing frontend paths exactly the same

router.post("/import/ensure-refs", async (req, res) => {
  try {
    const out = await ensureRefs(req.body || {});
    res.json(out);
  } catch (e) {
    console.error("ensure-refs:", e);
    res.status(500).json({ message: "Failed to ensure refs." });
  }
});

router.post("/import/cash-in-structured", async (req, res) => {
  try {
    const out = await importCashInStructured(req.body || {});
    res.json(out);
  } catch (e) {
    console.error("cash-in-structured:", e);
    res.status(400).json({ message: "Cash-in import failed." });
  }
});

router.post("/import/cash-out-structured", async (req, res) => {
  try {
    const out = await importCashOutStructured(req.body || {});
    res.json(out);
  } catch (e) {
    console.error("cash-out-structured:", e);
    res.status(400).json({ message: "Cash-out import failed." });
  }
});

router.post("/import/inventory-structured", async (req, res) => {
  try {
    const out = await importInventoryStructured(req.body || {});
    res.json(out);
  } catch (e) {
    console.error("inventory-structured:", e);
    res.status(400).json({ message: "Inventory import failed." });
  }
});

module.exports = router;
