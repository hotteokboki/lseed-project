const pgDatabase = require("../database.js"); // your pg Pool

// ---------------- helpers ----------------
const norm = (s) => String(s ?? "").trim();
const toNum = (v) => (v === "" || v == null || Number.isNaN(+v) ? null : +v);
const toISODate = (s) => {
  if (!s) return null;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  // return yyyy-mm-dd (DATE column)
  return d.toISOString().slice(0, 10);
};

// normalize to yyyy-mm-01 (you already have monthBucket, using it here)
const monthBucket = (s) => {
  if (!s) return null;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1))
    .toISOString()
    .slice(0, 10);
};

// Throw 409 if the same SE already uploaded the same report type for the month
async function ensureUploadGuard(client, se_id, report_month, report_type) {
  const m = monthBucket(report_month);
  const { rowCount } = await client.query(
    `INSERT INTO monthly_report_guard (se_id, "month", report_type)
     VALUES ($1, $2::date, $3)
     ON CONFLICT DO NOTHING`,
    [se_id, m, report_type]
  );
  if (rowCount === 0) {
    const err = new Error("Report for this SE and month already exists.");
    err.status = 409;
    throw err;
  }
}

// advisory lock per import
async function takeLock(client, key) {
  if (!key) return;
  await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [key]);
}

// ---------- reference upserts (match your schema) ----------
// asset(asset_id, asset_name UNIQUE, asset_amount NOT NULL)
async function getOrCreateAssetByName(client, name, amount = 0) {
  const n = norm(name);
  if (!n) return null;
  const { rows } = await client.query(
    `INSERT INTO asset (asset_name, asset_amount)
     VALUES ($1, $2)
     ON CONFLICT (asset_name)
       DO UPDATE SET asset_name = EXCLUDED.asset_name
     RETURNING asset_id`,
    [n, toNum(amount) ?? 0]
  );
  return rows[0].asset_id;
}

// expense(expense_id, expense_name UNIQUE, expense_amount NOT NULL)
async function getOrCreateExpenseByName(client, name, amount = 0) {
  const n = norm(name);
  if (!n) return null;
  const { rows } = await client.query(
    `INSERT INTO expense (expense_name, expense_amount)
     VALUES ($1, $2)
     ON CONFLICT (expense_name)
       DO UPDATE SET expense_name = EXCLUDED.expense_name
     RETURNING expense_id`,
    [n, toNum(amount) ?? 0]
  );
  return rows[0].expense_id;
}

// bom(bom_id, bom_name UNIQUE)
async function getOrCreateBOMByName(client, bomName) {
  const n = norm(bomName || "Default BOM");
  const { rows } = await client.query(
    `INSERT INTO bom (bom_name)
     VALUES ($1)
     ON CONFLICT (bom_name)
       DO UPDATE SET bom_name = EXCLUDED.bom_name
     RETURNING bom_id`,
    [n]
  );
  return rows[0].bom_id;
}

// item(item_id, item_name, item_price NOT NULL, item_beginning_inventory, item_less_count, bom_id)
// No unique on item_name in your DDL, so we do a SELECT-first upsert by lower(name)
async function getOrCreateItemByName(client, name, defaults = {}) {
  const n = norm(name);
  if (!n) return null;

  const find = await client.query(
    `SELECT item_id FROM item WHERE lower(item_name) = lower($1) LIMIT 1`,
    [n]
  );
  if (find.rows[0]) {
    return find.rows[0].item_id;
  }

  // new insert with safe defaults
  const {
    price = 0,
    beginning = 0,
    less = 0,
    bom_id = null,
  } = defaults;

  const { rows } = await client.query(
    `INSERT INTO item (item_name, item_price, item_beginning_inventory, item_less_count, bom_id)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING item_id`,
    [n, toNum(price) ?? 0, toNum(beginning) ?? 0, toNum(less) ?? 0, bom_id]
  );
  return rows[0].item_id;
}

// bill_of_materials(bom_id, record_id, raw_material_name, raw_material_price, raw_material_qty)
// No unique key on (bom_id, raw_material_name), so: SELECT-if-exists then UPDATE else INSERT
async function upsertBOMLine(client, bom_id, name, qty, price) {
  const n = norm(name);
  if (!n) return;

  const found = await client.query(
    `SELECT record_id FROM bill_of_materials
     WHERE bom_id = $1 AND lower(raw_material_name) = lower($2)
     LIMIT 1`,
    [bom_id, n]
  );

  if (found.rows[0]) {
    await client.query(
      `UPDATE bill_of_materials
       SET raw_material_qty   = $2,
           raw_material_price = $3
       WHERE record_id = $1`,
      [found.rows[0].record_id, toNum(qty) ?? 0, toNum(price) ?? 0]
    );
  } else {
    await client.query(
      `INSERT INTO bill_of_materials
         (bom_id, raw_material_name, raw_material_qty, raw_material_price)
       VALUES ($1, $2, $3, $4)`,
      [bom_id, n, toNum(qty) ?? 0, toNum(price) ?? 0]
    );
  }
}

async function getCashInReportId(client, se_id, report_month) {
  const iso = toISODate(report_month);
  const m = monthBucket(iso);
  try {
    const { rows } = await client.query(
      `INSERT INTO cash_in_report (se_id, report_month)
       VALUES ($1, $2::date)
       RETURNING cash_in_report_id`,
      [se_id, iso]
    );
    return rows[0].cash_in_report_id;
  } catch (e) {
    if (e.code === '23505') {
      const { rows } = await client.query(
        `SELECT cash_in_report_id
           FROM cash_in_report
          WHERE se_id = $1
            AND (report_month - (EXTRACT(DAY FROM report_month)::int - 1)) = $2::date
          LIMIT 1`,
        [se_id, m]
      );
      if (rows[0]) return rows[0].cash_in_report_id;
    }
    throw e;
  }
}

async function getCashOutReportId(client, se_id, report_month) {
  const iso = toISODate(report_month);
  const m = monthBucket(iso);
  try {
    const { rows } = await client.query(
      `INSERT INTO cash_out_report (se_id, report_month)
       VALUES ($1, $2::date)
       RETURNING cash_out_report_id`,
      [se_id, iso]
    );
    return rows[0].cash_out_report_id;
  } catch (e) {
    if (e.code === '23505') {
      const { rows } = await client.query(
        `SELECT cash_out_report_id
           FROM cash_out_report
          WHERE se_id = $1
            AND (report_month - (EXTRACT(DAY FROM report_month)::int - 1)) = $2::date
          LIMIT 1`,
        [se_id, m]
      );
      if (rows[0]) return rows[0].cash_out_report_id;
    }
    throw e;
  }
}

// ---------------- PUBLIC MODEL FNS ----------------

// Ensures assets & expenses exist; returns maps name->id (lowercased keys)
exports.ensureRefs = async ({ assets = [], expenses = [] }) => {
  const client = await pgDatabase.connect();
  try {
    await client.query("BEGIN");

    const assetMap = {};
    const expenseMap = {};

    // assets
    for (const a of [...new Set(assets.filter(Boolean))]) {
      const id = await getOrCreateAssetByName(client, a, 0);
      assetMap[a.toLowerCase()] = id;
    }

    // expenses
    for (const e of [...new Set(expenses.filter(Boolean))]) {
      const id = await getOrCreateExpenseByName(client, e, 0);
      expenseMap[e.toLowerCase()] = id;
    }

    await client.query("COMMIT");
    return { assetMap, expenseMap };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
};

// Cash In (structured)
exports.importCashInStructured = async ({ se_id, report_month, transactions = [] }) => {
  if (!se_id || !report_month) throw new Error("Missing se_id or report_month");

  const client = await pgDatabase.connect();
  try {
    await client.query("BEGIN");
    await takeLock(client, `import:cashin:${se_id}:${monthBucket(report_month)}`);

    await ensureUploadGuard(client, se_id, report_month, 'cash_in');
    const cash_in_report_id = await getCashInReportId(client, se_id, report_month);

    // Ensure any ad-hoc dynamic assets exist
    const dynNames = new Set();
    for (const t of transactions || []) {
      if (t.asset_name) dynNames.add(t.asset_name);
      const dyn = t.__dynamicAssets || t.dynamic_assets || {};
      Object.keys(dyn).forEach((k) => dynNames.add(k));
    }
    const dynMap = {};
    for (const name of dynNames) {
      dynMap[name.toLowerCase()] = await getOrCreateAssetByName(client, name, 0);
    }

    let inserted = 0;

    for (const t of transactions || []) {
      // base row
      const assetId =
        t.asset_id ??
        (t.asset_name ? dynMap[norm(t.asset_name).toLowerCase()] : null);

      await client.query(
        `INSERT INTO cash_in_transaction
           (cash_in_report_id, transaction_date, cash_amount, sales_amount, other_revenue_amount,
            asset_id, liability_amount, owners_capital_amount, note, entered_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [
          cash_in_report_id,
          toISODate(t.transaction_date),
          toNum(t.cash_amount),
          toNum(t.sales_amount),
          toNum(t.other_revenue_amount),
          assetId,
          toNum(t.liability_amount),
          toNum(t.owners_capital_amount),
          norm(t.note),
          norm(t.entered_by),
        ]
      );
      inserted++;

      // dynamic splits
      const dyn = t.__dynamicAssets || t.dynamic_assets || {};
      for (const [label, amt] of Object.entries(dyn)) {
        await client.query(
          `INSERT INTO cash_in_transaction
             (cash_in_report_id, transaction_date, cash_amount, sales_amount, other_revenue_amount,
              asset_id, liability_amount, owners_capital_amount, note, entered_by)
           VALUES ($1,$2,$3,NULL,NULL,$4,NULL,NULL,$5,$6)`,
          [
            cash_in_report_id,
            toISODate(t.transaction_date),
            toNum(amt),
            dynMap[label.toLowerCase()],
            norm(t.note),
            norm(t.entered_by),
          ]
        );
        inserted++;
      }
    }

    await client.query("COMMIT");
    return { inserted };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
};

// Cash Out (structured)
exports.importCashOutStructured = async ({ se_id, report_month, transactions = [] }) => {
  if (!se_id || !report_month) throw new Error("Missing se_id or report_month");

  const client = await pgDatabase.connect();
  try {
    await client.query("BEGIN");
    await takeLock(client, `import:cashout:${se_id}:${monthBucket(report_month)}`);

    await ensureUploadGuard(client, se_id, report_month, 'cash_out');
    const cash_out_report_id = await getCashOutReportId(client, se_id, report_month);

    // collect any ad-hoc names
    const assetNames = new Set();
    const expenseNames = new Set();
    for (const t of transactions || []) {
      if (t.asset_name) assetNames.add(t.asset_name);
      if (t.expense_name) expenseNames.add(t.expense_name);

      const dExp = t.__dynamicExpenses || t.dynamic_expenses || {};
      const dAst = t.__dynamicAssets || t.dynamic_assets || {};
      Object.keys(dExp).forEach((k) => expenseNames.add(k));
      Object.keys(dAst).forEach((k) => assetNames.add(k));
    }

    const assetMap = {};
    for (const n of assetNames) assetMap[n.toLowerCase()] = await getOrCreateAssetByName(client, n, 0);

    const expenseMap = {};
    for (const n of expenseNames) expenseMap[n.toLowerCase()] = await getOrCreateExpenseByName(client, n, 0);

    let inserted = 0;

    for (const t of transactions || []) {
      const assetId =
        t.asset_id ??
        (t.asset_name ? assetMap[norm(t.asset_name).toLowerCase()] : null);
      const expenseId =
        t.expense_id ??
        (t.expense_name ? expenseMap[norm(t.expense_name).toLowerCase()] : null);

      // base
      await client.query(
        `INSERT INTO cash_out_transaction
           (cash_out_report_id, transaction_date, cash_amount, expense_id, asset_id,
            inventory_amount, liability_amount, owners_withdrawal_amount, note, entered_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [
          cash_out_report_id,
          toISODate(t.transaction_date),
          toNum(t.cash_amount),
          expenseId,
          assetId,
          toNum(t.inventory_amount),
          toNum(t.liability_amount),
          toNum(t.owners_withdrawal_amount),
          norm(t.note),
          norm(t.entered_by),
        ]
      );
      inserted++;

      // dynamic expense splits
      const dExp = t.__dynamicExpenses || t.dynamic_expenses || {};
      for (const [label, amt] of Object.entries(dExp)) {
        await client.query(
          `INSERT INTO cash_out_transaction
             (cash_out_report_id, transaction_date, cash_amount, expense_id, asset_id,
              inventory_amount, liability_amount, owners_withdrawal_amount, note, entered_by)
           VALUES ($1,$2,$3,$4,NULL,NULL,NULL,NULL,$5,$6)`,
          [
            cash_out_report_id,
            toISODate(t.transaction_date),
            toNum(amt),
            expenseMap[label.toLowerCase()],
            norm(t.note),
            norm(t.entered_by),
          ]
        );
        inserted++;
      }

      // dynamic asset splits
      const dAst = t.__dynamicAssets || t.dynamic_assets || {};
      for (const [label, amt] of Object.entries(dAst)) {
        await client.query(
          `INSERT INTO cash_out_transaction
             (cash_out_report_id, transaction_date, cash_amount, expense_id, asset_id,
              inventory_amount, liability_amount, owners_withdrawal_amount, note, entered_by)
           VALUES ($1,$2,$3,NULL,$4,NULL,NULL,NULL,$5,$6)`,
          [
            cash_out_report_id,
            toISODate(t.transaction_date),
            toNum(amt),
            assetMap[label.toLowerCase()],
            norm(t.note),
            norm(t.entered_by),
          ]
        );
        inserted++;
      }
    }

    await client.query("COMMIT");
    return { inserted };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
};

// Inventory (structured) — matches your bom/item/bill_of_materials/inventory_report tables
exports.importInventoryStructured = async ({ se_id, items = [], bom_lines = [], report_links = [] }) => {
  if (!se_id) throw new Error("Missing se_id");

  // Derive the single month this upload targets (bucketed to yyyy-mm-01)
  const months = Array.from(
    new Set((report_links || []).map((r) => monthBucket(r.month)).filter(Boolean))
  );
  if (months.length === 0) {
    const err = new Error("Missing or invalid month for inventory report.");
    err.status = 400;
    throw err;
  }
  if (months.length > 1) {
    const err = new Error("Multiple months detected in one inventory upload. Please split by month.");
    err.status = 400;
    throw err;
  }
  const guardMonth = months[0]; // yyyy-mm-01

  const client = await pgDatabase.connect();
  try {
    await client.query("BEGIN");
    await takeLock(client, `import:inventory:${se_id}:${guardMonth}`);

    // Guard: prevent duplicate inventory upload for the same SE + month
    await ensureUploadGuard(client, se_id, guardMonth, "inventory");

    let upsertedItems = 0;
    let insertedBOMLines = 0;
    let linked = 0;

    // Map bom_name (lower) -> bom_id
    const bomIdByName = new Map();

    // 1) Items (ensure + update values, optionally attach BOM)
    for (const it of items || []) {
      let bom_id = null;
      if (it.bom_name) {
        const key = norm(it.bom_name).toLowerCase();
        if (!bomIdByName.has(key)) {
          const id = await getOrCreateBOMByName(client, it.bom_name);
          bomIdByName.set(key, id);
        }
        bom_id = bomIdByName.get(key);
      }

      const item_id = await getOrCreateItemByName(client, it.item_name, {
        price: it.item_price,
        beginning: it.item_beginning_inventory,
        less: it.item_less_count,
        bom_id,
      });

      await client.query(
        `UPDATE item
           SET item_price = COALESCE($2, item_price),
               item_beginning_inventory = COALESCE($3, item_beginning_inventory),
               item_less_count = COALESCE($4, item_less_count),
               bom_id = COALESCE($5, bom_id)
         WHERE item_id = $1`,
        [
          item_id,
          toNum(it.item_price),
          toNum(it.item_beginning_inventory),
          toNum(it.item_less_count),
          bom_id,
        ]
      );
      upsertedItems++;
    }

    // 2) BOM lines (upsert per raw_material_name, per BOM)
    for (const bl of bom_lines || []) {
      const bomKey = norm(bl.bom_name || "Default BOM").toLowerCase();
      let bom_id = bomIdByName.get(bomKey);
      if (!bom_id) {
        bom_id = await getOrCreateBOMByName(client, bl.bom_name || "Default BOM");
        bomIdByName.set(bomKey, bom_id);
      }
      await upsertBOMLine(
        client,
        bom_id,
        bl.raw_material_name,
        bl.raw_material_qty,
        bl.raw_material_price
      );
      insertedBOMLines++;
    }

    // 3) Report links (inventory_report) — use month bucket for uniqueness per month
    for (const rl of report_links || []) {
      const { rows } = await client.query(
        `SELECT item_id FROM item WHERE lower(item_name) = lower($1) LIMIT 1`,
        [norm(rl.item_name)]
      );
      if (!rows[0]) continue;

      const res = await client.query(
        `INSERT INTO inventory_report (se_id, "month", item_id)
         VALUES ($1, $2::date, $3)
         ON CONFLICT (se_id, "month", item_id) DO NOTHING`,
        [se_id, monthBucket(rl.month), rows[0].item_id]
      );
      linked += res.rowCount || 0;
    }

    await client.query("COMMIT");
    return { upsertedItems, insertedBOMLines, linked };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
};