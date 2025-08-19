const pgDatabase = require("../database.js"); // your pg Pool
const crypto = require("crypto");

// ---------------- helpers ----------------
const norm = (s) => String(s ?? "").trim();
const num0 = (v) => (v === "" || v == null || Number.isNaN(+v) ? 0 : +v);

/** Excel serial (days since 1899-12-30) → UTC date */
function fromExcelSerial(n) {
  const epoch = Date.UTC(1899, 11, 30);
  return new Date(epoch + Math.round(Number(n) * 86400000));
}
const pad2 = (n) => String(n).padStart(2, "0");

// Robust "YYYY-MM-DD" without timezone shifts
function toISODate(s) {
  if (s == null || s === "") return null;
  if (typeof s === "number" && isFinite(s)) {
    const d = fromExcelSerial(s);
    return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
  }
  const str = String(s).trim();

  let m = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;

  m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const mm = pad2(+m[1]), dd = pad2(+m[2]), yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }

  const d = new Date(str);
  if (Number.isNaN(d.getTime())) return null;
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

// Month bucket 'YYYY-MM-01'
function monthBucket(s) {
  const iso = toISODate(s);
  if (!iso) return null;
  return `${iso.slice(0, 7)}-01`;
}

const sha1 = (s) => crypto.createHash("sha1").update(s).digest("hex");
const joinK = (parts) => parts.map((p) => (p == null ? "" : String(p))).join("|");

// Prefer sheet-provided client key; else derive a stable base key from the row (no classification)
function baseRowKey(reportId, row) {
  if (row.client_txn_key) return String(row.client_txn_key);
  return sha1(
    joinK([
      reportId,
      toISODate(row.transaction_date),
      num0(row.cash_amount).toFixed(2),
      norm(row.note),
      norm(row.entered_by),
    ])
  );
}

// Deterministic keys for split lines (keeps 1:N rows idempotent)
function splitKey(baseKey, tag, id, amt) {
  return `${baseKey}|${tag}:${id || "null"}|amt:${num0(amt).toFixed(2)}`;
}

// Safe adder (skips blanks)
function addName(set, s) {
  const n = norm(s);
  if (n) set.add(n);
}

// advisory lock per import
async function takeLock(client, key) {
  if (!key) return;
  await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [key]);
}

// ---------- reference upserts ----------
async function getOrCreateAssetByName(client, name, amount = 0) {
  const n = norm(name);
  if (!n) return null;
  const { rows } = await client.query(
    `INSERT INTO asset (asset_name, asset_amount)
     VALUES ($1, $2)
     ON CONFLICT (asset_name)
       DO UPDATE SET asset_name = EXCLUDED.asset_name
     RETURNING asset_id`,
    [n, num0(amount)]
  );
  return rows[0].asset_id;
}

async function getOrCreateExpenseByName(client, name, amount = 0) {
  const n = norm(name);
  if (!n) return null;
  const { rows } = await client.query(
    `INSERT INTO expense (expense_name, expense_amount)
     VALUES ($1, $2)
     ON CONFLICT (expense_name)
       DO UPDATE SET expense_name = EXCLUDED.expense_name
     RETURNING expense_id`,
    [n, num0(amount)]
  );
  return rows[0].expense_id;
}

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

// No unique on item_name; SELECT-first by lower(name)
async function getOrCreateItemByName(client, name, defaults = {}) {
  const n = norm(name);
  if (!n) return null;

  const find = await client.query(
    `SELECT item_id FROM item WHERE lower(item_name) = lower($1) LIMIT 1`,
    [n]
  );
  if (find.rows[0]) return find.rows[0].item_id;

  const { price = 0, beginning = 0, less = 0, bom_id = null } = defaults;

  const { rows } = await client.query(
    `INSERT INTO item (item_name, item_price, item_beginning_inventory, item_less_count, bom_id)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING item_id`,
    [n, num0(price), num0(beginning), num0(less), bom_id]
  );
  return rows[0].item_id;
}

// bill_of_materials upsert per (bom_id, lower(raw_material_name))
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
      [found.rows[0].record_id, num0(qty), num0(price)]
    );
  } else {
    await client.query(
      `INSERT INTO bill_of_materials
         (bom_id, raw_material_name, raw_material_qty, raw_material_price)
       VALUES ($1, $2, $3, $4)`,
      [bom_id, n, num0(qty), num0(price)]
    );
  }
}

// -------- optional guard (inventory preflight only) --------
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

// ---------- totals maintenance (idempotent) ----------
async function refreshExpenseTotals(client) {
  await client.query(`
    WITH totals AS (
      SELECT e.expense_id,
             COALESCE(x.total, 0) AS total
      FROM expense e
      LEFT JOIN (
        SELECT expense_id, SUM(cash_amount) AS total
        FROM cash_out_transaction
        WHERE expense_id IS NOT NULL
        GROUP BY expense_id
      ) x ON x.expense_id = e.expense_id
    )
    UPDATE expense e
       SET expense_amount = t.total
      FROM totals t
     WHERE e.expense_id = t.expense_id
  `);
}

async function refreshAssetTotals(client) {
  await client.query(`
    WITH totals AS (
      SELECT a.asset_id,
             COALESCE(o.total_out, 0) + COALESCE(i.total_in, 0) AS total
      FROM asset a
      LEFT JOIN (
        SELECT asset_id, SUM(cash_amount) AS total_out
        FROM cash_out_transaction
        WHERE asset_id IS NOT NULL
        GROUP BY asset_id
      ) o ON o.asset_id = a.asset_id
      LEFT JOIN (
        SELECT asset_id, SUM(cash_amount) AS total_in
        FROM cash_in_transaction
        WHERE asset_id IS NOT NULL
        GROUP BY asset_id
      ) i ON i.asset_id = a.asset_id
    )
    UPDATE asset a
       SET asset_amount = t.total
      FROM totals t
     WHERE a.asset_id = t.asset_id
  `);
}

// ---------- report header helpers ----------
async function getCashInReportId(client, se_id, report_month) {
  const m = monthBucket(report_month);
  try {
    const { rows } = await client.query(
      `INSERT INTO cash_in_report (se_id, report_month)
       VALUES ($1, $2::date)
       RETURNING cash_in_report_id`,
      [se_id, m]
    );
    return rows[0].cash_in_report_id;
  } catch (e) {
    if (e.code === "23505") {
      const { rows } = await client.query(
        `SELECT cash_in_report_id
           FROM cash_in_report
          WHERE se_id = $1 AND report_month = $2::date
          LIMIT 1`,
        [se_id, m]
      );
      if (rows[0]) return rows[0].cash_in_report_id;
    }
    throw e;
  }
}

async function getCashOutReportId(client, se_id, report_month) {
  const m = monthBucket(report_month);
  try {
    const { rows } = await client.query(
      `INSERT INTO cash_out_report (se_id, report_month)
       VALUES ($1, $2::date)
       RETURNING cash_out_report_id`,
      [se_id, m]
    );
    return rows[0].cash_out_report_id;
  } catch (e) {
    if (e.code === "23505") {
      const { rows } = await client.query(
        `SELECT cash_out_report_id
           FROM cash_out_report
          WHERE se_id = $1 AND report_month = $2::date
          LIMIT 1`,
        [se_id, m]
      );
      if (rows[0]) return rows[0].cash_out_report_id;
    }
    throw e;
  }
}

// ---------- one-bucket normalization ----------
function normalizeInflow(t) {
  const a = {
    cash: num0(t.cash_amount),
    sales: num0(t.sales_amount),
    other: num0(t.other_revenue_amount),
    liability: num0(t.liability_amount),
    capital: num0(t.owners_capital_amount),
  };
  const order = ["sales", "other", "liability", "capital", "cash"];
  let kept = false;
  for (const k of order) {
    if (a[k] > 0) {
      if (!kept) kept = true; else a[k] = 0;
    }
  }
  return a;
}

function normalizeOutflow(t) {
  const a = {
    cash: num0(t.cash_amount),
    inventory: num0(t.inventory_amount),
    liability: num0(t.liability_amount),
    withdraw: num0(t.owners_withdrawal_amount),
  };
  const order = ["inventory", "liability", "withdraw", "cash"];
  let kept = false;
  for (const k of order) {
    if (a[k] > 0) {
      if (!kept) kept = true; else a[k] = 0;
    }
  }
  return a;
}

// ---------- label derivation with CANONICALIZATION ----------
const LABEL_KEYS_EXPENSE = [
  "expense_name", "expense", "expenseName",
  "category", "Category", "account", "Account",
];
const LABEL_KEYS_ASSET = ["asset_name", "asset", "assetName"];

// turn " Workshop   rent " -> "workshop rent"
const lowkey = (s) => norm(s).toLowerCase().replace(/\s+/g, " ");

// Canonical names for expenses (synonyms collapse to one)
const EXPENSE_CANON = new Map([
  // Transportation & Delivery
  ["transportation & delivery", "Transportation & Delivery"],
  ["transportation and delivery", "Transportation & Delivery"],
  ["local deliveries", "Transportation & Delivery"],

  // Rent
  ["rent", "Rent"],
  ["workshop rent", "Rent"],

  // Marketing
  ["marketing & advertising", "Marketing & Advertising"],
  ["marketing and advertising", "Marketing & Advertising"],
  ["fb ads, flyers", "Marketing & Advertising"],

  // Raw materials
  ["raw materials", "Raw Materials"],
  ["pul/tpu waterproof fabric", "Raw Materials"],

  // Salaries / wages
  ["salaries", "Salaries"],
  ["production staff wages", "Salaries"],

  // Utilities
  ["electric & water bills", "Electric & water bills"],
  ["electric and water bills", "Electric & water bills"],

  // Packaging
  ["packaging supplies", "Packaging Supplies"],
  ["pouches, labels, cartons", "Packaging Supplies"],
]);

function canonExpenseName(nameOrNote) {
  if (!nameOrNote) return null;
  const k = lowkey(nameOrNote);
  return EXPENSE_CANON.get(k) || norm(nameOrNote); // keep original if no match
}

function pickFirst(obj, keys) {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, k)) {
      const v = norm(obj[k]);
      if (v) return v.replace(/\s+/g, " ");
    }
  }
  return null;
}

function deriveExpenseLabel(t) {
  // 1) take explicit label if present
  const explicit = pickFirst(t, LABEL_KEYS_EXPENSE);
  if (explicit) return canonExpenseName(explicit);

  // 2) else try note mapping into canonical
  const fromNote = canonExpenseName(t.note);
  return fromNote || null;
}

function deriveAssetLabel(t) {
  return pickFirst(t, LABEL_KEYS_ASSET);
}

// ---------------- PUBLIC MODEL FNS ----------------
exports.ensureRefs = async ({ assets = [], expenses = [] }) => {
  const client = await pgDatabase.connect();
  try {
    await client.query("BEGIN");

    const assetMap = {};
    const expenseMap = {};

    for (const a of [...new Set(assets.filter(Boolean).map(norm))]) {
      const id = await getOrCreateAssetByName(client, a, 0);
      assetMap[a.toLowerCase()] = id;
    }
    for (const e of [...new Set(expenses.filter(Boolean).map(norm))]) {
      const canon = canonExpenseName(e);
      const id = await getOrCreateExpenseByName(client, canon, 0);
      expenseMap[canon.toLowerCase()] = id;
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

// -------- Cash In (split-safe + stable keys + base purge when splits) --------
exports.importCashInStructured = async ({ se_id, report_month, transactions = [] }) => {
  if (!se_id || !report_month) throw new Error("Missing se_id or report_month");

  const client = await pgDatabase.connect();
  try {
    await client.query("BEGIN");
    await takeLock(client, `import:cashin:${se_id}:${monthBucket(report_month)}`);

    const cash_in_report_id = await getCashInReportId(client, se_id, report_month);

    // Collect ad-hoc asset names (from asset_name and dynamic assets)
    const assetNames = new Set();
    for (const t of transactions || []) {
      addName(assetNames, deriveAssetLabel(t));
      const dyn = t.__dynamicAssets || t.dynamic_assets || {};
      Object.keys(dyn).forEach((k) => addName(assetNames, k));
    }
    const assetMap = {};
    for (const n of assetNames) {
      assetMap[n.toLowerCase()] = await getOrCreateAssetByName(client, n, 0);
    }

    let upserted = 0;

    for (const t of transactions || []) {
      const a = normalizeInflow(t);
      const dyn = t.__dynamicAssets || t.dynamic_assets || {};
      const hasSplits = Object.keys(dyn).length > 0;

      const baseKey = baseRowKey(cash_in_report_id, t);
      const assetIdBase =
        t.asset_id ??
        (deriveAssetLabel(t) ? assetMap[deriveAssetLabel(t).toLowerCase()] : null);

      if (!hasSplits) {
        const amt =
          a.sales || a.other || a.liability || a.capital || a.cash;

        const clientKey = baseKey;

        await client.query(
          `INSERT INTO cash_in_transaction
             (client_txn_key, cash_in_report_id, transaction_date,
              cash_amount, sales_amount, other_revenue_amount,
              asset_id, liability_amount, owners_capital_amount, note, entered_by)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (client_txn_key) DO UPDATE
           SET cash_amount           = EXCLUDED.cash_amount,
               sales_amount          = EXCLUDED.sales_amount,
               other_revenue_amount  = EXCLUDED.other_revenue_amount,
               asset_id              = COALESCE(EXCLUDED.asset_id, cash_in_transaction.asset_id),
               liability_amount      = EXCLUDED.liability_amount,
               owners_capital_amount = EXCLUDED.owners_capital_amount,
               note                  = EXCLUDED.note,
               entered_by            = EXCLUDED.entered_by`,
          [
            clientKey,
            cash_in_report_id,
            toISODate(t.transaction_date),
            a.cash, a.sales, a.other,
            assetIdBase,
            a.liability, a.capital,
            norm(t.note), norm(t.entered_by),
          ]
        );
        upserted++;
      } else {
        // Purge base row if splits exist
        await client.query(
          `DELETE FROM cash_in_transaction
           WHERE cash_in_report_id = $1 AND client_txn_key = $2`,
          [cash_in_report_id, baseKey]
        );

        for (const [label, amtRaw] of Object.entries(dyn)) {
          const amt = num0(amtRaw);
          if (amt <= 0) continue;
          const assetId = assetMap[norm(label).toLowerCase()];
          const clientKey = splitKey(baseKey, "ast", assetId, amt);

          await client.query(
            `INSERT INTO cash_in_transaction
               (client_txn_key, cash_in_report_id, transaction_date,
                cash_amount, sales_amount, other_revenue_amount,
                asset_id, liability_amount, owners_capital_amount, note, entered_by)
             VALUES ($1,$2,$3,$4,0,0,$5,0,0,$6,$7)
             ON CONFLICT (client_txn_key) DO UPDATE
             SET cash_amount = EXCLUDED.cash_amount,
                 asset_id    = COALESCE(EXCLUDED.asset_id, cash_in_transaction.asset_id),
                 note        = EXCLUDED.note,
                 entered_by  = EXCLUDED.entered_by`,
            [
              clientKey,
              cash_in_report_id,
              toISODate(t.transaction_date),
              amt,
              assetId,
              norm(t.note), norm(t.entered_by),
            ]
          );
          upserted++;
        }
      }
    }

    await refreshAssetTotals(client);

    await client.query("COMMIT");
    return { upserted };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
};

// -------- Cash Out (mutually-exclusive link; canonical names; no auto-scan) --------
exports.importCashOutStructured = async ({ se_id, report_month, transactions = [] }) => {
  if (!se_id || !report_month) throw new Error("Missing se_id or report_month");

  const client = await pgDatabase.connect();
  try {
    await client.query("BEGIN");
    await takeLock(client, `import:cashout:${se_id}:${monthBucket(report_month)}`);

    const cash_out_report_id = await getCashOutReportId(client, se_id, report_month);

    // Always have a concrete expense for cash-only rows with no explicit label
    const UNC_NAME = "Uncategorized Expense";
    const uncategorizedExpenseId = await getOrCreateExpenseByName(client, UNC_NAME, 0);

    // Collect names from the sheet (canonicalized)
    const assetNames = new Set();
    const expenseNames = new Set();
    for (const t of transactions || []) {
      const expLabel = deriveExpenseLabel(t); // already canonicalized
      const astLabel = deriveAssetLabel(t);
      addName(expenseNames, expLabel);
      addName(assetNames, astLabel);

      const dExp = t.__dynamicExpenses || t.dynamic_expenses || {};
      const dAst = t.__dynamicAssets || t.dynamic_assets || {};
      Object.keys(dExp).forEach((k) => addName(expenseNames, canonExpenseName(k)));
      Object.keys(dAst).forEach((k) => addName(assetNames, k));
    }
    const assetMap = {};
    for (const n of assetNames) if (n) assetMap[n.toLowerCase()] = await getOrCreateAssetByName(client, n, 0);
    const expenseMap = {};
    for (const n of expenseNames) if (n) expenseMap[n.toLowerCase()] = await getOrCreateExpenseByName(client, n, 0);

    let upserted = 0;

    for (const t of transactions || []) {
      const a = normalizeOutflow(t);
      const baseKey = baseRowKey(cash_out_report_id, t);

      // base labels (canonicalized)
      let assetIdBase =
        t.asset_id ??
        (deriveAssetLabel(t) ? assetMap[deriveAssetLabel(t).toLowerCase()] : null);

      const derivedExpense = deriveExpenseLabel(t); // canonical
      let expenseIdBase =
        t.expense_id ??
        (derivedExpense ? expenseMap[derivedExpense.toLowerCase()] : null);

      // explicit dynamic maps (no auto inference)
      const dExpRaw = t.__dynamicExpenses || t.dynamic_expenses || {};
      const dExp = Object.fromEntries(
        Object.entries(dExpRaw).map(([k, v]) => [canonExpenseName(k), v])
      );
      const dAst = t.__dynamicAssets || t.dynamic_assets || {};
      const hasExpSplits = Object.keys(dExp).length > 0;
      const hasAstSplits = Object.keys(dAst).length > 0;

      // Enforce mutual exclusivity
      let rowMode = null; // "asset" | "expense" | "unc"
      if (assetIdBase || hasAstSplits) rowMode = "asset";
      else if (expenseIdBase || hasExpSplits) rowMode = "expense";
      else {
        const onlyCashBase =
          a.cash > 0 && a.inventory === 0 && a.liability === 0 && a.withdraw === 0;
        rowMode = onlyCashBase ? "unc" : "expense";
      }

      if (!hasExpSplits && !hasAstSplits) {
        // Single-line record (one positive bucket only)
        const amt = a.inventory || a.liability || a.withdraw || a.cash;
        const clientKey = baseKey;

        // exclusive linking
        let expId = null;
        let astId = null;
        if (rowMode === "asset") {
          astId = assetIdBase;
        } else if (rowMode === "expense") {
          expId = expenseIdBase || uncategorizedExpenseId;
        } else {
          expId = uncategorizedExpenseId;
        }

        await client.query(
          `INSERT INTO cash_out_transaction
             (client_txn_key, cash_out_report_id, transaction_date,
              cash_amount, expense_id, asset_id,
              inventory_amount, liability_amount, owners_withdrawal_amount,
              note, entered_by)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (client_txn_key) DO UPDATE
           SET cash_amount              = EXCLUDED.cash_amount,
               expense_id               = EXCLUDED.expense_id,
               asset_id                 = EXCLUDED.asset_id,
               inventory_amount         = EXCLUDED.inventory_amount,
               liability_amount         = EXCLUDED.liability_amount,
               owners_withdrawal_amount = EXCLUDED.owners_withdrawal_amount,
               note                     = EXCLUDED.note,
               entered_by               = EXCLUDED.entered_by`,
          [
            clientKey,
            cash_out_report_id,
            toISODate(t.transaction_date),
            amt === a.cash ? a.cash : 0,
            expId,
            astId,
            a.inventory, a.liability, a.withdraw,
            norm(t.note), norm(t.entered_by),
          ]
        );
        upserted++;
        continue;
      }

      // Splits exist → purge prior base (if any) for this row
      await client.query(
        `DELETE FROM cash_out_transaction
         WHERE cash_out_report_id = $1 AND client_txn_key = $2`,
        [cash_out_report_id, baseKey]
      );

      if (rowMode === "asset") {
        if (assetIdBase && num0(a.cash) > 0) {
          const clientKey = splitKey(baseKey, "ast", assetIdBase, a.cash);
          await client.query(
            `INSERT INTO cash_out_transaction
               (client_txn_key, cash_out_report_id, transaction_date,
                cash_amount, expense_id, asset_id,
                inventory_amount, liability_amount, owners_withdrawal_amount,
                note, entered_by)
             VALUES ($1,$2,$3,$4,NULL,$5,0,0,0,$6,$7)
             ON CONFLICT (client_txn_key) DO UPDATE
             SET cash_amount = EXCLUDED.cash_amount,
                 asset_id    = EXCLUDED.asset_id,
                 note        = EXCLUDED.note,
                 entered_by  = EXCLUDED.entered_by`,
            [
              clientKey,
              cash_out_report_id,
              toISODate(t.transaction_date),
              a.cash,
              assetIdBase,
              norm(t.note), norm(t.entered_by),
            ]
          );
          upserted++;
        }
        for (const [label, amtRaw] of Object.entries(dAst)) {
          const amt = num0(amtRaw);
          if (amt <= 0) continue;
          const assetId = assetMap[norm(label).toLowerCase()];
          const clientKey = splitKey(baseKey, "ast", assetId, amt);
          await client.query(
            `INSERT INTO cash_out_transaction
               (client_txn_key, cash_out_report_id, transaction_date,
                cash_amount, expense_id, asset_id,
                inventory_amount, liability_amount, owners_withdrawal_amount,
                note, entered_by)
             VALUES ($1,$2,$3,$4,NULL,$5,0,0,0,$6,$7)
             ON CONFLICT (client_txn_key) DO UPDATE
             SET cash_amount = EXCLUDED.cash_amount,
                 asset_id    = EXCLUDED.asset_id,
                 note        = EXCLUDED.note,
                 entered_by  = EXCLUDED.entered_by`,
            [
              clientKey,
              cash_out_report_id,
              toISODate(t.transaction_date),
              amt,
              assetId,
              norm(t.note), norm(t.entered_by),
            ]
          );
          upserted++;
        }
      } else {
        if (expenseIdBase && num0(a.cash) > 0) {
          const clientKey = splitKey(baseKey, "exp", expenseIdBase, a.cash);
          await client.query(
            `INSERT INTO cash_out_transaction
               (client_txn_key, cash_out_report_id, transaction_date,
                cash_amount, expense_id, asset_id,
                inventory_amount, liability_amount, owners_withdrawal_amount,
                note, entered_by)
             VALUES ($1,$2,$3,$4,$5,NULL,0,0,0,$6,$7)
             ON CONFLICT (client_txn_key) DO UPDATE
             SET cash_amount = EXCLUDED.cash_amount,
                 expense_id  = EXCLUDED.expense_id,
                 note        = EXCLUDED.note,
                 entered_by  = EXCLUDED.entered_by`,
            [
              clientKey,
              cash_out_report_id,
              toISODate(t.transaction_date),
              a.cash,
              expenseIdBase,
              norm(t.note), norm(t.entered_by),
            ]
          );
          upserted++;
        }
        for (const [label, amtRaw] of Object.entries(dExp)) {
          const amt = num0(amtRaw);
          if (amt <= 0) continue;
          const expenseId = expenseMap[norm(label).toLowerCase()];
          const clientKey = splitKey(baseKey, "exp", expenseId, amt);
          await client.query(
            `INSERT INTO cash_out_transaction
               (client_txn_key, cash_out_report_id, transaction_date,
                cash_amount, expense_id, asset_id,
                inventory_amount, liability_amount, owners_withdrawal_amount,
                note, entered_by)
             VALUES ($1,$2,$3,$4,$5,NULL,0,0,0,$6,$7)
             ON CONFLICT (client_txn_key) DO UPDATE
             SET cash_amount = EXCLUDED.cash_amount,
                 expense_id  = EXCLUDED.expense_id,
                 note        = EXCLUDED.note,
                 entered_by  = EXCLUDED.entered_by`,
            [
              clientKey,
              cash_out_report_id,
              toISODate(t.transaction_date),
              amt,
              expenseId,
              norm(t.note), norm(t.entered_by),
            ]
          );
          upserted++;
        }
      }
    }

    await refreshExpenseTotals(client);
    await refreshAssetTotals(client);

    await client.query("COMMIT");
    return { upserted };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
};

// -------- Inventory (structured; supports month-scoped fields) --------
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

    // Preflight guard
    await ensureUploadGuard(client, se_id, guardMonth, "inventory");

    let upsertedItems = 0;
    let insertedBOMLines = 0;
    let linked = 0;

    // Map bom_name (lower) -> bom_id
    const bomIdByName = new Map();

    // 1) Items
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
          it.item_price == null ? null : num0(it.item_price),
          it.item_beginning_inventory == null ? null : num0(it.item_beginning_inventory),
          it.item_less_count == null ? null : num0(it.item_less_count),
          bom_id,
        ]
      );
      upsertedItems++;
    }

    // 2) BOM lines
    for (const bl of bom_lines || []) {
      const bomKey = norm(bl.bom_name || "Default BOM").toLowerCase();
      let bom_id = bomIdByName.get(bomKey);
      if (!bom_id) {
        bom_id = await getOrCreateBOMByName(client, bl.bom_name || "Default BOM");
        bomIdByName.set(bomKey, bom_id);
      }
      await upsertBOMLine(client, bom_id, bl.raw_material_name, bl.raw_material_qty, bl.raw_material_price);
      insertedBOMLines++;
    }

    // 3) Link items to inventory_report (+ optional month-scoped values)
    for (const rl of report_links || []) {
      const itemName = norm(rl.item_name);
      if (!itemName) continue;

      const { rows } = await client.query(
        `SELECT item_id FROM item WHERE lower(item_name) = lower($1) LIMIT 1`,
        [itemName]
      );
      if (!rows[0]) continue;

      const begin_qty = rl.begin_qty == null ? null : num0(rl.begin_qty);
      const begin_unit_price = rl.begin_unit_price == null ? null : num0(rl.begin_unit_price);
      const final_qty = rl.final_qty == null ? null : num0(rl.final_qty);
      const final_unit_price = rl.final_unit_price == null ? null : num0(rl.final_unit_price);

      const res = await client.query(
        `INSERT INTO inventory_report
           (se_id, "month", item_id, begin_qty, begin_unit_price, final_qty, final_unit_price)
         VALUES ($1, $2::date, $3, $4, $5, $6, $7)
         ON CONFLICT (se_id, "month", item_id)
         DO UPDATE
           SET begin_qty        = COALESCE(EXCLUDED.begin_qty,        inventory_report.begin_qty),
               begin_unit_price = COALESCE(EXCLUDED.begin_unit_price, inventory_report.begin_unit_price),
               final_qty        = COALESCE(EXCLUDED.final_qty,        inventory_report.final_qty),
               final_unit_price = COALESCE(EXCLUDED.final_unit_price, inventory_report.final_unit_price)`,
        [ se_id, guardMonth, rows[0].item_id, begin_qty, begin_unit_price, final_qty, final_unit_price ]
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