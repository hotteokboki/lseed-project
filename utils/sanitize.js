// utils/sanitize.js

function stripDangerousChars(s) {
  return String(s || "").replace(/[<>{}]/g, "");
}

const norm = (s) => String(s ?? "").trim();

const toNum = (v) =>
  v === null || v === undefined || v === "" || Number.isNaN(+v) ? null : +v;

const asDate = (s) => {
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
};

module.exports = { stripDangerousChars, norm, toNum, asDate };
