import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Box, Typography, Select, MenuItem, IconButton,
  Tooltip as MuiTooltip, useTheme, Button
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { alpha } from "@mui/material/styles";
import axiosClient from "../api/axiosClient";
import { tokens } from "../theme";
import { useAuth } from "../context/authContext";

/** Q1: Jan–Mar, Q2: Apr–Jun, Q3: Jul–Sep, Q4: Oct–Dec */
const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default:   return { from: null, to: null };
  }
};

const MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

export default function RevenueSeasonalityHeatmap({
  // Parent can lock a custom window; if so, local period controls are disabled.
  from,
  to,
  // For SE profile pages: non-coordinator can scope to a specific SE
  selectedSEId = null,
}) {
  const theme = useTheme();
  const tok = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

  // Subtle surfaces
  const panelBg = alpha(tok.primary[600], 0.55);
  const gridBg  = alpha(tok.primary[700], 0.66);
  const emptyBg = theme.palette.mode === "dark"
    ? alpha(tok.primary[300], 0.16)
    : alpha(tok.grey[900], 0.06);

  // ── Coordinator program_id (load once) ───────────────────────────────────────
  const [programId, setProgramId] = useState(null);
  const [programLoading, setProgramLoading] = useState(isCoordinator);
  useEffect(() => {
    if (!isCoordinator) return;
    let alive = true;
    (async () => {
      setProgramLoading(true);
      try {
        const resp = await axiosClient.get("/api/get-program-coordinator");
        if (!alive) return;
        setProgramId(resp?.data?.[0]?.program_id ?? null);
      } catch (e) {
        if (!alive) return;
        console.error("load coordinator program:", e?.response?.data || e.message);
        setProgramId(null);
      } finally {
        if (alive) setProgramLoading(false);
      }
    })();
    return () => { alive = false; };
  }, [isCoordinator]);

  // ── Period controls (local unless parent locked range) ───────────────────────
  const now = new Date();
  const fallbackYear = now.getFullYear();

  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");
  const [selectedYear, setSelectedYear] = useState(fallbackYear);

  const canUseLocalFilters = !from && !to;

  // ── Fetch ALL months for scope (to build dynamic Year/Quarter options) ──────
  const [allRows, setAllRows] = useState([]);
  const [availLoading, setAvailLoading] = useState(true);

  const scopeKey = JSON.stringify({ isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    if (isCoordinator && programLoading) return;

    (async () => {
      setAvailLoading(true);
      try {
        const params = {};
        if (isCoordinator && programId) params.program_id = programId;
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;
        // no from/to → get the whole scope for availability
        const { data } = await axiosClient.get("/api/finance/revenue-seasonality", { params });
        if (!alive) return;
        setAllRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (!alive) return;
        console.error("load seasonality availability:", e?.response?.data || e.message);
        setAllRows([]);
      } finally {
        if (alive) setAvailLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [scopeKey, programLoading, isCoordinator, programId, selectedSEId]);

  // Derive available years & quarters from allRows
  const availableYears = useMemo(() => {
    const ys = new Set(allRows.map(r => Number(r.year)));
    const arr = Array.from(ys).filter(y => Number.isFinite(y)).sort((a,b) => a - b);
    return arr.length ? arr : [fallbackYear];
  }, [allRows, fallbackYear]);

  const quartersByYear = useMemo(() => {
    const map = new Map(); // year -> Set("Q1"...)
    for (const r of allRows) {
      const y = Number(r.year);
      const m = Number(r.month);
      if (!Number.isFinite(y) || !Number.isFinite(m)) continue;
      const q = m <= 3 ? "Q1" : m <= 6 ? "Q2" : m <= 9 ? "Q3" : "Q4";
      if (!map.has(y)) map.set(y, new Set());
      map.get(y).add(q);
    }
    return map;
  }, [allRows]);

  const quarterOptionsFor = (y) => Array.from(quartersByYear.get(y) ?? []).sort();

  // Keep selection valid when availability changes
  useEffect(() => {
    if (!availableYears.length) return;
    if (!availableYears.includes(selectedYear)) {
      setSelectedYear(availableYears[availableYears.length - 1]); // latest year
    }
  }, [availableYears, selectedYear]);

  useEffect(() => {
    if (periodMode !== "quarterly") return;
    const qs = quarterOptionsFor(selectedYear);
    if (qs.length && !qs.includes(selectedQuarter)) {
      setSelectedQuarter(qs[0]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [periodMode, selectedYear, quartersByYear]);

  // Defaults snapshot for "Clear Filter"
  const defaultsRef = useRef(null);
  useEffect(() => {
    if (!canUseLocalFilters || availLoading) return;
    defaultsRef.current = {
      periodMode: "overall",
      selectedYear: availableYears[availableYears.length - 1] ?? fallbackYear,
      selectedQuarter: quarterOptionsFor(
        availableYears[availableYears.length - 1] ?? fallbackYear
      )[0] ?? "Q1",
    };
    // sync current state to sensible defaults for this scope
    setSelectedYear(defaultsRef.current.selectedYear);
    setSelectedQuarter(defaultsRef.current.selectedQuarter);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scopeKey, availLoading]);

  const isDirty = useMemo(() => {
    if (!canUseLocalFilters || !defaultsRef.current) return false;
    const d = defaultsRef.current;
    return (
      periodMode !== d.periodMode ||
      selectedYear !== d.selectedYear ||
      selectedQuarter !== d.selectedQuarter
    );
  }, [canUseLocalFilters, periodMode, selectedYear, selectedQuarter]);

  const handleClear = () => {
    if (!defaultsRef.current) return;
    const d = defaultsRef.current;
    setPeriodMode(d.periodMode);
    setSelectedYear(d.selectedYear);
    setSelectedQuarter(d.selectedQuarter);
  };

  // Compute effective window
  const { effFrom, effTo, label } = useMemo(() => {
    if (!canUseLocalFilters) {
      return { effFrom: from ?? null, effTo: to ?? null, label: "Custom" };
    }
    if (periodMode === "overall") return { effFrom: null, effTo: null, label: "Overall" };
    if (periodMode === "yearly") {
      const y = selectedYear;
      return { effFrom: `${y}-01-01`, effTo: `${Number(y) + 1}-01-01`, label: `Year ${y}` };
    }
    const r = quarterRange(selectedYear, selectedQuarter);
    return { effFrom: r.from, effTo: r.to, label: `${selectedYear} ${selectedQuarter}` };
  }, [canUseLocalFilters, from, to, periodMode, selectedYear, selectedQuarter]);

  // ── Fetch filtered rows for the heatmap ──────────────────────────────────────
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [errMsg, setErrMsg] = useState("");

  const fetchKey = JSON.stringify({ effFrom, effTo, isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setErrMsg("");
    setRows([]);

    if (isCoordinator && programLoading) return;

    (async () => {
      try {
        const params = {};
        if (effFrom) params.from = effFrom;
        if (effTo)   params.to   = effTo;
        if (isCoordinator && programId) params.program_id = programId;
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;

        const { data } = await axiosClient.get("/api/finance/revenue-seasonality", { params });
        if (!alive) return;
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (!alive) return;
        console.error("load revenue seasonality:", e?.response?.data || e.message);
        setErrMsg("Failed to load revenue seasonality.");
        setRows([]);
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [fetchKey, programLoading, isCoordinator, programId, selectedSEId]);

  // Normalize grid + stats
  const { years, grid, maxVal, minVal } = useMemo(() => {
    const ys = Array.from(new Set(rows.map(r => Number(r.year)))).sort((a,b) => a - b);
    const g = new Map();
    let max = 0;
    let min = Infinity;
    for (const r of rows) {
      const y = Number(r.year), m = Number(r.month);
      if (!Number.isFinite(y) || !Number.isFinite(m)) continue;
      const v = Number(r.revenue || 0);
      g.set(`${y}-${m}`, v);
      if (v > max) max = v;
      if (v < min) min = v;
    }
    if (!isFinite(min)) min = 0;
    return { years: ys, grid: g, maxVal: max || 1, minVal: min };
  }, [rows]);

  // Color scale (green = higher revenue)
  const cellColor = (v) => {
    if (!v) return emptyBg;
    const t = Math.sqrt(Math.min(1, Math.max(0, v / maxVal)));
    const low  = theme.palette.mode === "dark" ? tok.primary[600] : tok.grey[300];
    const high = tok.greenAccent[500];
    const toRGB = (h) => h.replace("#", "").match(/.{2}/g).map(x => parseInt(x, 16));
    const [lr, lg, lb] = toRGB(low); const [hr, hg, hb] = toRGB(high);
    const r = Math.round(lr + (hr - lr) * t);
    const g = Math.round(lg + (hg - lg) * t);
    const b = Math.round(lb + (hb - lb) * t);
    return `rgb(${r},${g},${b})`;
  };

  const peso = (n) => `₱${Number(n || 0).toLocaleString("en-PH", { maximumFractionDigits: 0 })}`;

  const controlsDisabled = !canUseLocalFilters || loading || availLoading || (isCoordinator && programLoading);

  return (
    <Box bgcolor={tok.primary[400]} p="20px" pb={3} sx={{ borderRadius: 2 }}>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2} flexWrap="wrap" gap={1.5}>
        <Box display="flex" alignItems="center" gap={2} flexWrap="wrap">
          <Typography variant="h3" fontWeight="bold" color={tok.greenAccent[500]}>
            Revenue Seasonality
          </Typography>

          {/* Gradient legend */}
          <Box display="flex" alignItems="center" gap={1}>
            <Typography variant="caption" color={tok.grey[300]}>{peso(minVal)}</Typography>
            <Box
              sx={{
                width: 140, height: 10, borderRadius: 999,
                background: `linear-gradient(90deg, ${cellColor(0)} 0%, ${cellColor(maxVal)} 100%)`,
                border: `1px solid ${alpha(theme.palette.common.black, theme.palette.mode === "dark" ? 0.25 : 0.08)}`
              }}
            />
            <Typography variant="caption" color={tok.grey[300]}>{peso(maxVal)}</Typography>
          </Box>

          {/* Help */}
          <MuiTooltip
            arrow placement="top"
            title={
              <Box sx={{ maxWidth: 380, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">How to read • Revenue Seasonality</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Rows are <b>years</b>, columns are <b>months</b>. Cells show portfolio revenue for that month.
                </Typography>
                <Typography variant="body2" sx={{ mt: 0.5 }}>
                  Brighter/greener = higher revenue. Hover for exact amount; numbers in cells are in <b>₱ ‘000</b>.
                </Typography>
              </Box>
            }
          >
            <IconButton sx={{ color: tok.grey[300], ml: 0.5 }}>
              <HelpOutlineIcon />
            </IconButton>
          </MuiTooltip>

          {errMsg && <Typography variant="caption" color="#f44336">{errMsg}</Typography>}
        </Box>

        {/* Period controls + Clear Filter */}
        <Box display="flex" alignItems="center" gap={1}>
          <Select
            value={periodMode}
            onChange={(e) => setPeriodMode(e.target.value)}
            disabled={controlsDisabled}
            sx={{
              height: 40, minWidth: 130,
              backgroundColor: controlsDisabled ? tok.grey[600] : tok.blueAccent[600],
              color: tok.grey[100], fontWeight: "bold",
              "& .MuiSelect-icon": { color: tok.grey[100] },
              "& fieldset": { border: "none" },
            }}
          >
            <MenuItem value="overall">Overall</MenuItem>
            <MenuItem value="quarterly">Quarterly</MenuItem>
            <MenuItem value="yearly">Yearly</MenuItem>
          </Select>

          {periodMode === "quarterly" && canUseLocalFilters && (
            <Select
              value={selectedQuarter}
              onChange={(e) => setSelectedQuarter(e.target.value)}
              disabled={controlsDisabled || quarterOptionsFor(selectedYear).length === 0}
              sx={{
                height: 40, minWidth: 100,
                backgroundColor: controlsDisabled ? tok.grey[600] : tok.blueAccent[600],
                color: tok.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: tok.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {quarterOptionsFor(selectedYear).map(q => (
                <MenuItem key={q} value={q}>{q}</MenuItem>
              ))}
            </Select>
          )}

          {(periodMode === "quarterly" || periodMode === "yearly") && canUseLocalFilters && (
            <Select
              value={selectedYear}
              onChange={(e) => setSelectedYear(Number(e.target.value))}
              disabled={controlsDisabled || availableYears.length === 0}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: controlsDisabled ? tok.grey[600] : tok.blueAccent[600],
                color: tok.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: tok.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {availableYears.map(y => (<MenuItem key={y} value={y}>{y}</MenuItem>))}
            </Select>
          )}

          {isDirty && canUseLocalFilters && (
            <Button
              onClick={handleClear}
              variant="outlined"
              sx={{
                height: 40, minWidth: 120,
                borderColor: tok.grey[100], color: tok.grey[100],
                backgroundColor: tok.blueAccent[600], fontWeight: "bold",
                "&:hover": { backgroundColor: tok.blueAccent[700] },
              }}
            >
              CLEAR FILTER
            </Button>
          )}
        </Box>
      </Box>

      {/* Heatmap */}
      <Box sx={{ p: 1.5, borderRadius: 2, bgcolor: panelBg, border: `1px solid ${theme.palette.divider}` }}>
        <Box sx={{ overflowX: "auto", p: 2, borderRadius: 2, bgcolor: gridBg }}>
          <Box sx={{ display: "grid", gridTemplateColumns: `100px repeat(12, 56px)`, gap: 6, alignItems: "center" }}>
            {/* header */}
            <Box />
            {MONTH_LABELS.map(m => (
              <Box key={m} sx={{ textAlign: "center", color: "text.secondary", fontSize: 13 }}>{m}</Box>
            ))}

            {/* rows */}
            {years.length === 0 ? (
              <Box sx={{ gridColumn: "1 / span 13", textAlign: "center", color: tok.grey[300], py: 6 }}>
                {loading ? "Loading…" : "No data to display."}
              </Box>
            ) : years.map((y) => (
              <React.Fragment key={y}>
                <Box sx={{ display: "flex", alignItems: "center", fontWeight: 700, color: tok.grey[100] }}>{y}</Box>
                {MONTH_LABELS.map((_, i) => {
                  const v = grid.get(`${y}-${i+1}`) ?? 0;
                  const bgc = cellColor(v);
                  return (
                    <Box
                      key={`${y}-${i}`}
                      sx={{
                        height: 38, borderRadius: 1.25, bgcolor: bgc,
                        display: "flex", alignItems: "center", justifyContent: "center",
                        px: 0.75, color: theme.palette.mode === "dark" ? "rgba(255,255,255,.92)" : "rgba(0,0,0,.85)",
                        fontSize: 12, fontVariantNumeric: "tabular-nums",
                        border: `1px solid ${alpha(theme.palette.common.black, theme.palette.mode === "dark" ? 0.25 : 0.08)}`,
                        boxShadow: v > 0 ? "inset 0 0 0 1px rgba(255,255,255,.06)" : "none",
                      }}
                      title={`${y}-${String(i+1).padStart(2,"0")}: ${peso(v)}`}
                    >
                      {loading ? "…" : v > 0 ? `₱${Math.round(v/1000).toLocaleString()}k` : "—"}
                    </Box>
                  );
                })}
              </React.Fragment>
            ))}
          </Box>
        </Box>
      </Box>

      <Typography variant="caption" sx={{ mt: 1.25, display: "block", color: tok.grey[300] }}>
        {label}: higher intensity = higher monthly revenue. Values show in ₱ ‘000 inside cells (hover for exact).
      </Typography>
    </Box>
  );
}