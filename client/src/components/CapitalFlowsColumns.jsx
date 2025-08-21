// src/components/CapitalFlowsColumns.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import { Box, Typography, Select, MenuItem, Button, IconButton, Tooltip as MuiTooltip, useTheme } from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid
} from "recharts";
import axiosClient from "../api/axiosClient";
import { tokens } from "../theme";
import { useAuth } from "../context/authContext";

/** Quarter ranges — Q1: Jan–Mar, Q2: Apr–Jun, Q3: Jul–Sep, Q4: Oct–Dec (end-exclusive "to") */
const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default:   return { from: null,            to: null };
  }
};

const peso = (v) => `₱${Math.round(Number(v || 0)).toLocaleString()}`;

export default function CapitalFlowsColumns({
  from, to,                      // optional: parent can lock a custom window
  selectedSEId = null            // optional: when viewing a specific SE profile (non-coordinator)
}) {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

  // colors
  const strokeGrid   = theme.palette.mode === "dark" ? colors.grey[700]  : colors.grey[300];
  const tickColor    = theme.palette.mode === "dark" ? colors.grey[300]  : colors.grey[700];
  const legendColor  = theme.palette.mode === "dark" ? colors.grey[100]  : colors.grey[700];
  const tooltipBg    = colors.primary[400];
  const tooltipText  = theme.palette.mode === "dark" ? colors.grey[100]  : colors.grey[900];
  const tooltipBorder= theme.palette.mode === "dark" ? colors.grey[700]  : colors.grey[300];

  const debtInFill   = colors.blueAccent[400];
  const debtOutFill  = colors.redAccent[400];
  const ownerInFill  = colors.greenAccent[500];
  const ownerOutFill = colors.redAccent[300];

  // ── Program (coordinator scope) ──────────────────────────────────────────────
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

  // ── Availability (years/quarters) — derive from all rows for the scope ──────
  const [allRows, setAllRows] = useState([]);
  const [availLoading, setAvailLoading] = useState(true);

  // key to refetch when scope changes
  const scopeKey = JSON.stringify({ isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    if (isCoordinator && programLoading) return; // wait for program id
    (async () => {
      setAvailLoading(true);
      try {
        const params = {};
        if (isCoordinator && programId) params.program_id = programId;
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;
        // fetch all months for the current scope (no from/to) → build availability
        const { data } = await axiosClient.get("/api/finance/monthly-capital-flows", { params });
        if (!alive) return;
        setAllRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (!alive) return;
        console.error("load capital-flow availability:", e?.response?.data || e.message);
        setAllRows([]);
      } finally {
        if (alive) setAvailLoading(false);
      }
    })();
    return () => { alive = false; };
  }, [scopeKey, programLoading]);

  // derive available years/quarters from allRows
  const availableYears = useMemo(() => {
    const ys = new Set((allRows || []).map(r => new Date(r.month).getFullYear()));
    return Array.from(ys).sort((a, b) => a - b);
  }, [allRows]);

  const quartersForYear = useMemo(() => {
    const map = new Map(); // year -> Set(Q1..Q4)
    for (const r of allRows || []) {
      const d = new Date(r.month);
      if (isNaN(d)) continue;
      const y = d.getFullYear();
      const m = d.getMonth() + 1;
      const q = m <= 3 ? "Q1" : m <= 6 ? "Q2" : m <= 9 ? "Q3" : "Q4";
      if (!map.has(y)) map.set(y, new Set());
      map.get(y).add(q);
    }
    return map;
  }, [allRows]);

  const quartersOf = (y) => Array.from(quartersForYear.get(y) ?? []).sort((a,b)=>Number(a.slice(1))-Number(b.slice(1)));

  // ── Period controls with sticky defaults + conditional "Clear Filter" ───────
  const canUseLocalFilters = !from && !to; // if parent passed a custom range, lock the controls
  const now = new Date();
  const fallbackYear = now.getFullYear();
  const latestYearWithData = availableYears[availableYears.length - 1] ?? fallbackYear;

  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedYear, setSelectedYear] = useState(latestYearWithData);
  const [selectedQuarter, setSelectedQuarter] = useState(quartersOf(latestYearWithData)[0] || "Q1");

  // keep selection valid when availability changes
  useEffect(() => {
    if (!availableYears.length) return;
    if (!availableYears.includes(selectedYear)) {
      setSelectedYear(latestYearWithData);
    }
    if (periodMode === "quarterly") {
      const qs = quartersOf(availableYears.includes(selectedYear) ? selectedYear : latestYearWithData);
      if (qs.length && !qs.includes(selectedQuarter)) setSelectedQuarter(qs[0]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availableYears.join(","), periodMode]);

  // defaults snapshot tied to current scope
  const defaultsRef = useRef(null);
  useEffect(() => {
    defaultsRef.current = {
      periodMode: "overall",
      selectedYear: latestYearWithData,
      selectedQuarter: quartersOf(latestYearWithData)[0] || "Q1",
    };
    setPeriodMode("overall");
    setSelectedYear(latestYearWithData);
    setSelectedQuarter(quartersOf(latestYearWithData)[0] || "Q1");
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
  }, [periodMode, selectedYear, selectedQuarter, canUseLocalFilters]);

  const handleClear = () => {
    if (!defaultsRef.current) return;
    const d = defaultsRef.current;
    setPeriodMode(d.periodMode);
    setSelectedYear(d.selectedYear);
    setSelectedQuarter(d.selectedQuarter);
  };

  // effective range
  const { effFrom, effTo, label } = useMemo(() => {
    if (!canUseLocalFilters) {
      return { effFrom: from ?? null, effTo: to ?? null, label: "Custom" };
    }
    if (periodMode === "overall") return { effFrom: null, effTo: null, label: "Overall" };
    if (periodMode === "yearly") {
      const y = selectedYear;
      return { effFrom: `${y}-01-01`, effTo: `${y + 1}-01-01`, label: `Year ${y}` };
    }
    const r = quarterRange(selectedYear, selectedQuarter);
    return { effFrom: r.from, effTo: r.to, label: `${selectedYear} ${selectedQuarter}` };
  }, [canUseLocalFilters, from, to, periodMode, selectedYear, selectedQuarter]);

  // ── Filter rows for the selected window (client-side; no extra endpoint) ─────
  const filteredRows = useMemo(() => {
    if (!allRows?.length) return [];
    if (!effFrom && !effTo) return allRows.slice().sort((a,b)=>new Date(a.month)-new Date(b.month));
    const fromD = effFrom ? new Date(effFrom) : null;
    const toD   = effTo   ? new Date(effTo)   : null;
    return allRows
      .filter(r => {
        const d = new Date(r.month);
        if (fromD && d < fromD) return false;
        if (toD   && d >= toD)  return false; // end-exclusive
        return true;
      })
      .sort((a,b)=>new Date(a.month)-new Date(b.month));
  }, [allRows, effFrom, effTo]);

  // recharts data shape
  const data = filteredRows.map(r => ({
    x: String(r.month).slice(0, 7),
    debt_in: Number(r.debt_in || 0),
    debt_out: Number(r.debt_out || 0),
    owner_in: Number(r.owner_capital_in || 0),
    owner_out: Number(r.owner_withdrawal || 0),
  }));

  const yearOptions = availableYears.length ? availableYears : [selectedYear];
  const quarterOptions = periodMode === "quarterly" ? quartersOf(selectedYear) : [];

  const controlsDisabled = availLoading || (isCoordinator && programLoading) || !canUseLocalFilters;

  // NEW: loading & empty flags for the chart area
  const isLoading = availLoading || (isCoordinator && programLoading);
  const isEmpty   = !isLoading && data.length === 0;

  return (
    <Box>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={1.5}>
        <Box display="flex" alignItems="center" gap={1.25}>
          <Typography variant="h3" color={colors.greenAccent[500]} fontWeight="bold">
            Capital Flows
          </Typography>
          <MuiTooltip
            arrow
            placement="top"
            title={
              <Box sx={{ maxWidth: 420, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">How to read • Capital Flows</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Stacked bars show <b>Debt</b> (In/Out) and <b>Owner</b> (Capital In/Withdrawals) per month.
                </Typography>
                <Typography variant="body2" sx={{ mt: 0.5 }}>
                  The window resets when you switch <b>Overall / Quarter / Year</b>. {label && `• ${label}`}
                </Typography>
              </Box>
            }
          >
            <IconButton size="small" sx={{ color: colors.grey[300] }}>
              <HelpOutlineIcon fontSize="small" />
            </IconButton>
          </MuiTooltip>
        </Box>

        {/* Period controls */}
        <Box display="flex" alignItems="center" gap={1}>
          <Select
            value={periodMode}
            onChange={(e) => setPeriodMode(e.target.value)}
            disabled={controlsDisabled}
            sx={{
              height: 40, minWidth: 130,
              backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
              color: colors.grey[100], fontWeight: "bold",
              "& .MuiSelect-icon": { color: colors.grey[100] },
              "& fieldset": { border: "none" },
            }}
          >
            <MenuItem value="overall">Overall</MenuItem>
            <MenuItem value="quarterly">Quarterly</MenuItem>
            <MenuItem value="yearly">Yearly</MenuItem>
          </Select>

          {periodMode === "quarterly" && canUseLocalFilters && (
            <Select
              value={quarterOptions.length ? selectedQuarter : ""}
              onChange={(e) => setSelectedQuarter(e.target.value)}
              disabled={controlsDisabled || quarterOptions.length === 0}
              displayEmpty
              sx={{
                height: 40, minWidth: 100,
                backgroundColor: controlsDisabled || quarterOptions.length === 0 ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {quarterOptions.length === 0 ? (
                <MenuItem value="">No quarters</MenuItem>
              ) : quarterOptions.map(q => <MenuItem key={q} value={q}>{q}</MenuItem>)}
            </Select>
          )}

          {(periodMode === "quarterly" || periodMode === "yearly") && canUseLocalFilters && (
            <Select
              value={yearOptions.includes(selectedYear) ? selectedYear : (yearOptions[0] ?? selectedYear)}
              onChange={(e) => setSelectedYear(e.target.value)}
              disabled={controlsDisabled || yearOptions.length === 0}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: controlsDisabled || yearOptions.length === 0 ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {yearOptions.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
            </Select>
          )}

          {isDirty && canUseLocalFilters && (
            <Button
              onClick={handleClear}
              variant="outlined"
              sx={{
                height: 40, minWidth: 120,
                borderColor: colors.grey[100],
                color: colors.grey[100],
                backgroundColor: colors.blueAccent[600],
                fontWeight: "bold",
                "&:hover": { backgroundColor: colors.blueAccent[700] },
              }}
            >
              Clear Filter
            </Button>
          )}
        </Box>
      </Box>

      {/* Chart with loading/empty states */}
      <div style={{ height: 420, display: "flex", flexDirection: "column", alignItems: "center" }}>
        <div style={{ height: 360, width: "100%" }}>
          {isLoading ? (
            <Box sx={{ color: colors.grey[300], display:"flex", alignItems:"center", justifyContent:"center", height:"100%" }}>
              <Typography>Loading…</Typography>
            </Box>
          ) : isEmpty ? (
            <Box sx={{ color: colors.grey[300], display:"flex", alignItems:"center", justifyContent:"center", height:"100%" }}>
              <Typography>No data to display.</Typography>
            </Box>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
                <CartesianGrid stroke={strokeGrid} strokeDasharray="3 3" />
                <XAxis
                  dataKey="x"
                  tick={{ fill: tickColor, fontSize: 12 }}
                  axisLine={{ stroke: strokeGrid }}
                  tickLine={{ stroke: strokeGrid }}
                />
                <YAxis
                  tick={{ fill: tickColor, fontSize: 12 }}
                  axisLine={{ stroke: strokeGrid }}
                  tickLine={{ stroke: strokeGrid }}
                  tickFormatter={peso}
                />
                <Tooltip
                  formatter={(v, n) => [peso(v), n]}
                  contentStyle={{ backgroundColor: tooltipBg, border: `1px solid ${tooltipBorder}`, borderRadius: 8, color: tooltipText }}
                  labelStyle={{ color: tooltipText }}
                />
                <Legend verticalAlign="bottom" wrapperStyle={{ color: legendColor, paddingBottom: 8 }} />
                {/* Two stacks: Debt (in/out) and Owner (in/out) */}
                <Bar dataKey="debt_in"   name="Debt In"           stackId="debt"  fill={debtInFill}   barSize={14} radius={[4,4,0,0]} />
                <Bar dataKey="debt_out"  name="Debt Out"          stackId="debt"  fill={debtOutFill}  barSize={14} radius={[4,4,0,0]} />
                <Bar dataKey="owner_in"  name="Owner Capital In"  stackId="owner" fill={ownerInFill}  barSize={14} radius={[4,4,0,0]} />
                <Bar dataKey="owner_out" name="Owner Withdrawals" stackId="owner" fill={ownerOutFill} barSize={14} radius={[4,4,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </Box>
  );
}