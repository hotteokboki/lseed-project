import { ResponsiveHeatMap } from "@nivo/heatmap";
import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Box, Select, MenuItem, Typography, Tooltip as MuiTooltip,
  IconButton, Button, useTheme
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { tokens } from "../theme";
import axiosClient from "../api/axiosClient";
import { useAuth } from "../context/authContext";

/** Quarter ranges — Q1: Jan–Mar, Q2: Apr–Jun, Q3: Jul–Sep, Q4: Oct–Dec */
const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default:   return { from: null, to: null };
  }
};

const SEsPerPage = 10;

const FinanceRiskHeatmap = ({ selectedSEId = null }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

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

  // ── Availability: dynamic years/quarters from actual months for scope ───────
  const [availableMonths, setAvailableMonths] = useState([]); // ["YYYY-MM", ...]
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

        // No from/to -> server returns { months, rows } for the whole scope
        const { data } = await axiosClient.get("/api/finance-risk-heatmap", { params });
        if (!alive) return;
        const months = Array.isArray(data?.months) ? data.months : [];
        setAvailableMonths(months);
      } catch (e) {
        if (!alive) return;
        console.error("load risk availability:", e?.response?.data || e.message);
        setAvailableMonths([]);
      } finally {
        if (alive) setAvailLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [scopeKey, programLoading, isCoordinator, programId, selectedSEId]);

  // Build dynamic year/quarter options
  const availableYears = useMemo(() => {
    const ys = new Set(availableMonths.map(m => Number(m.slice(0, 4))));
    return Array.from(ys).sort((a, b) => a - b);
  }, [availableMonths]);

  const quartersByYear = useMemo(() => {
    const map = new Map();
    for (const ym of availableMonths) {
      const y = Number(ym.slice(0, 4));
      const m = Number(ym.slice(5, 7));
      const q = m <= 3 ? "Q1" : m <= 6 ? "Q2" : m <= 9 ? "Q3" : "Q4";
      if (!map.has(y)) map.set(y, new Set());
      map.get(y).add(q);
    }
    return map;
  }, [availableMonths]);
  const quarterOptionsFor = (y) => Array.from(quartersByYear.get(y) ?? []).sort();

  // ── Period controls + defaults (with conditional Clear Filter) ──────────────
  const now = new Date();
  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedYear, setSelectedYear] = useState(now.getFullYear());
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");

  // Keep year selection valid as availability loads
  useEffect(() => {
    if (!availableYears.length) return;
    if (!availableYears.includes(selectedYear)) {
      setSelectedYear(availableYears[availableYears.length - 1]); // latest year in data
    }
  }, [availableYears, selectedYear]);

  // Keep quarter valid when year/availability changes
  useEffect(() => {
    if (periodMode !== "quarterly") return;
    const qs = quarterOptionsFor(selectedYear);
    if (qs.length && !qs.includes(selectedQuarter)) setSelectedQuarter(qs[0]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [periodMode, selectedYear, quartersByYear]);

  // Defaults snapshot per-scope (for Clear Filter)
  const defaultsRef = useRef(null);
  useEffect(() => {
    if (availLoading) return;
    const latestYear = availableYears[availableYears.length - 1] ?? now.getFullYear();
    const firstQuarter = quarterOptionsFor(latestYear)[0] ?? "Q1";
    defaultsRef.current = { periodMode: "overall", selectedYear: latestYear, selectedQuarter: firstQuarter };
    setSelectedYear(latestYear);
    setSelectedQuarter(firstQuarter);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scopeKey, availLoading]);

  const isDirty = useMemo(() => {
    if (!defaultsRef.current) return false;
    const d = defaultsRef.current;
    return (
      periodMode !== d.periodMode ||
      selectedYear !== d.selectedYear ||
      selectedQuarter !== d.selectedQuarter
    );
  }, [periodMode, selectedYear, selectedQuarter]);

  const handleClearFilters = () => {
    if (!defaultsRef.current) return;
    const d = defaultsRef.current;
    setPeriodMode(d.periodMode);
    setSelectedYear(d.selectedYear);
    setSelectedQuarter(d.selectedQuarter);
  };

  // Effective from/to
  const { effFrom, effTo, periodLabel } = useMemo(() => {
    if (periodMode === "overall") return { effFrom: null, effTo: null, periodLabel: "Overall" };
    if (periodMode === "yearly") {
      const y = selectedYear;
      return { effFrom: `${y}-01-01`, effTo: `${Number(y) + 1}-01-01`, periodLabel: `Year ${y}` };
    }
    const r = quarterRange(selectedYear, selectedQuarter);
    return { effFrom: r.from, effTo: r.to, periodLabel: `${selectedYear} ${selectedQuarter}` };
  }, [periodMode, selectedYear, selectedQuarter]);

  // ── Data fetch (scores) ─────────────────────────────────────────────────────
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

        const { data } = await axiosClient.get("/api/finance-risk-heatmap", { params });
        if (!alive) return;
        const rowsData = Array.isArray(data?.rows) ? data.rows : (Array.isArray(data) ? data : []);
        setRows(rowsData);
      } catch (e) {
        if (!alive) return;
        console.error("load finance heatmap:", e?.response?.data || e.message);
        setErrMsg("Failed to load risk heatmap.");
        setRows([]);
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [fetchKey, programLoading, isCoordinator, programId, selectedSEId]);

  // ── Local SE filter + pagination ────────────────────────────────────────────
  const [selectedSE, setSelectedSE] = useState("");
  const [page, setPage] = useState(0);

  useEffect(() => { setPage(0); }, [selectedSE, rows]);

  const uniqueSEs = useMemo(
    () => Array.from(new Set(rows.map(r => r.team_name))).sort(),
    [rows]
  );

  const filtered = selectedSE ? rows.filter(r => r.team_name === selectedSE) : rows;

  const paged = useMemo(() => {
    const start = page * SEsPerPage;
    return filtered.slice(start, start + SEsPerPage);
  }, [filtered, page]);

  // Transform for Nivo
  const data = useMemo(() => {
    return paged.map(r => ({
      id: r.abbr || r.team_name,
      team_name: r.team_name,
      data: [
        { x: "Cash Margin",         y: Number(r["Cash Margin"]         || 0), team_name: r.team_name },
        { x: "In/Out Ratio",        y: Number(r["In/Out Ratio"]        || 0), team_name: r.team_name },
        { x: "Inventory Turnover",  y: Number(r["Inventory Turnover"]  || 0), team_name: r.team_name },
        { x: "Reporting",           y: Number(r["Reporting"]           || 0), team_name: r.team_name },
      ],
    }));
  }, [paged]);

  const cellColor = (val) => {
    if (val <= 1.5) return theme.palette.mode === "dark" ? colors.redAccent[300] : colors.redAccent[500];
    if (val <= 3.0) return theme.palette.mode === "dark" ? colors.primary[300]  : colors.grey[700];
    return theme.palette.mode === "dark" ? colors.greenAccent[300] : colors.greenAccent[500];
  };

  const controlsDisabled =
    loading || availLoading || (isCoordinator && programLoading);

  return (
    <Box>
      {/* Header & controls */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
            Financial Risk Heatmap
          </Typography>
          <MuiTooltip
            arrow placement="top"
            title={
              <Box sx={{ maxWidth: 360, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">What this shows</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Scores (1–5) derived from cash flows, inventory turnover, and reporting completeness.
                  Lower scores suggest an SE may need mentoring.
                </Typography>
                <ul style={{ margin: 0, paddingLeft: 18 }}>
                  <li><b>Cash Margin</b>: (Inflow − Outflow)/Inflow</li>
                  <li><b>In/Out Ratio</b>: Inflow/Outflow</li>
                  <li><b>Inventory Turnover</b>: COGS / Avg Inventory (monthly)</li>
                  <li><b>Reporting</b>: Share of months with cash-in, cash-out, and inventory reports</li>
                </ul>
              </Box>
            }
          >
            <IconButton sx={{ ml: 1, color: colors.grey[300] }}>
              <HelpOutlineIcon />
            </IconButton>
          </MuiTooltip>
          {errMsg && <Typography variant="caption" color="#f44336" sx={{ ml: 2 }}>{errMsg}</Typography>}
        </Box>

        <Box display="flex" alignItems="center" gap={1}>
          <Select
            value={periodMode}
            onChange={(e)=>setPeriodMode(e.target.value)}
            disabled={controlsDisabled}
            sx={{
              height: 40, minWidth: 130,
              backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
              color: colors.grey[100], fontWeight:"bold",
              "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
            }}>
            <MenuItem value="overall">Overall</MenuItem>
            <MenuItem value="quarterly">Quarterly</MenuItem>
            <MenuItem value="yearly">Yearly</MenuItem>
          </Select>

          {periodMode === "quarterly" && (
            <Select
              value={selectedQuarter}
              onChange={(e)=>setSelectedQuarter(e.target.value)}
              disabled={controlsDisabled || quarterOptionsFor(selectedYear).length === 0}
              sx={{
                height: 40, minWidth: 100,
                backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight:"bold",
                "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
              }}>
              {quarterOptionsFor(selectedYear).map(q => <MenuItem key={q} value={q}>{q}</MenuItem>)}
            </Select>
          )}

          {(periodMode === "quarterly" || periodMode === "yearly") && (
            <Select
              value={selectedYear}
              onChange={(e)=>setSelectedYear(Number(e.target.value))}
              disabled={controlsDisabled || availableYears.length === 0}
              sx={{
                height:40, minWidth:110,
                backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight:"bold",
                "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
              }}>
              {availableYears.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
            </Select>
          )}

          {isDirty && (
            <Button
              onClick={handleClearFilters}
              variant="outlined"
              sx={{
                height: 40, minWidth: 120,
                borderColor: colors.grey[100], color: colors.grey[100],
                backgroundColor: colors.blueAccent[600], fontWeight: "bold",
                "&:hover": { backgroundColor: colors.blueAccent[700] },
              }}
            >
              CLEAR FILTER
            </Button>
          )}

          {/* SE dropdown (local filter) */}
          <Select
            value={selectedSE}
            onChange={(e)=>{ setSelectedSE(e.target.value); setPage(0); }}
            displayEmpty
            disabled={loading}
            sx={{
              height: 40, minWidth: 220,
              backgroundColor: loading ? colors.grey[600] : colors.blueAccent[600],
              color: colors.grey[100], fontWeight:"bold",
              "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
            }}
          >
            <MenuItem value="">All SEs</MenuItem>
            {Array.from(new Set(rows.map(r => r.team_name))).sort().map(se => (
              <MenuItem key={se} value={se}>{se}</MenuItem>
            ))}
          </Select>
        </Box>
      </Box>

      {/* Heatmap + pager */}
      <div style={{ height: 540, display:"flex", flexDirection:"column", alignItems:"center" }}>
        <div style={{ height: 480, width: "100%" }}>
          {loading ? (
            <Box sx={{ color: colors.grey[300], display:"flex", alignItems:"center", justifyContent:"center", height:"100%" }}>
              <Typography>Loading…</Typography>
            </Box>
          ) : rows.length === 0 ? (
            <Box sx={{ color: colors.grey[300], display:"flex", alignItems:"center", justifyContent:"center", height:"100%" }}>
              <Typography>No data to display.</Typography>
            </Box>
          ) : (
            <ResponsiveHeatMap
              data={data}
              valueFormat=">-.2f"
              margin={{ top: 60, right: 40, bottom: 70, left: 180 }}
              axisTop={{
                tickSize: 5, tickPadding: 5,
                legend: `Financial Indicators • ${periodLabel}`, legendOffset: -50, legendPosition: "middle",
                tickRotation: 0, truncateTickAt: 0,
              }}
              axisLeft={{
                tickSize: 5, tickPadding: 5,
                legend: "Social Enterprise", legendPosition: "middle", legendOffset: -170,
                truncateTickAt: 0,
              }}
              colors={({ value }) => cellColor(value)}
              emptyColor={colors.grey[600]}
              tooltip={({ cell }) => (
                <div style={{
                  background: theme.palette.mode === "dark" ? colors.primary[500] : "#fff",
                  color: theme.palette.mode === "dark" ? colors.grey[100] : "#222",
                  padding: 10, borderRadius: 6, boxShadow: "0 2px 6px rgba(0,0,0,0.2)"
                }}>
                  <strong>{cell.data.team_name}</strong><br/>
                  {cell.data.x}: {Number(cell.data.y).toFixed(2)} / 5
                </div>
              )}
              theme={{
                axis: {
                  ticks: { text: { fill: colors.grey[100] } },
                  legend:{ text: { fill: colors.grey[100] } },
                },
                legends: { text: { fill: colors.grey[100] } },
              }}
            />
          )}
        </div>

        {/* Pager or Clear SE filter */}
        <div className="flex items-center mt-3">
          {!selectedSE ? (
            <>
              <Button
                variant="contained"
                disabled={page === 0}
                onClick={()=>setPage(p=>Math.max(0,p-1))}
                sx={{ mx:2, backgroundColor: colors.blueAccent[600], color: colors.grey[100],
                  "&:disabled": { backgroundColor: colors.grey[600], color: colors.grey[300] } }}
              >◀ Prev</Button>
              <Button
                variant="contained"
                disabled={(page+1)*SEsPerPage >= filtered.length}
                onClick={()=>setPage(p=>p+1)}
                sx={{ mx:2, backgroundColor: colors.blueAccent[600], color: colors.grey[100],
                  "&:disabled": { backgroundColor: colors.grey[600], color: colors.grey[300] } }}
              >Next ▶</Button>
            </>
          ) : (
            <Button
              variant="outlined"
              onClick={()=>{ setSelectedSE(""); setPage(0); }}
              sx={{ backgroundColor: colors.blueAccent[600], color: colors.grey[100],
                borderColor: colors.grey[100], "&:hover": { backgroundColor: colors.blueAccent[700] } }}
            >Clear SE</Button>
          )}
        </div>

        {/* Color legend */}
        <div style={{ display:"flex", alignItems:"center", gap:12, marginTop:10 }}>
          <div style={{ display:"flex", alignItems:"center", gap:6 }}>
            <div style={{ width:18, height:18, borderRadius:4, background: cellColor(1.0) }} />
            <span style={{ color: colors.grey[100], fontSize: 14 }}>Needs Attention (≤ 1.5)</span>
          </div>
          <div style={{ display:"flex", alignItems:"center", gap:6 }}>
            <div style={{ width:18, height:18, borderRadius:4, background: cellColor(2.5) }} />
            <span style={{ color: colors.grey[100], fontSize: 14 }}>Moderate (1.5 – 3)</span>
          </div>
          <div style={{ display:"flex", alignItems:"center", gap:6 }}>
            <div style={{ width:18, height:18, borderRadius:4, background: cellColor(4.2) }} />
            <span style={{ color: colors.grey[100], fontSize: 14 }}>Healthy (&gt; 3)</span>
          </div>
        </div>
      </div>
    </Box>
  );
};

export default FinanceRiskHeatmap;