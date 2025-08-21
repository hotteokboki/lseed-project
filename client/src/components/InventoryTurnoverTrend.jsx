import { useEffect, useMemo, useRef, useState } from "react";
import { ResponsiveLine } from "@nivo/line";
import {
  Box, Typography, Select, MenuItem, Tooltip, IconButton, CircularProgress, Button
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { useTheme } from "@mui/material";
import { tokens } from "../theme";
import axiosClient from "../api/axiosClient";
import { useAuth } from "../context/authContext";

const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default:   return { from: null, to: null };
  }
};

const InventoryTurnoverTrend = ({ selectedSEId = null }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

  // legend colors
  const LEGEND_COLORS = {
    poor: theme.palette.mode === "dark" ? colors.redAccent[300] : colors.redAccent[500],
    moderate: theme.palette.mode === "dark" ? colors.primary[300] : colors.grey[700],
    good: theme.palette.mode === "dark" ? colors.greenAccent[300] : colors.greenAccent[500],
  };

  // ── Coordinator program_id (once) ────────────────────────────────────────────
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

  // ── Load ALL rows for scope (to derive dynamic years/quarters & filter locally) ─
  const [allRows, setAllRows] = useState([]);
  const [baseLoading, setBaseLoading] = useState(true);
  const [errMsg, setErrMsg] = useState("");

  const scopeKey = JSON.stringify({ isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    setBaseLoading(true);
    setErrMsg("");
    setAllRows([]);

    if (isCoordinator && programLoading) return; // wait for program id resolution

    (async () => {
      try {
        const params = {};
        if (isCoordinator && programId) params.program_id = programId;
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;

        // fetch ALL months for the current scope (no from/to)
        const { data } = await axiosClient.get("/api/get-overall-inventory-turnover", { params });
        if (!alive) return;
        setAllRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (!alive) return;
        console.error("load inv turnover (base):", e?.response?.data || e.message);
        setErrMsg("Failed to load inventory turnover.");
        setAllRows([]);
      } finally {
        if (alive) setBaseLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [scopeKey, programLoading, isCoordinator, programId, selectedSEId]);

  // ── Derive dynamic availability from allRows ─────────────────────────────────
  const availableMonths = useMemo(
    () => allRows.map(r => String(r.month).slice(0, 7)).sort(),
    [allRows]
  );

  const availableYears = useMemo(() => {
    const ys = new Set(availableMonths.map(m => Number(m.slice(0, 4))));
    return Array.from(ys).sort((a, b) => a - b);
  }, [availableMonths]);

  const quartersByYear = useMemo(() => {
    const map = new Map(); // y -> Set('Q1'..'Q4')
    for (const ym of availableMonths) {
      const y = Number(ym.slice(0, 4));
      const m = Number(ym.slice(5, 7));
      const q = m <= 3 ? "Q1" : m <= 6 ? "Q2" : m <= 9 ? "Q3" : "Q4";
      if (!map.has(y)) map.set(y, new Set());
      map.get(y).add(q);
    }
    return map;
  }, [availableMonths]);

  const quartersOf = (y) => Array.from(quartersByYear.get(y) ?? []).sort();

  // ── Period controls + defaults snapshot ──────────────────────────────────────
  const latestYear = availableYears[availableYears.length - 1] ?? new Date().getFullYear();

  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedYear, setSelectedYear] = useState(latestYear);
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");

  // Defaults snapshot (per scope) — used by Clear Filter
  const defaultsRef = useRef(null);
  useEffect(() => {
    // reset snapshot whenever scope or availability changes
    defaultsRef.current = {
      periodMode: "overall",
      selectedYear: latestYear,
      selectedQuarter: quartersOf(latestYear)[0] ?? "Q1",
    };
    // also normalize current selection to valid values
    setSelectedYear(latestYear);
    const qs = quartersOf(latestYear);
    if (qs.length) setSelectedQuarter(qs[0]);
  }, [latestYear, JSON.stringify(Array.from(quartersByYear.entries()))]);

  // Keep current selection valid when options change
  useEffect(() => {
    if (periodMode === "yearly" || periodMode === "quarterly") {
      if (availableYears.length && !availableYears.includes(selectedYear)) {
        setSelectedYear(latestYear);
      }
    }
  }, [periodMode, availableYears, selectedYear, latestYear]);

  useEffect(() => {
    if (periodMode === "quarterly") {
      const qs = quartersOf(selectedYear);
      if (qs.length && !qs.includes(selectedQuarter)) {
        setSelectedQuarter(qs[0]);
      }
    }
  }, [periodMode, selectedYear, selectedQuarter, quartersByYear]);

  const isDirty = useMemo(() => {
    const d = defaultsRef.current;
    if (!d) return false;
    return (
      periodMode !== d.periodMode ||
      selectedYear !== d.selectedYear ||
      selectedQuarter !== d.selectedQuarter
    );
  }, [periodMode, selectedYear, selectedQuarter]);

  const handleClear = () => {
    const d = defaultsRef.current;
    if (!d) return;
    setPeriodMode(d.periodMode);
    setSelectedYear(d.selectedYear);
    setSelectedQuarter(d.selectedQuarter);
  };

  // Compute from/to from current period selection
  const { from, to, label } = useMemo(() => {
    if (periodMode === "overall") return { from: null, to: null, label: "Overall" };
    if (periodMode === "yearly")
      return { from: `${selectedYear}-01-01`, to: `${selectedYear + 1}-01-01`, label: `Year ${selectedYear}` };
    const r = quarterRange(selectedYear, selectedQuarter);
    return { ...r, label: `${selectedYear} ${selectedQuarter}` };
  }, [periodMode, selectedYear, selectedQuarter]);

  // Filter allRows locally by from/to
  const rows = useMemo(() => {
    if (!from && !to) return allRows;
    const f = from ? new Date(from) : null;
    const t = to ? new Date(to) : null;
    return allRows.filter(r => {
      const d = new Date(r.month);
      if (f && d < f) return false;
      if (t && d >= t) return false; // end-exclusive
      return true;
    });
  }, [allRows, from, to]);

  // Build series
  const series = useMemo(() => {
    const pts = rows.map(r => ({
      x: String(r.month).slice(0, 7),                  // YYYY-MM
      y: r.turnover === null ? null : Number(r.turnover)
    }));
    const data = [{ id: "Inventory Turnover", data: pts }];
    const months = Array.from(new Set(pts.map(p => p.x))).sort();
    return { data, months };
  }, [rows]);

  const controlsDisabled =
    baseLoading || (isCoordinator && programLoading) || availableYears.length === 0;

  // Current valid option arrays
  const yearOptions = availableYears.length ? availableYears : [latestYear];
  const quarterOptions = periodMode === "quarterly" ? quartersOf(selectedYear) : [];

  // Guard values to avoid MUI out-of-range warnings
  const safeYearValue =
    (periodMode !== "overall" && yearOptions.includes(selectedYear)) ? selectedYear : "";
  const safeQuarterValue =
    (periodMode === "quarterly" && quarterOptions.includes(selectedQuarter)) ? selectedQuarter : "";

  return (
    <Box backgroundColor={colors.primary[400]} p="20px">
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Box>
            <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
              Inventory Turnover Trend
            </Typography>
            {!baseLoading && (
              <Typography variant="h6" color={colors.grey[300]} mt={0.5}>
                {label} • Turnover = COGS / Avg Inventory • Tooltip shows COGS, Avg Inv & DIO
              </Typography>
            )}
            {errMsg && (
              <Typography variant="caption" color="#f44336" display="block" mt={0.5}>
                {errMsg}
              </Typography>
            )}
          </Box>
          <Tooltip
            arrow placement="top"
            title={
              <Box sx={{ maxWidth: 360, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">About Inventory Turnover</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Higher turnover means faster inventory movement (good). Lower turnover may indicate overstock or slow sales.
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  DIO (Days Inventory Outstanding) ≈ days in month ÷ turnover.
                </Typography>
              </Box>
            }
          >
            <IconButton sx={{ ml: 1, color: colors.grey[300] }}>
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Box>

        {/* Period controls + Clear Filter (only when dirty) */}
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

          {periodMode === "quarterly" && (
            <Select
              value={safeQuarterValue}
              onChange={(e) => setSelectedQuarter(e.target.value)}
              disabled={controlsDisabled || quarterOptions.length === 0}
              displayEmpty
              renderValue={(v) => v || "Quarter"}
              sx={{
                height: 40, minWidth: 100,
                backgroundColor: (controlsDisabled || quarterOptions.length === 0)
                  ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {quarterOptions.map(q => <MenuItem key={q} value={q}>{q}</MenuItem>)}
            </Select>
          )}

          {(periodMode === "quarterly" || periodMode === "yearly") && (
            <Select
              value={safeYearValue}
              onChange={(e) => setSelectedYear(e.target.value)}
              disabled={controlsDisabled || yearOptions.length === 0}
              displayEmpty
              renderValue={(v) => v || "Year"}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: (controlsDisabled || yearOptions.length === 0)
                  ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {yearOptions.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
            </Select>
          )}

          {isDirty && (
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

      {/* Chart */}
      <Box height="360px" display="flex" alignItems="center" justifyContent="center">
        {baseLoading ? (
          <CircularProgress sx={{ color: colors.greenAccent[500] }} />
        ) : series.data[0].data.length === 0 ? (
          <Typography color={colors.grey[300]}>No data to display.</Typography>
        ) : (
          <ResponsiveLine
            data={series.data}
            theme={{
              axis: {
                domain: { line: { stroke: colors.grey[100] } },
                ticks: { line: { stroke: colors.grey[100] }, text: { fill: colors.grey[100] } },
                legend: { text: { fill: colors.grey[100] } },
              },
              legends: { text: { fill: colors.grey[100] } },
              tooltip: { container: { color: colors.primary[500] } },
            }}
            colors={{ scheme: "category10" }}
            margin={{ top: 40, right: 30, bottom: 50, left: 70 }}
            xScale={{ type: "point", domain: series.months }}
            yScale={{ type: "linear", min: 0, max: "auto", stacked: false }}
            axisBottom={{
              tickSize: 0, tickPadding: 8, tickRotation: 0,
              legend: "Month", legendPosition: "middle", legendOffset: 36,
              format: (v) => v,
            }}
            axisLeft={{
              tickSize: 3, tickPadding: 5,
              legend: "Turnover (x per month)", legendPosition: "middle", legendOffset: -55,
            }}
            pointSize={7}
            pointColor={{ theme: "background" }}
            pointBorderWidth={2}
            pointBorderColor={{ from: "serieColor" }}
            useMesh
            tooltip={({ point }) => {
              const row = rows.find(r => String(r.month).slice(0,7) === point.data.x);
              const d = new Date(`${point.data.x}-01`);
              const title = isNaN(d) ? point.data.x : d.toLocaleDateString("en-US", { year: "numeric", month: "long" });
              const cogs = Number(row?.cogs || 0);
              const avgInv = Number(row?.avg_inventory || 0);
              const dio = row?.dio_days != null ? Number(row.dio_days) : null;
              const t = point.data.y != null ? Number(point.data.y) : null;
              return (
                <div style={{ background: "white", padding: 10, borderRadius: 6, boxShadow: "0 2px 6px rgba(0,0,0,0.2)", color: "#222" }}>
                  <strong>{title}</strong><br/>
                  Turnover: <b>{t != null ? t.toFixed(2) : "—"}x</b><br/>
                  COGS: ₱{cogs.toLocaleString("en-US", { minimumFractionDigits: 2 })}<br/>
                  Avg Inventory: ₱{avgInv.toLocaleString("en-US", { minimumFractionDigits: 2 })}<br/>
                  DIO: {dio != null ? `${dio.toLocaleString("en-US", { maximumFractionDigits: 0 })} days` : "—"}
                </div>
              );
            }}
          />
        )}
      </Box>

      {/* Bottom legend (Good / Moderate / Poor) */}
      <Box
        mt={1.5}
        display="flex"
        alignItems="center"
        justifyContent="center"
        gap={3}
        sx={{ flexWrap: "wrap" }}
      >
        <Box display="flex" alignItems="center" gap={1}>
          <Box sx={{ width: 18, height: 18, borderRadius: 1, backgroundColor: LEGEND_COLORS.poor }} />
          <Typography variant="body2" sx={{ color: colors.grey[100] }}>
            <b>Poor / Slow</b> &nbsp; (&lt; 0.25×/mo &nbsp;≈&nbsp; &lt; 3×/yr; &nbsp;DIO &gt; ~120d)
          </Typography>
        </Box>

        <Box display="flex" alignItems="center" gap={1}>
          <Box sx={{ width: 18, height: 18, borderRadius: 1, backgroundColor: LEGEND_COLORS.moderate }} />
          <Typography variant="body2" sx={{ color: colors.grey[100] }}>
            <b>Moderate</b> &nbsp; (0.25–0.50×/mo &nbsp;≈&nbsp; 3–6×/yr; &nbsp;DIO ~60–120d)
          </Typography>
        </Box>

        <Box display="flex" alignItems="center" gap={1}>
          <Box sx={{ width: 18, height: 18, borderRadius: 1, backgroundColor: LEGEND_COLORS.good }} />
          <Typography variant="body2" sx={{ color: colors.grey[100] }}>
            <b>Good / Fast</b> &nbsp; (≥ 0.50×/mo &nbsp;≈&nbsp; ≥ 6×/yr; &nbsp;DIO &lt; ~60d)
          </Typography>
        </Box>
      </Box>
    </Box>
  );
};

export default InventoryTurnoverTrend;