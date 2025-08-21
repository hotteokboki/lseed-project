// CashFlowBarChart.jsx (component)
import { useEffect, useMemo, useRef, useState } from "react";
import { ResponsiveBar } from "@nivo/bar";
import {
  Box, Typography, Select, MenuItem, Button, Tooltip, IconButton
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

const CashFlowBarChart = ({ selectedSEId = null }) => {
  const theme  = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = !!user?.roles?.includes?.("LSEED-Coordinator");

  const SERIES = [
    { key: "inflow",  label: "Inflow",  color: colors.greenAccent[500] },
    { key: "outflow", label: "Outflow", color: colors.blueAccent[500]  },
  ];

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

  // ── Availability derived from SAME endpoint (no from/to) ─────────────────────
  const [availabilityLoading, setAvailabilityLoading] = useState(true);
  const [availYears, setAvailYears] = useState([]);               // e.g., [2023, 2024, 2025] (desc)
  const [availQuartersByYear, setAvailQuartersByYear] = useState({}); // { 2025: ["Q1","Q2","Q4"] }

  const scopeKey = JSON.stringify({ isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    if (isCoordinator && programLoading) return; // wait for program id

    (async () => {
      setAvailabilityLoading(true);
      try {
        const params = {};
        if (isCoordinator && programId) params.program_id = programId;
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;

        // No from/to → get all months in scope
        const resp = await axiosClient.get("/api/get-cash-flow-data", { params });
        if (!alive) return;

        const rows = Array.isArray(resp?.data) ? resp.data : [];
        const months = rows
          .map(r => {
            try { return new Date(r.date).toISOString().slice(0, 7); }
            catch { return String(r?.date ?? "").slice(0, 7); }
          })
          .filter(Boolean);

        const yearsDesc = Array.from(new Set(months.map(ym => Number(ym.slice(0, 4)))))
          .sort((a, b) => b - a);

        const qmap = {};
        for (const ym of months) {
          const y = Number(ym.slice(0, 4));
          const m = Number(ym.slice(5, 7));
          const q = m <= 3 ? "Q1" : m <= 6 ? "Q2" : m <= 9 ? "Q3" : "Q4";
          if (!qmap[y]) qmap[y] = new Set();
          qmap[y].add(q);
        }
        const qbj = {};
        Object.keys(qmap).forEach(y => {
          qbj[y] = Array.from(qmap[y]).sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
        });

        setAvailYears(yearsDesc);
        setAvailQuartersByYear(qbj);
      } catch (e) {
        console.error("availability (cashflow):", e?.response?.data || e.message);
        setAvailYears([]);
        setAvailQuartersByYear({});
      } finally {
        if (alive) setAvailabilityLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [scopeKey, isCoordinator, programLoading, programId, selectedSEId]);

  // ── Period controls with safe defaults & Clear Filter ────────────────────────
  const defaultYear = new Date().getFullYear();
  const latestYear  = availYears[0] ?? defaultYear;

  const [periodMode, setPeriodMode]       = useState("overall"); // overall | quarterly | yearly
  const [selectedYear, setSelectedYear]   = useState(latestYear);
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");

  // keep selection valid when availability changes
  useEffect(() => {
    if (!availYears.length) return;
    if (!availYears.includes(selectedYear)) setSelectedYear(latestYear);

    if (periodMode === "quarterly") {
      const qs = availQuartersByYear[selectedYear] || [];
      if (qs.length && !qs.includes(selectedQuarter)) setSelectedQuarter(qs[0]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availYears.join(","), periodMode, selectedYear, selectedQuarter]);

  // per-scope defaults snapshot
  const defaultsRef = useRef(null);
  useEffect(() => { defaultsRef.current = null; }, [scopeKey]);

  useEffect(() => {
    if (availabilityLoading) return;
    if (!defaultsRef.current) {
      defaultsRef.current = {
        periodMode: "overall",
        selectedYear: latestYear,
        selectedQuarter: (availQuartersByYear[latestYear] || [])[0] ?? "Q1",
      };
      setSelectedYear(latestYear);
      const qs = availQuartersByYear[latestYear] || [];
      if (qs.length) setSelectedQuarter(qs[0]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availabilityLoading, latestYear]);

  const isDirty = useMemo(() => {
    if (!defaultsRef.current) return false;
    const d = defaultsRef.current;
    return (
      periodMode !== d.periodMode ||
      selectedYear !== d.selectedYear ||
      selectedQuarter !== d.selectedQuarter
    );
  }, [periodMode, selectedYear, selectedQuarter]);

  const handleClear = () => {
    if (!defaultsRef.current) return;
    const d = defaultsRef.current;
    setPeriodMode(d.periodMode);
    setSelectedYear(d.selectedYear);
    setSelectedQuarter(d.selectedQuarter);
  };

  // from/to based on controls
  const { from, to, label } = useMemo(() => {
    if (periodMode === "overall") return { from: null, to: null, label: "Overall" };
    if (periodMode === "yearly")
      return { from: `${selectedYear}-01-01`, to: `${Number(selectedYear) + 1}-01-01`, label: `Year ${selectedYear}` };
    const r = quarterRange(selectedYear, selectedQuarter);
    return { ...r, label: `${selectedYear} ${selectedQuarter}` };
  }, [periodMode, selectedYear, selectedQuarter]);

  // ── Data fetch (respects scope + from/to) ────────────────────────────────────
  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState([]);

  const fetchKey = JSON.stringify({ from, to, isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    if (isCoordinator && programLoading) return; // wait program

    (async () => {
      setLoading(true);
      setRows([]);
      try {
        const params = {};
        if (from) params.from = from;
        if (to)   params.to   = to;
        if (isCoordinator && programId) params.program_id = programId;
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;

        const { data } = await axiosClient.get("/api/get-cash-flow-data", { params });
        setRows(alive ? (Array.isArray(data) ? data : []) : []);
      } catch (e) {
        if (alive) {
          console.error("load overall cashflow:", e?.response?.data || e.message);
          setRows([]);
        }
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [fetchKey, isCoordinator, programLoading, programId, selectedSEId]);

  // chart data
  const chartData = useMemo(() => {
    return rows.map(r => ({
      month: r.date,
      inflow: Number(r.inflow || 0),
      outflow: Number(r.outflow || 0),
      net: Number(r.net || 0),
      cash_on_hand: Number(r.cash_on_hand || 0),
    }));
  }, [rows]);

  // derived control state
  const yearOptions    = availYears.length ? availYears : [latestYear];
  const quarterOptions = periodMode === "quarterly" ? (availQuartersByYear[selectedYear] || []) : [];
  const controlsDisabled = loading || availabilityLoading || (isCoordinator && programLoading);

  return (
    <Box backgroundColor={colors.primary[400]} p="20px" pb={8}>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2} flexWrap="wrap" gap={1.5}>
        <Box display="flex" alignItems="center">
          <Box>
            <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
              Cash Flow (Inflow vs Outflow)
            </Typography>
            {!loading && (
              <Typography variant="h6" color={colors.grey[300]} mt={0.5}>
                {label} • Bars: monthly inflow vs outflow • Net & cash-on-hand in tooltips
              </Typography>
            )}
          </Box>
          <Tooltip
            arrow placement="top"
            title={
              <Box sx={{ maxWidth: 420, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">How to read • Cash Flow (In vs Out)</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>• <b>Inflow</b> = Sales + Other Revenue (portfolio total per month).</Typography>
                <Typography variant="body2" sx={{ mt: .5 }}>• <b>Outflow</b> = Cash Out (OPEX + Inventory Purchases + Liability Payments + Owner Withdrawals).</Typography>
                <Typography variant="body2" sx={{ mt: .5 }}>• <b>Net</b> = Inflow − Outflow.</Typography>
                <Typography variant="body2" sx={{ mt: .5 }}>• <b>Cash on hand</b> = running sum of Net from the first visible month.</Typography>
                <Typography variant="body2" sx={{ mt: 1, opacity: .85 }}>• Currency: <b>₱</b> • Timezone: <b>Asia/Manila</b></Typography>
              </Box>
            }
          >
            <IconButton sx={{ ml: 1, color: colors.grey[300] }}>
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Box>

        {/* Period controls with Clear Filter when dirty */}
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

          {periodMode === "yearly" && (
            <Select
              value={selectedYear}
              onChange={(e) => setSelectedYear(e.target.value)}
              disabled={controlsDisabled || yearOptions.length === 0}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {yearOptions.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
            </Select>
          )}

          {periodMode === "quarterly" && (
            <>
              <Select
                value={selectedYear}
                onChange={(e) => setSelectedYear(e.target.value)}
                disabled={controlsDisabled || yearOptions.length === 0}
                sx={{
                  height: 40, minWidth: 110,
                  backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                  color: colors.grey[100], fontWeight: "bold",
                  "& .MuiSelect-icon": { color: colors.grey[100] },
                  "& fieldset": { border: "none" },
                }}
              >
                {yearOptions.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
              </Select>

              <Select
                value={selectedQuarter}
                onChange={(e) => setSelectedQuarter(e.target.value)}
                disabled={controlsDisabled || (availQuartersByYear[selectedYear] || []).length === 0}
                sx={{
                  height: 40, minWidth: 100,
                  backgroundColor: (controlsDisabled || (availQuartersByYear[selectedYear] || []).length === 0)
                    ? colors.grey[600]
                    : colors.blueAccent[600],
                  color: colors.grey[100], fontWeight: "bold",
                  "& .MuiSelect-icon": { color: colors.grey[100] },
                  "& fieldset": { border: "none" },
                }}
              >
                {(availQuartersByYear[selectedYear] || []).map(q => (
                  <MenuItem key={q} value={q}>{q}</MenuItem>
                ))}
              </Select>
            </>
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

      {/* Chart with loading/empty states */}
      <div style={{ height: 420, display: "flex", flexDirection: "column", alignItems: "center" }}>
        <div style={{ height: 360, width: "100%" }}>
          {loading ? (
            <Box sx={{ color: colors.grey[300], display:"flex", alignItems:"center", justifyContent:"center", height:"100%" }}>
              <Typography>Loading…</Typography>
            </Box>
          ) : chartData.length === 0 ? (
            <Box sx={{ color: colors.grey[300], display:"flex", alignItems:"center", justifyContent:"center", height:"100%" }}>
              <Typography>No data to display.</Typography>
            </Box>
          ) : (
            <ResponsiveBar
              data={chartData}
              keys={SERIES.map(s => s.key)}
              indexBy="month"
              groupMode="grouped"
              margin={{ top: 50, right: 30, bottom: 100, left: 80 }}
              padding={0.2}
              minValue={0}
              colors={({ id }) => SERIES.find(s => s.key === id)?.color || "#ccc"}
              valueFormat={v => `₱${Number(v).toLocaleString("en-US", { maximumFractionDigits: 2 })}`}
              axisBottom={{
                tickSize: 0,
                tickPadding: 8,
                tickRotation: 0,
                format: (v) => {
                  try { return new Date(v).toLocaleDateString("en-US", { month: "short", year: "numeric" }); }
                  catch { return String(v ?? "").slice(0, 7); }
                },
              }}
              axisLeft={{
                legend: "Cash Flow (₱)",
                legendPosition: "middle",
                legendOffset: -60,
                format: (value) => Number(value).toLocaleString("en-US"),
              }}
              enableLabel
              labelSkipHeight={12}
              labelTextColor={{ from: "color", modifiers: [["brighter", 2.0]] }}
              tooltip={({ value, indexValue, data }) => {
                const d = new Date(indexValue);
                const title = d.toLocaleDateString("en-US", { year: "numeric", month: "long" });
                const inflow = Number(data.inflow || 0);
                const outflow = Number(data.outflow || 0);
                const net = Number(data.net || inflow - outflow);
                const cash = Number(data.cash_on_hand || 0);
                return (
                  <div style={{ background: "white", padding: 10, borderRadius: 6, boxShadow: "0 2px 6px rgba(0,0,0,0.2)", color: "#222" }}>
                    <strong>{title}</strong><br />
                    Inflow: ₱{inflow.toLocaleString("en-US", { minimumFractionDigits: 2 })}<br />
                    Outflow: ₱{outflow.toLocaleString("en-US", { minimumFractionDigits: 2 })}<br />
                    Net: <b style={{ color: net >= 0 ? "#2e7d32" : "#c62828" }}>
                      ₱{net.toLocaleString("en-US", { minimumFractionDigits: 2 })}
                    </b><br />
                    Cash on hand: ₱{cash.toLocaleString("en-US", { minimumFractionDigits: 2 })}
                  </div>
                );
              }}
              theme={{
                axis: {
                  ticks: { text: { fill: colors.grey[100] } },
                  legend: { text: { fill: colors.grey[100] } },
                },
                legends: { text: { fill: colors.grey[100] } },
              }}
              legends={[
                {
                  anchor: "bottom",
                  direction: "row",
                  translateY: 100,
                  itemsSpacing: 16,
                  itemDirection: "left-to-right",
                  itemWidth: 100,
                  itemHeight: 20,
                  symbolSize: 14,
                  symbolShape: "circle",
                  itemTextColor: colors.grey[100],
                  data: SERIES.map(s => ({ id: s.key, label: s.label, color: s.color })),
                  effects: [{ on: "hover", style: { itemOpacity: 0.85 } }],
                },
              ]}
            />
          )}
        </div>
      </div>
    </Box>
  );
};

export default CashFlowBarChart;