import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Box, Typography, Tooltip as MuiTooltip, IconButton, useTheme,
  Select, MenuItem, Button
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { alpha } from "@mui/material/styles";
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, Legend
} from "recharts";
import axiosClient from "../api/axiosClient";
import { tokens } from "../theme";
import { useAuth } from "../context/authContext";

/** Quarter ranges — Q1: Jan–Mar, Q2: Apr–Jun, Q3: Jul–Sep, Q4: Oct–Dec */
const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default: return { from: null, to: null };
  }
};

function CashTooltip({ active, payload, label }) {
  const theme = useTheme();
  const tok = tokens(theme.palette.mode);
  if (!active || !payload?.length) return null;

  const peso = (v) => `₱${Math.round(Number(v || 0)).toLocaleString()}`;
  const fmtMonth = (ym) => {
    const [y, m] = String(ym).split("-");
    return new Date(Number(y), Number(m) - 1, 1)
      .toLocaleDateString(undefined, { month: "short", year: "numeric" });
  };

  return (
    <Box
      sx={{
        p: 1.25, px: 1.5,
        bgcolor: theme.palette.mode === "dark"
          ? alpha(tok.primary[700] ?? "#0c101b", 0.98)
          : "#fff",
        border: `1px solid ${theme.palette.divider}`,
        borderRadius: 1.25,
        boxShadow: theme.palette.mode === "dark"
          ? "0 10px 24px rgba(0,0,0,.45)"
          : "0 10px 24px rgba(0,0,0,.12)",
        color: theme.palette.text.primary,
        minWidth: 200,
      }}
    >
      <Typography variant="subtitle2" sx={{ mb: 0.5, color: theme.palette.text.secondary }}>
        {fmtMonth(label)}
      </Typography>

      {payload.map((row) => (
        <Box key={row.dataKey} sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <Box sx={{ width: 10, height: 10, borderRadius: "50%", bgcolor: row.stroke || row.color || theme.palette.info.main }} />
          <Typography variant="body2" sx={{ fontVariantNumeric: "tabular-nums" }}>
            {row.name ?? row.dataKey}: {peso(row.value)}
          </Typography>
        </Box>
      ))}
    </Box>
  );
}

export default function CumulativeCashPosition({
  // parent can still lock a custom window; otherwise the component owns filtering
  from,
  to,
  // optional: pass an SE when used in an SE profile
  selectedSEId = null,
}) {
  const theme = useTheme();
  const tok = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

  const SERIES = [
    { key: "cum", label: "Cumulative Position", color: theme.palette.success.main },
    { key: "net", label: "Monthly Net", color: theme.palette.info.main },
  ];

  // ── Coordinator's program_id (load once) ─────────────────────────────────────
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

  // ── Build dynamic availability (years/quarters) using the SAME endpoint ──────
  // We fetch ALL months for the current scope (no from/to), then derive years/quarters.
  const [availabilityLoading, setAvailabilityLoading] = useState(true);
  const [availYears, setAvailYears] = useState([]);                 // [2024, 2025, ...] (latest first)
  const [availQuartersByYear, setAvailQuartersByYear] = useState({});// { 2025: ["Q1","Q2"] }

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

        // Single endpoint (no dates) → returns [{ month, net_cash }, ...]
        const resp = await axiosClient.get("/api/finance/monthly-net-cash", { params });
        if (!alive) return;
        const all = Array.isArray(resp?.data) ? resp.data : [];

        // Normalize to "YYYY-MM"
        const months = all
          .map(r => {
            try { return new Date(r.month).toISOString().slice(0, 7); }
            catch { return String(r?.month ?? "").slice(0, 7); }
          })
          .filter(Boolean);

        // Years (latest first)
        const yearsSet = new Set(months.map(ym => Number(ym.slice(0, 4))));
        const years = Array.from(yearsSet).sort((a, b) => b - a);

        // Quarters by year
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

        setAvailYears(years);
        setAvailQuartersByYear(qbj);
      } catch (e) {
        console.error("availability load:", e?.response?.data || e.message);
        setAvailYears([]);
        setAvailQuartersByYear({});
      } finally {
        if (alive) setAvailabilityLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [scopeKey, programLoading, isCoordinator, programId, selectedSEId]);

  const now = new Date();
  const fallbackYear = now.getFullYear();
  const latestYear = availYears[0] ?? fallbackYear;

  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedYear, setSelectedYear] = useState(latestYear);
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
  }, [availYears.join(","), periodMode]);

  // Reset defaults snapshot whenever scope changes
  const defaultsRef = useRef(null);
  useEffect(() => {
    defaultsRef.current = null;
  }, [scopeKey]);

  // initialize defaults snapshot (per-scope)
  useEffect(() => {
    if (availabilityLoading) return;
    if (!defaultsRef.current) {
      defaultsRef.current = {
        periodMode: "overall",
        selectedYear: latestYear,
        selectedQuarter: (availQuartersByYear[latestYear] || [])[0] ?? "Q1",
      };
      setSelectedYear(latestYear);
      if (periodMode === "quarterly") {
        const qs = availQuartersByYear[latestYear] || [];
        if (qs.length) setSelectedQuarter(qs[0]);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availabilityLoading, latestYear]);

  const canUseLocalFilters = !from && !to;

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

  // Compute from/to for the data request
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

  // Main data fetch (filtered by from/to)
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [errMsg, setErrMsg] = useState("");

  const fetchKey = JSON.stringify({ effFrom, effTo, isCoordinator, programId, selectedSEId });

  // Data fetch: controls loading / rows / error state
  useEffect(() => {
    let alive = true;

    // start a fresh load
    setLoading(true);
    setErrMsg("");
    setRows([]);

    // if coordinator, wait until we know the program_id
    if (isCoordinator && programLoading) {
      return () => { alive = false; }; // keep spinner up until program id arrives
    }

    (async () => {
      try {
        const params = {};
        if (effFrom) params.from = effFrom;
        if (effTo) params.to = effTo;
        if (isCoordinator && programId) params.program_id = programId;
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;

        const { data } = await axiosClient.get("/api/finance/monthly-net-cash", { params });
        if (!alive) return;
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (!alive) return;
        console.error("load monthly-net-cash:", e?.response?.data || e.message);
        setRows([]);
        setErrMsg("Failed to load net cash.");
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [effFrom, effTo, isCoordinator, programId, selectedSEId, programLoading]);

  // Recharts data shape
  const data = useMemo(() => {
    let run = 0;
    return rows.map(r => {
      const net = Number(r.net_cash || 0);
      run += net; // running total from first visible month
      return { x: String(r.month).slice(0, 7), net, cum: run };
    });
  }, [rows]);

  const yearOptions = availYears.length ? availYears : [latestYear];
  const quarterOptions = periodMode === "quarterly" ? (availQuartersByYear[selectedYear] || []) : [];

  const controlsDisabled = loading || availabilityLoading || (isCoordinator && programLoading) || !canUseLocalFilters;

  return (
    <Box bgcolor={tok.primary[400]} p="20px" pb={4} sx={{ borderRadius: 2 }}>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2} flexWrap="wrap" gap={1.5}>
        <Box display="flex" alignItems="center" gap={2} flexWrap="wrap">
          <Typography variant="h3" fontWeight="bold" color={tok.greenAccent[500]}>
            Cumulative Net Cash Position
          </Typography>

          <MuiTooltip
            arrow placement="top"
            title={
              <Box sx={{ maxWidth: 420, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">
                  How to read • Cumulative Net Cash
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  • <b>Green line</b>: running total of monthly Net (Inflow − Outflow) from the first visible month.
                </Typography>
                <Typography variant="body2" sx={{ mt: 0.5 }}>
                  • <b>Blue line</b>: monthly Net for each month.
                </Typography>
                <Typography variant="body2" sx={{ mt: 0.5 }}>
                  • The cumulative line <b>restarts</b> at the start of the selected window ({label}).
                </Typography>
                <Typography variant="body2" sx={{ mt: 1, opacity: 0.85 }}>
                  • Currency: <b>₱</b> • Timezone: <b>Asia/Manila</b>.
                </Typography>
              </Box>
            }
          >
            <IconButton sx={{ color: tok.grey[300], ml: 0.5 }}>
              <HelpOutlineIcon />
            </IconButton>
          </MuiTooltip>

          {errMsg && (
            <Typography variant="caption" color="#f44336">
              {errMsg}
            </Typography>
          )}
        </Box>

        {/* Period Controls (single endpoint pattern) */}
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

          {periodMode === "yearly" && (
            <Select
              value={selectedYear}
              onChange={(e) => setSelectedYear(e.target.value)}
              disabled={controlsDisabled || yearOptions.length === 0}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: controlsDisabled ? tok.grey[600] : tok.blueAccent[600],
                color: tok.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: tok.grey[100] },
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
                  backgroundColor: controlsDisabled ? tok.grey[600] : tok.blueAccent[600],
                  color: tok.grey[100], fontWeight: "bold",
                  "& .MuiSelect-icon": { color: tok.grey[100] },
                  "& fieldset": { border: "none" },
                }}
              >
                {yearOptions.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
              </Select>

              <Select
                value={selectedQuarter}
                onChange={(e) => setSelectedQuarter(e.target.value)}
                disabled={controlsDisabled || quarterOptions.length === 0}
                sx={{
                  height: 40, minWidth: 100,
                  backgroundColor: (controlsDisabled || quarterOptions.length === 0) ? tok.grey[600] : tok.blueAccent[600],
                  color: tok.grey[100], fontWeight: "bold",
                  "& .MuiSelect-icon": { color: tok.grey[100] },
                  "& fieldset": { border: "none" },
                }}
              >
                {quarterOptions.map(q => <MenuItem key={q} value={q}>{q}</MenuItem>)}
              </Select>
            </>
          )}

          {/* Clear Filter only when user changed filters (per-scope defaults) */}
          {isDirty && canUseLocalFilters && (
            <Button
              onClick={handleClear}
              variant="outlined"
              sx={{
                height: 40, minWidth: 120,
                borderColor: tok.grey[100],
                color: tok.grey[100],
                backgroundColor: tok.blueAccent[600],
                fontWeight: "bold",
                "&:hover": { backgroundColor: tok.blueAccent[700] },
              }}
            >
              Clear Filter
            </Button>
          )}
        </Box>
      </Box>

      {/* Chart */}
      <Box sx={{ height: 360, overflow: "visible", position: "relative" }}>
        {/* Chart */}
        <div style={{ height: 360, display: "flex", flexDirection: "column", alignItems: "center" }}>
          <div style={{ height: 320, width: "100%" }}>
            {loading ? (
              <Box sx={{ color: tok.grey[300], display: "flex", alignItems: "center", justifyContent: "center", height: "100%" }}>
                <Typography>Loading…</Typography>
              </Box>
            ) : rows.length === 0 ? (
              <Box sx={{ color: tok.grey[300], display: "flex", alignItems: "center", justifyContent: "center", height: "100%" }}>
                <Typography>No data to display.</Typography>
              </Box>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke={theme.palette.divider} strokeDasharray="3 3" />
                  <XAxis
                    dataKey="x"
                    tick={{ fill: theme.palette.text.secondary, fontSize: 12 }}
                    axisLine={{ stroke: theme.palette.divider }}
                    tickLine={{ stroke: theme.palette.divider }}
                  />
                  <YAxis
                    tick={{ fill: theme.palette.text.secondary, fontSize: 12 }}
                    axisLine={{ stroke: theme.palette.divider }}
                    tickLine={{ stroke: theme.palette.divider }}
                    tickFormatter={(v) => `₱${Math.round(Number(v || 0)).toLocaleString()}`}
                  />
                  <Tooltip
                    wrapperStyle={{ zIndex: 9999, pointerEvents: "auto" }}
                    allowEscapeViewBox={{ x: true, y: true }}
                    cursor={{ stroke: theme.palette.action.disabled, strokeWidth: 1 }}
                    content={(props) => <CashTooltip {...props} />}
                  />
                  <Legend
                    verticalAlign="top"
                    align="right"
                    iconType="plainline"
                    iconSize={18}
                    wrapperStyle={{ color: theme.palette.text.secondary, paddingBottom: 8 }}
                    formatter={(v) => (v === "cum" ? "Cumulative Position" : v === "net" ? "Monthly Net" : v)}
                  />
                  <Line
                    type="monotone"
                    dataKey="cum"
                    name="Cumulative Position"
                    stroke={SERIES[0].color}
                    strokeWidth={2.25}
                    dot={false}
                    activeDot={{ r: 4 }}
                    isAnimationActive={false}
                  />
                  <Line
                    type="monotone"
                    dataKey="net"
                    name="Monthly Net"
                    stroke={SERIES[1].color}
                    strokeWidth={1.75}
                    dot={false}
                    activeDot={{ r: 4 }}
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      </Box>
    </Box>
  );
}