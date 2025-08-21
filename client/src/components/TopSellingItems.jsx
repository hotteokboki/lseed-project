// src/components/TopSellingItemsPie/index.js
import { useEffect, useMemo, useRef, useState } from "react";
import { ResponsivePie } from "@nivo/pie";
import {
  Box, Typography, Select, MenuItem, Button, Tooltip, IconButton, CircularProgress
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { useTheme } from "@mui/material";
import { tokens } from "../theme";
import axiosClient from "../api/axiosClient";
import { useAuth } from "../context/authContext";

const PAGE_SIZE = 5;

const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default: return { from: null, to: null };
  }
};

const TopSellingItemsPie = ({ selectedSEId = null }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

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

  // ── Dynamic availability (years/quarters) via include_meta on SAME endpoint ─
  const [availabilityLoading, setAvailabilityLoading] = useState(true);
  const [availYears, setAvailYears] = useState([]);                    // e.g., [2025, 2024]
  const [availQuartersByYear, setAvailQuartersByYear] = useState({});  // { 2025: ["Q1","Q2"] }

  const scopeKey = JSON.stringify({ isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    if (isCoordinator && programLoading) return; // wait program id

    (async () => {
      setAvailabilityLoading(true);
      try {
        const params = { include_meta: 1 };
        if (isCoordinator && programId) params.program_id = programId;
        if (selectedSEId) params.se_id = selectedSEId;

        const resp = await axiosClient.get("/api/get-top-items-overall", { params });
        if (!alive) return;

        const meta = resp?.data?.meta;
        const years = Array.isArray(meta?.years) ? [...meta.years].sort((a, b) => b - a) : [];
        const qbj = meta?.quartersByYear || {};

        setAvailYears(years);
        setAvailQuartersByYear(qbj);
      } catch (e) {
        console.error("availability load (top items):", e?.response?.data || e.message);
        setAvailYears([]);
        setAvailQuartersByYear({});
      } finally {
        if (alive) setAvailabilityLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [scopeKey, programLoading, isCoordinator, programId, selectedSEId]);

  // ── Period controls (Overall / Quarterly / Yearly) ──────────────────────────
  const now = new Date();
  const fallbackYear = now.getFullYear();
  const latestYear = availYears[0] ?? fallbackYear;

  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedYear, setSelectedYear] = useState(latestYear);
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");
  const [metric, setMetric] = useState("value"); // "value" | "qty"

  useEffect(() => {
    if (!availYears.length) return;
    if (!availYears.includes(selectedYear)) setSelectedYear(latestYear);
    if (periodMode === "quarterly") {
      const qs = availQuartersByYear[selectedYear] || [];
      if (qs.length && !qs.includes(selectedQuarter)) setSelectedQuarter(qs[0]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availYears.join(","), periodMode]);

  // Defaults snapshot per scope (for Clear Filter)
  const defaultsRef = useRef(null);
  useEffect(() => { defaultsRef.current = null; }, [scopeKey]);

  useEffect(() => {
    if (availabilityLoading) return;
    if (!defaultsRef.current) {
      defaultsRef.current = {
        periodMode: "overall",
        selectedYear: latestYear,
        selectedQuarter: (availQuartersByYear[latestYear] || [])[0] ?? "Q1",
        metric: "value",
      };
      setSelectedYear(latestYear);
      setMetric("value");
      if (periodMode === "quarterly") {
        const qs = availQuartersByYear[latestYear] || [];
        if (qs.length) setSelectedQuarter(qs[0]);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availabilityLoading, latestYear]);

  const isDirty = useMemo(() => {
    if (!defaultsRef.current) return false;
    const d = defaultsRef.current;
    return (
      periodMode !== d.periodMode ||
      selectedYear !== d.selectedYear ||
      selectedQuarter !== d.selectedQuarter ||
      metric !== d.metric
    );
  }, [periodMode, selectedYear, selectedQuarter, metric]);

  const handleClear = () => {
    if (!defaultsRef.current) return;
    const d = defaultsRef.current;
    setPeriodMode(d.periodMode);
    setSelectedYear(d.selectedYear);
    setSelectedQuarter(d.selectedQuarter);
    setMetric(d.metric);
  };

  // Compute from/to + label
  const { from, to, label } = useMemo(() => {
    if (periodMode === "overall") return { from: null, to: null, label: "Overall" };
    if (periodMode === "yearly")
      return { from: `${selectedYear}-01-01`, to: `${Number(selectedYear) + 1}-01-01`, label: `Year ${selectedYear}` };
    const r = quarterRange(selectedYear, selectedQuarter);
    return { ...r, label: `${selectedYear} ${selectedQuarter}` };
  }, [periodMode, selectedQuarter, selectedYear]);

  // ── Main data fetch ─────────────────────────────────────────────────────────
  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState([]);
  const [errMsg, setErrMsg] = useState("");

  const fetchKey = JSON.stringify({ from, to, metric, isCoordinator, programId, selectedSEId });

  useEffect(() => {
    let alive = true;
    if (isCoordinator && programLoading) return; // wait program id

    setLoading(true);
    setRows([]);
    setErrMsg("");

    (async () => {
      try {
        const params = { metric };
        if (from) params.from = from;
        if (to) params.to = to;
        if (isCoordinator && programId) params.program_id = programId;
        if (selectedSEId) params.se_id = selectedSEId;

        const resp = await axiosClient.get("/api/get-top-items-overall", { params });
        if (!alive) return;
        const data = Array.isArray(resp?.data) ? resp.data : (resp?.data?.rows ?? []);
        console.log(data);
        setRows(data);
      } catch (e) {
        if (!alive) return;
        console.error("load top items:", e?.response?.data || e.message);
        setRows([]);
        setErrMsg("Failed to load top-selling items.");
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [fetchKey, programLoading, isCoordinator, programId, selectedSEId]);

  // ── Pagination + pie data ───────────────────────────────────────────────────
  const [showAll, setShowAll] = useState(false);
  const [page, setPage] = useState(0);

  useEffect(() => { setPage(0); }, [rows, showAll, metric, from, to]);

  const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  const sliceForPage = (arr, p) => arr.slice(p * PAGE_SIZE, p * PAGE_SIZE + PAGE_SIZE);

  const visibleRows = useMemo(() => {
    if (!showAll) return rows.slice(0, PAGE_SIZE);
    return sliceForPage(rows, page);
  }, [rows, showAll, page]);

  const pieData = useMemo(() => {
    return visibleRows.map(r => ({
      id: r.item_name,
      value: metric === "qty" ? Number(r.moved_qty || 0) : Number(r.moved_value || 0),
      raw: r,
    }));
  }, [visibleRows, metric]);

  // ── Ensure container has size (important for Nivo in tabs/collapses) ────────
  const chartRef = useRef(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useEffect(() => {
    const el = chartRef.current;
    if (!el) return;
    const update = () => setSize({ w: el.clientWidth, h: el.clientHeight });
    update(); // initial
    const ro = new ResizeObserver(update);
    ro.observe(el);
    window.addEventListener("resize", update);
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", update);
    };
  }, []);

  // dynamic options
  const yearOptions = availYears.length ? availYears : [latestYear];
  const quarterOptions = periodMode === "quarterly" ? (availQuartersByYear[selectedYear] || []) : [];
  const controlsDisabled = loading || availabilityLoading || (isCoordinator && programLoading);

  return (
    <Box backgroundColor={colors.primary[400]} p="20px">
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Box>
            <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
              Top-Selling Items
            </Typography>
            {!loading && !availabilityLoading && (
              <Typography variant="h6" color={colors.grey[300]} mt={0.5}>
                {label} • {showAll ? `All items (page ${page + 1}/${totalPages})` : "Top 5"} • Ranked by {metric === "qty" ? "quantity moved" : "estimated value moved"}
              </Typography>
            )}
            {!!errMsg && (
              <Typography variant="caption" color="#f44336" display="block" mt={0.5}>
                {errMsg}
              </Typography>
            )}
          </Box>
          <Tooltip
            arrow
            placement="top"
            title={
              <Box sx={{ maxWidth: 420, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">
                  How this is computed
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  • <b>Moved Qty</b> per month = max(Begin Qty − Final Qty, 0).
                </Typography>
                <Typography variant="body2" sx={{ mt: 0.5 }}>
                  • <b>Value</b> = Moved Qty × Item Unit Price (proxy for sales value).
                </Typography>
              </Box>
            }
          >
            <IconButton sx={{ ml: 1, color: colors.grey[300] }}>
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Box>

        {/* Controls */}
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
                disabled={controlsDisabled || quarterOptions.length === 0}
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
            </>
          )}

          <Select
            value={metric}
            onChange={(e) => setMetric(e.target.value)}
            disabled={controlsDisabled}
            sx={{
              height: 40, minWidth: 160,
              backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
              color: colors.grey[100], fontWeight: "bold",
              "& .MuiSelect-icon": { color: colors.grey[100] },
              "& fieldset": { border: "none" },
            }}
          >
            <MenuItem value="value">By Value (₱)</MenuItem>
            <MenuItem value="qty">By Quantity</MenuItem>
          </Select>

          {/* ← Add this: shows only when filters differ from defaults */}
          {isDirty && (
            <Button
              onClick={handleClear}
              variant="outlined"
              disabled={controlsDisabled}
              sx={{
                height: 40, minWidth: 130,
                borderColor: colors.grey[100],
                color: colors.grey[100],
                backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                fontWeight: "bold",
                "&:hover": { backgroundColor: colors.blueAccent[700] },
              }}
            >
              Reset Filter
            </Button>
          )}

          <Button
            variant="outlined"
            onClick={() => setShowAll(s => !s)}
            disabled={controlsDisabled}
            sx={{
              height: 40, minWidth: 130,
              borderColor: colors.grey[100],
              backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
              color: colors.grey[100],
              fontWeight: "bold",
              "&:hover": { backgroundColor: colors.blueAccent[700] },
            }}
          >
            {showAll ? "Show Top 5" : "Show All"}
          </Button>
        </Box>
      </Box>

      {/* Chart + pager (container has explicit size & resize awareness) */}
      <Box display="flex" alignItems="center">
        {showAll && !loading && !availabilityLoading && (
          <Button
            variant="contained"
            disabled={page === 0}
            onClick={() => setPage(p => Math.max(0, p - 1))}
            sx={{
              mx: 2,
              height: "fit-content",
              backgroundColor: colors.blueAccent[600],
              color: colors.grey[100],
              "&:disabled": { backgroundColor: colors.grey[600], color: colors.grey[300] },
            }}
          >
            ◀ Prev
          </Button>
        )}

        <Box ref={chartRef} sx={{ height: 420, flexGrow: 1, minWidth: 0, display: "flex", alignItems: "center", justifyContent: "center" }}>
          {loading || availabilityLoading || (isCoordinator && programLoading) ? (
            <CircularProgress sx={{ color: colors.greenAccent[500] }} />
          ) : pieData.length === 0 ? (
            <Typography color={colors.grey[300]}>No data to display.</Typography>
          ) : size.w > 0 && size.h > 0 ? (
            <ResponsivePie
              data={pieData}
              margin={{ top: 30, right: 80, bottom: 80, left: 80 }}
              innerRadius={0.5}
              padAngle={0.7}
              cornerRadius={3}
              colors={{ scheme: "nivo" }}
              borderWidth={1}
              borderColor={{ from: "color", modifiers: [["darker", 0.2]] }}
              arcLinkLabelsTextColor={colors.grey[100]}
              arcLinkLabelsThickness={2}
              arcLinkLabelsColor={{ from: "color" }}
              arcLabelsSkipAngle={10}
              arcLabelsTextColor={colors.grey[100]}
              valueFormat={v =>
                metric === "qty"
                  ? `${Number(v).toLocaleString()}`
                  : `₱${Number(v).toLocaleString()}`
              }
              tooltip={({ datum }) => {
                const id = datum?.id ?? "Item";
                const v = Number(datum?.value || 0);
                const r = datum?.data?.raw || {};
                return (
                  <div style={{
                    background: theme.palette.mode === "dark" ? "#333" : "#fff",
                    color: theme.palette.mode === "dark" ? "#fff" : "#333",
                    padding: "10px",
                    borderRadius: "6px",
                    boxShadow: "0 2px 6px rgba(0,0,0,0.2)"
                  }}>
                    <strong>{id}</strong><br />
                    {metric === "qty" ? (
                      <>Moved Qty: <b>{v.toLocaleString()}</b><br />
                        Unit price: ₱{Number(r.unit_price || 0).toLocaleString()}</>
                    ) : (
                      <>Value Moved: <b>₱{v.toLocaleString()}</b><br />
                        Qty: {Number(r.moved_qty || 0).toLocaleString()}</>
                    )}
                  </div>
                );
              }}
            />
          ) : null}
        </Box>

        {showAll && !loading && !availabilityLoading && (
          <Button
            variant="contained"
            disabled={(page + 1) >= totalPages}
            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
            sx={{
              mx: 2,
              height: "fit-content",
              backgroundColor: colors.blueAccent[600],
              color: colors.grey[100],
              "&:disabled": { backgroundColor: colors.grey[600], color: colors.grey[300] },
            }}
          >
            Next ▶
          </Button>
        )}
      </Box>
    </Box>
  );
};

export default TopSellingItemsPie;
