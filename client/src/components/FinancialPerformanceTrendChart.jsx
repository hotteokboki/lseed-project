// src/components/FinancialPerformanceTrendChart.jsx
import React, { useEffect, useMemo, useState } from "react";
import {
  Box,
  Typography,
  Button,
  CircularProgress,
  Tooltip,
  IconButton,
  Select,
  MenuItem,
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { ResponsiveLine } from "@nivo/line";
import { useTheme } from "@mui/material";
import { tokens } from "../theme";
import axiosClient from "../api/axiosClient";
import { useAuth } from "../context/authContext";

const SEsPerPage = 5;

// Map quarter -> { from, to } (to is END-EXCLUSIVE)
const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default: return { from: null, to: null };
  }
};

const FinancialPerformanceTrendChart = ({
  isDashboard = false,
  selectedSEId = null, // optional: non-coordinator can scope to a specific SE
}) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

  // Local UI state
  const [rows, setRows] = useState([]);
  const [labels, setLabels] = useState({});
  const [loading, setLoading] = useState(true);
  const [errMsg, setErrMsg] = useState("");
  const [showAll, setShowAll] = useState(false);
  const [currentPage, setCurrentPage] = useState(0);

  // Period selection
  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());

  // Dynamic availability (derived from all reports for the current scope)
  const [availYears, setAvailYears] = useState([]); // e.g., [2023, 2024, 2025]
  const [availQuartersByYear, setAvailQuartersByYear] = useState({}); // { 2025: ["Q1","Q2"] }
  const [availabilityLoading, setAvailabilityLoading] = useState(true);

  // Load SE labels once
  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const { data } = await axiosClient.get("/api/get-all-social-enterprises");
        if (!alive) return;
        const m = {};
        for (const se of data || []) m[se.se_id] = se.abbr || se.team_name || se.se_id;
        setLabels(m);
      } catch (e) {
        if (!alive) return;
        console.warn("labels load failed:", e?.response?.data || e.message);
      }
    })();
    return () => { alive = false; };
  }, []);

  // If coordinator, fetch their program_id once
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
        const pid = resp?.data?.[0]?.program_id ?? null;
        setProgramId(pid);
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

  // Build availability (years/quarters) for current scope (portfolio / program / selected SE)
  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        setAvailabilityLoading(true);
        if (isCoordinator && programLoading) return; // wait for program id

        const params = {};
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;
        else if (isCoordinator && programId) params.program_id = programId;
        // no from/to => get all rows for scope
        const resp = await axiosClient.get("/api/ratings/top-star-trend", { params });
        if (!alive) return;

        const all = Array.isArray(resp?.data) ? resp.data : [];
        const yearsSet = new Set();
        const quartersMap = new Map(); // year -> Set("Q1"...)

        for (const r of all) {
          const d = new Date(r.month);
          if (isNaN(d)) continue;
          const y = d.getFullYear();
          const qNum = Math.floor(d.getMonth() / 3) + 1; // 1..4
          yearsSet.add(y);
          if (!quartersMap.has(y)) quartersMap.set(y, new Set());
          quartersMap.get(y).add(`Q${qNum}`);
        }

        const ys = Array.from(yearsSet).sort((a, b) => b - a); // latest first
        const qbj = {};
        ys.forEach(y => {
          qbj[y] = Array.from(quartersMap.get(y) || []).sort(
            (a, b) => Number(a.slice(1)) - Number(b.slice(1))
          );
        });

        setAvailYears(ys);
        setAvailQuartersByYear(qbj);

        // keep selectedYear/Quarter valid for the new availability
        if (ys.length && !ys.includes(selectedYear)) {
          setSelectedYear(ys[0]); // default to latest available year
        }
        if (periodMode === "quarterly") {
          const qs = qbj[ys.includes(selectedYear) ? selectedYear : ys[0]] || [];
          if (qs.length && !qs.includes(selectedQuarter)) setSelectedQuarter(qs[0]);
        }
      } catch (e) {
        console.error("load availability:", e?.response?.data || e.message);
        setAvailYears([]);
        setAvailQuartersByYear({});
      } finally {
        if (alive) setAvailabilityLoading(false);
      }
    })();
    return () => { alive = false; };
  }, [isCoordinator, programId, programLoading, selectedSEId, periodMode, selectedYear, selectedQuarter]);

  // When switching period, auto-pick latest available year & valid quarter
  useEffect(() => {
    if ((periodMode === "yearly" || periodMode === "quarterly") && availYears.length) {
      if (!availYears.includes(selectedYear)) setSelectedYear(availYears[0]);
    }
  }, [periodMode, availYears, selectedYear]);

  useEffect(() => {
    if (periodMode === "quarterly") {
      const qs = availQuartersByYear[selectedYear] || [];
      if (qs.length && !qs.includes(selectedQuarter)) setSelectedQuarter(qs[0]);
    }
  }, [periodMode, selectedYear, availQuartersByYear, selectedQuarter]);

  // Compute from/to based on period UI
  const { from, to, periodLabel } = useMemo(() => {
    if (periodMode === "overall") {
      return { from: null, to: null, periodLabel: "Overall" };
    }
    if (periodMode === "yearly") {
      const y = selectedYear;
      return { from: `${y}-01-01`, to: `${Number(y) + 1}-01-01`, periodLabel: `Year ${y}` };
    }
    const r = quarterRange(selectedYear, selectedQuarter);
    return { ...r, periodLabel: `${selectedYear} ${selectedQuarter}` };
  }, [periodMode, selectedQuarter, selectedYear]);

  // Fetch star-trend rows whenever period or filters change
  useEffect(() => {
    let alive = true;
    setLoading(true);
    setErrMsg("");
    setRows([]);
    setCurrentPage(0);

    (async () => {
      try {
        if (isCoordinator && programLoading) return;

        const baseParams = {};
        if (from) baseParams.from = from;
        if (to) baseParams.to = to;

        let params = { ...baseParams };
        if (!isCoordinator && selectedSEId) params.se_id = selectedSEId;
        else if (isCoordinator && programId) params.program_id = programId;

        const resp = await axiosClient.get("/api/ratings/top-star-trend", { params });
        const data = resp?.data;
        if (!alive) return;
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        console.error("load star trend:", e?.response?.data || e.message);
        setErrMsg("Failed to load chart data.");
        setRows([]);
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => { alive = false; };
  }, [from, to, isCoordinator, programId, programLoading, selectedSEId]);

  // Build series (group by se_id), sort months, compute x-domain
  const { seriesAll, monthDomain } = useMemo(() => {
    const bySE = new Map();
    const monthsSet = new Set();

    const monthKey = (v) => {
      if (typeof v === "string") return v.slice(0, 7); // "YYYY-MM"
      try { return new Date(v).toISOString().slice(0, 7); } catch { return String(v ?? "").slice(0, 7); }
    };

    for (const r of rows) {
      const month = monthKey(r.month);
      if (!month) continue;
      monthsSet.add(month);
      if (!bySE.has(r.se_id)) bySE.set(r.se_id, []);
      bySE.get(r.se_id).push({
        x: month,
        y: typeof r.stars_half === "number" ? r.stars_half : Number(r.stars_half || 0),
      });
    }

    const seriesAll = Array.from(bySE.entries()).map(([se_id, pts]) => {
      pts.sort((a, b) => a.x.localeCompare(b.x));
      const avg = pts.length ? pts.reduce((s, p) => s + (p.y || 0), 0) / pts.length : 0;
      return { se_id, id: labels[se_id] || se_id.slice(0, 6), color: undefined, data: pts, _avg: avg };
    });

    // Rank by avg desc to prioritize best lines in Top 5
    seriesAll.sort((a, b) => b._avg - a._avg);

    const monthDomain = Array.from(monthsSet).sort((a, b) => a.localeCompare(b));
    return { seriesAll, monthDomain };
  }, [rows, labels]);

  // paging / top5 view
  const visibleSeries = useMemo(() => {
    if (seriesAll.length === 0) return [];
    if (!showAll) return seriesAll.slice(0, SEsPerPage);
    const start = currentPage * SEsPerPage;
    return seriesAll.slice(start, start + SEsPerPage);
  }, [seriesAll, showAll, currentPage]);

  // --- Clear Filters: show ONLY when there are changes from defaults ---
  // Defaults: periodMode="overall", showAll=false, currentPage=0
  const isDefaultFilters = periodMode === "overall" && !showAll && currentPage === 0;
  const handleClearFilters = () => {
    setPeriodMode("overall");
    setShowAll(false);
    setCurrentPage(0);
    // (Year/Quarter will auto-adjust when user switches mode)
  };

  const controlsDisabled = loading || availabilityLoading || (isCoordinator && programLoading);

  return (
    <Box gridColumn="span 12" gridRow="span 2" backgroundColor={colors.primary[400]} p="20px">
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Box>
            <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
              {`Financial Performance Trend (${showAll ? "All" : "Top 5"})`}
            </Typography>
            {!loading && (
              <Typography variant="h6" color={colors.grey[300]} mt={0.5}>
                Stars (0–5, half-steps) per month — derived from composite scores
                {periodLabel ? ` • ${periodLabel}` : ""}
              </Typography>
            )}
            {errMsg && (
              <Typography variant="caption" color="#f44336" display="block" mt={0.5}>
                {errMsg}
              </Typography>
            )}
          </Box>

          <Tooltip
            arrow
            placement="top"
            title={
              <Box sx={{ maxWidth: 360, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">How to read this chart</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Each line is a Social Enterprise’s monthly performance shown as <b>star ratings</b>
                  (0–5 in 0.5 steps), computed from your monthly composite scores.
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Use the <b>Period</b> selectors to view Overall, a specific <b>Quarter</b>
                  (Q1 Jan–Mar, Q2 Apr–Jun, Q3 Jul–Sep, Q4 Oct–Dec), or a full <b>Year</b>.
                </Typography>
              </Box>
            }
          >
            <IconButton sx={{ ml: 1, color: colors.grey[300] }}>
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Box>

        {/* Right controls: Period + (Quarter/Year) + Show All + Clear Filters (conditional) */}
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
            <>
              <Select
                value={selectedYear}
                onChange={(e) => setSelectedYear(e.target.value)}
                disabled={controlsDisabled}
                sx={{
                  height: 40, minWidth: 110,
                  backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                  color: colors.grey[100], fontWeight: "bold",
                  "& .MuiSelect-icon": { color: colors.grey[100] },
                  "& fieldset": { border: "none" },
                }}
              >
                {(availYears.length ? availYears : [selectedYear]).map((y) => (
                  <MenuItem key={y} value={y}>{y}</MenuItem>
                ))}
              </Select>

              <Select
                value={selectedQuarter}
                onChange={(e) => setSelectedQuarter(e.target.value)}
                disabled={controlsDisabled}
                sx={{
                  height: 40, minWidth: 100,
                  backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                  color: colors.grey[100], fontWeight: "bold",
                  "& .MuiSelect-icon": { color: colors.grey[100] },
                  "& fieldset": { border: "none" },
                }}
              >
                {(availQuartersByYear[selectedYear]?.length
                  ? availQuartersByYear[selectedYear]
                  : ["Q1", "Q2", "Q3", "Q4"]
                ).map((q) => (
                  <MenuItem key={q} value={q}>{q}</MenuItem>
                ))}
              </Select>
            </>
          )}

          {periodMode === "yearly" && (
            <Select
              value={selectedYear}
              onChange={(e) => setSelectedYear(e.target.value)}
              disabled={controlsDisabled}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {(availYears.length ? availYears : [selectedYear]).map((y) => (
                <MenuItem key={y} value={y}>{y}</MenuItem>
              ))}
            </Select>
          )}

          <Button
            variant="outlined"
            onClick={() => { setShowAll((s) => !s); setCurrentPage(0); }}
            disabled={controlsDisabled}
            sx={{
              height: 40, minWidth: 140,
              borderColor: colors.grey[100],
              backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
              color: colors.grey[100], fontWeight: "bold",
              "&:hover": { backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[700] },
              "&:disabled": { backgroundColor: colors.grey[600], color: colors.grey[300] },
            }}
          >
            {showAll ? "Show Top 5" : "Show All"}
          </Button>

          {/* Show Clear Filters ONLY if filters changed */}
          {!isDefaultFilters && (
            <Button
              variant="outlined"
              onClick={handleClearFilters}
              disabled={controlsDisabled}
              sx={{
                height: 40, minWidth: 130,
                borderColor: colors.grey[100],
                backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "&:hover": { backgroundColor: controlsDisabled ? colors.grey[600] : colors.blueAccent[700] },
                "&:disabled": { backgroundColor: colors.grey[600], color: colors.grey[300] },
              }}
            >
              Clear Filters
            </Button>
          )}
        </Box>
      </Box>

      {/* Chart */}
      <Box height="320px" display="flex" alignItems="center">
        {showAll && !loading && (
          <Button
            variant="contained"
            color="secondary"
            disabled={currentPage === 0}
            onClick={() => setCurrentPage((p) => Math.max(0, p - 1))}
            sx={{
              mx: 2, height: "fit-content",
              backgroundColor: colors.blueAccent[600],
              color: colors.grey[100],
              "&:disabled": { backgroundColor: colors.grey[600], color: colors.grey[300] },
            }}
          >
            ◀ Prev
          </Button>
        )}

        <Box flexGrow={1} minWidth={0} overflow="hidden" display="flex" justifyContent="center" alignItems="center" height="100%">
          {loading || availabilityLoading || (isCoordinator && programLoading) ? (
            <Box display="flex" flexDirection="column" alignItems="center" gap={2}>
              <CircularProgress
                size={60}
                sx={{ color: colors.greenAccent[500], "& .MuiCircularProgress-circle": { strokeLinecap: "round" } }}
              />
              <Typography variant="h6" color={colors.grey[300]}>
                {availabilityLoading || (isCoordinator && programLoading)
                  ? "Loading filters…"
                  : "Loading chart data…"}
              </Typography>
            </Box>
          ) : seriesAll.length === 0 ? (
            <Typography variant="h6" color={colors.grey[300]}>No data available for plotting.</Typography>
          ) : (
            <ResponsiveLine
              data={visibleSeries.map(({ id, color, data }) => ({ id, color, data }))}
              theme={{
                axis: {
                  domain: { line: { stroke: colors.grey[100] } },
                  legend: { text: { fill: colors.grey[100] } },
                  ticks: { line: { stroke: colors.grey[100], strokeWidth: 1 }, text: { fill: colors.grey[100] } },
                },
                legends: { text: { fill: colors.grey[100] } },
                tooltip: { container: { color: colors.primary[500] } },
              }}
              colors={isDashboard ? { datum: "color" } : { scheme: "nivo" }}
              margin={{ top: 50, right: 210, bottom: 50, left: 60 }}
              xScale={{ type: "point", domain: monthDomain }}
              yScale={{ type: "linear", min: 0, max: 5, stacked: false, reverse: false }}
              yFormat=" >-.2f"
              curve="catmullRom"
              axisTop={null}
              axisRight={null}
              axisBottom={{
                orient: "bottom",
                tickSize: 0,
                tickPadding: 5,
                tickRotation: 0,
                legend: isDashboard ? undefined : "Month",
                legendOffset: 36,
                legendPosition: "middle",
              }}
              axisLeft={{
                orient: "left",
                tickSize: 3,
                tickPadding: 5,
                tickRotation: 0,
                tickValues: [0, 1, 2, 3, 4, 5],
                format: (v) => `${v}`,
                legend: isDashboard ? undefined : "Stars (0–5)",
                legendOffset: -40,
                legendPosition: "middle",
              }}
              enableGridX={false}
              enableGridY={false}
              pointSize={8}
              pointColor={{ theme: "background" }}
              pointBorderWidth={2}
              pointBorderColor={{ from: "serieColor" }}
              pointLabelYOffset={-12}
              useMesh
              legends={[
                {
                  anchor: "bottom-right",
                  direction: "column",
                  translateX: 110,
                  translateY: -30,
                  itemsSpacing: 4,
                  itemDirection: "left-to-right",
                  itemWidth: 120,
                  itemHeight: 12,
                  itemOpacity: 1,
                  symbolSize: 12,
                  symbolShape: "circle",
                  symbolBorderColor: "rgba(0, 0, 0, .5)",
                  effects: [{ on: "hover", style: { itemBackground: "rgba(0, 0, 0, .03)", itemOpacity: 1 } }],
                },
              ]}
            />
          )}
        </Box>

        {showAll && !loading && (
          <Button
            variant="contained"
            color="secondary"
            disabled={(currentPage + 1) * SEsPerPage >= seriesAll.length}
            onClick={() => setCurrentPage((p) => p + 1)}
            sx={{
              mx: 2, height: "fit-content",
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

export default FinancialPerformanceTrendChart;