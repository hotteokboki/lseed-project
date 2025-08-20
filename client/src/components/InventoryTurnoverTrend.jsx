import { useEffect, useMemo, useState } from "react";
import { ResponsiveLine } from "@nivo/line";
import {
  Box, Typography, Select, MenuItem, Tooltip, IconButton, CircularProgress
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { useTheme } from "@mui/material";
import { tokens } from "../theme";
import axiosClient from "../api/axiosClient";

const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year) + 1}-01-01` };
    default:   return { from: null, to: null };
  }
};

const InventoryTurnoverTrend = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  // threshold legend colors
  const LEGEND_COLORS = {
    poor: theme.palette.mode === "dark" ? colors.redAccent[300] : colors.redAccent[500],
    moderate: theme.palette.mode === "dark" ? colors.primary[300] : colors.grey[700],
    good: theme.palette.mode === "dark" ? colors.greenAccent[300] : colors.greenAccent[500],
  };

  // Period controls
  const now = new Date();
  const defaultYear = now.getFullYear();
  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");
  const [selectedYear, setSelectedYear] = useState(defaultYear);
  const yearOptions = Array.from({ length: 7 }, (_, i) => defaultYear - 3 + i);

  const { from, to, label } = useMemo(() => {
    if (periodMode === "overall") return { from: null, to: null, label: "Overall" };
    if (periodMode === "yearly")
      return { from: `${selectedYear}-01-01`, to: `${selectedYear + 1}-01-01`, label: `Year ${selectedYear}` };
    const r = quarterRange(selectedYear, selectedQuarter);
    return { ...r, label: `${selectedYear} ${selectedQuarter}` };
  }, [periodMode, selectedQuarter, selectedYear]);

  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState([]);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setRows([]);
      try {
        const params = {};
        if (from) params.from = from;
        if (to)   params.to   = to;
        const { data } = await axiosClient.get("/api/get-overall-inventory-turnover", { params });
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        console.error("load inv turnover:", e?.response?.data || e.message);
        setRows([]);
      } finally {
        setLoading(false);
      }
    })();
  }, [from, to]);

  const series = useMemo(() => {
    const pts = rows.map(r => ({
      x: String(r.month).slice(0, 7),                  // YYYY-MM
      y: r.turnover === null ? null : Number(r.turnover)
    }));
    const data = [{ id: "Inventory Turnover", data: pts }];
    const months = Array.from(new Set(pts.map(p => p.x))).sort();
    return { data, months };
  }, [rows]);

  return (
    <Box backgroundColor={colors.primary[400]} p="20px">
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Box>
            <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
              {loading ? "Loading…" : (series.data[0].data.length ? "Inventory Turnover Trend (Overall)" : "No Data")}
            </Typography>
            {!loading && (
              <Typography variant="h6" color={colors.grey[300]} mt={0.5}>
                {label} • Turnover = COGS / Avg Inventory • Tooltip shows COGS, Avg Inv & DIO
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

        {/* Period controls */}
        <Box display="flex" alignItems="center" gap={1}>
          <Select
            value={periodMode}
            onChange={(e) => setPeriodMode(e.target.value)}
            sx={{
              height: 40, minWidth: 130,
              backgroundColor: colors.blueAccent[600],
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
              value={selectedQuarter}
              onChange={(e) => setSelectedQuarter(e.target.value)}
              sx={{
                height: 40, minWidth: 100,
                backgroundColor: colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              <MenuItem value="Q1">Q1</MenuItem>
              <MenuItem value="Q2">Q2</MenuItem>
              <MenuItem value="Q3">Q3</MenuItem>
              <MenuItem value="Q4">Q4</MenuItem>
            </Select>
          )}

          {(periodMode === "quarterly" || periodMode === "yearly") && (
            <Select
              value={selectedYear}
              onChange={(e) => setSelectedYear(e.target.value)}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: colors.blueAccent[600],
                color: colors.grey[100], fontWeight: "bold",
                "& .MuiSelect-icon": { color: colors.grey[100] },
                "& fieldset": { border: "none" },
              }}
            >
              {yearOptions.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
            </Select>
          )}
        </Box>
      </Box>

      {/* Chart */}
      <Box height="360px" display="flex" alignItems="center" justifyContent="center">
        {loading ? (
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
              const title = d.toLocaleDateString("en-US", { year: "numeric", month: "long" });
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