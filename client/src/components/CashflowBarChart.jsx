import { useEffect, useMemo, useState } from "react";
import { ResponsiveBar } from "@nivo/bar";
import {
  Box, Typography, CircularProgress, Select, MenuItem, Button, Tooltip, IconButton
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
    default: return { from: null, to: null };
  }
};

const CashFlowBarChart = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  // period controls (like your guide)
  const now = new Date();
  const defaultYear = now.getFullYear();
  const [periodMode, setPeriodMode] = useState("overall"); // overall | quarterly | yearly
  const [selectedQuarter, setSelectedQuarter] = useState("Q1");
  const [selectedYear, setSelectedYear] = useState(defaultYear);
  const yearOptions = Array.from({ length: 7 }, (_, i) => defaultYear - 3 + i);

  // 👇 add near the top of CashFlowBarChart component
  const SERIES = [
    { key: "inflow", label: "Inflow", color: colors.greenAccent[500] },
    { key: "outflow", label: "Outflow", color: colors.blueAccent[500] },
  ];

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
        if (to) params.to = to;
        const { data } = await axiosClient.get("/api/get-cash-flow-data", { params });
        // Expect: [{ date, inflow, outflow, net, cash_on_hand, ... }]
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        console.error("load overall cashflow:", e?.response?.data || e.message);
        setRows([]);
      } finally {
        setLoading(false);
      }
    })();
  }, [from, to]);

  // shape for nivo
  const chartData = useMemo(() => {
    return rows.map(r => ({
      month: r.date,                // indexBy
      inflow: Number(r.inflow || 0),
      outflow: Number(r.outflow || 0),
      net: Number(r.net || 0),
      cash_on_hand: Number(r.cash_on_hand || 0),
    }));
  }, [rows]);

  return (
    <Box backgroundColor={colors.primary[400]} p="20px" paddingBottom={8}>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Box>
            <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
              {loading ? "Loading…" : (chartData.length ? "Overall Cash Flow (Inflow vs Outflow)" : "No Data")}
            </Typography>
            {!loading && (
              <Typography variant="h6" color={colors.grey[300]} mt={0.5}>
                {label} • Bars: monthly inflow vs outflow • Net & cash-on-hand available in tooltips
              </Typography>
            )}
          </Box>
          <Tooltip
            arrow placement="top"
            title={
              <Box sx={{ maxWidth: 360, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">How to read</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Each month shows <b>total inflow</b> and <b>total outflow</b> across all Social Enterprises.
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Hover a bar to see <b>net</b> and <b>cash on hand</b>. Use Period to switch Overall / Quarter / Year.
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
            disabled={loading}
            sx={{
              height: 40, minWidth: 130,
              backgroundColor: loading ? colors.grey[600] : colors.blueAccent[600],
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
              disabled={loading}
              sx={{
                height: 40, minWidth: 100,
                backgroundColor: loading ? colors.grey[600] : colors.blueAccent[600],
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
              disabled={loading}
              sx={{
                height: 40, minWidth: 110,
                backgroundColor: loading ? colors.grey[600] : colors.blueAccent[600],
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
      <Box height="400px" display="flex" alignItems="center" justifyContent="center">
        {loading ? (
          <Typography color={colors.grey[300]}>Loading…</Typography>
        ) : chartData.length === 0 ? (
          <Typography color={colors.grey[300]}>No data to display.</Typography>
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
                catch { return v?.slice(0, 7) ?? v; }
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
            // make labels readable against bar color
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
                anchor: "bottom",          // ⬅️ put it at the bottom
                direction: "row",
                justify: false,
                translateX: 0,
                translateY: 100,            // ⬅️ push it below the axis ticks
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
      </Box>
    </Box>
  );
};

export default CashFlowBarChart;