// src/components/FinanceRiskHeatmap.jsx
import { ResponsiveHeatMap } from "@nivo/heatmap";
import { useTheme } from "@mui/material";
import { tokens } from "../theme";
import { useState, useEffect, useMemo } from "react";
import { Box, Select, MenuItem, Typography, Tooltip, IconButton, Button } from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import axiosClient from "../api/axiosClient";

const SEsPerPage = 10;

const FinanceRiskHeatmap = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  const [period, setPeriod] = useState("overall"); // overall | quarterly | yearly
  const [quarter, setQuarter] = useState("Q1");
  const [year, setYear] = useState(new Date().getFullYear());
  const yearOptions = Array.from({ length: 7 }, (_, i) => year - 3 + i);

  const [rows, setRows] = useState([]);
  const [selectedSE, setSelectedSE] = useState("");
  const [page, setPage] = useState(0);

  // load
  useEffect(() => {
    (async () => {
      try {
        const params = { period };
        if (period === "yearly") { params.year = year; }
        if (period === "quarterly") { params.year = year; params.quarter = quarter; }
        const { data } = await axiosClient.get("/api/finance-risk-heatmap", { params });
        setRows(Array.isArray(data) ? data : []);
        setPage(0);
      } catch (e) {
        console.error("load finance heatmap:", e?.response?.data || e.message);
        setRows([]);
      }
    })();
  }, [period, quarter, year]);

  const uniqueSEs = useMemo(
    () => Array.from(new Set(rows.map(r => r.team_name))).sort(),
    [rows]
  );

  const filtered = selectedSE ? rows.filter(r => r.team_name === selectedSE) : rows;

  // paginate by 10 SEs
  const paged = useMemo(() => {
    const start = page * SEsPerPage;
    return filtered.slice(start, start + SEsPerPage);
  }, [filtered, page]);

  // transform for nivo
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

  // color rule same as your existing heatmap (1..5)
  const cellColor = (val) => {
    if (val <= 1.5) return theme.palette.mode === "dark" ? colors.redAccent[300] : colors.redAccent[500];
    if (val <= 3.0) return theme.palette.mode === "dark" ? colors.primary[300]  : colors.grey[700];
    return theme.palette.mode === "dark" ? colors.greenAccent[300] : colors.greenAccent[500];
  };

  return (
    <Box>
      {/* Header & controls */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
            Financial Risk Heatmap
          </Typography>
          <Tooltip
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
          </Tooltip>
        </Box>

        <Box display="flex" alignItems="center" gap={1}>
          <Select value={period} onChange={(e)=>setPeriod(e.target.value)} sx={{
            height: 40, minWidth: 130, backgroundColor: colors.blueAccent[600], color: colors.grey[100], fontWeight:"bold",
            "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
          }}>
            <MenuItem value="overall">Overall</MenuItem>
            <MenuItem value="quarterly">Quarterly</MenuItem>
            <MenuItem value="yearly">Yearly</MenuItem>
          </Select>

          {period === "quarterly" && (
            <Select value={quarter} onChange={(e)=>setQuarter(e.target.value)} sx={{
              height: 40, minWidth: 100, backgroundColor: colors.blueAccent[600], color: colors.grey[100], fontWeight:"bold",
              "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
            }}>
              <MenuItem value="Q1">Q1</MenuItem><MenuItem value="Q2">Q2</MenuItem>
              <MenuItem value="Q3">Q3</MenuItem><MenuItem value="Q4">Q4</MenuItem>
            </Select>
          )}

          {(period === "quarterly" || period === "yearly") && (
            <Select value={year} onChange={(e)=>setYear(e.target.value)} sx={{
              height:40, minWidth:110, backgroundColor: colors.blueAccent[600], color: colors.grey[100], fontWeight:"bold",
              "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
            }}>
              {yearOptions.map(y => <MenuItem key={y} value={y}>{y}</MenuItem>)}
            </Select>
          )}

          <Select
            value={selectedSE}
            onChange={(e)=>{ setSelectedSE(e.target.value); setPage(0); }}
            displayEmpty
            sx={{
              height: 40, minWidth: 220, backgroundColor: colors.blueAccent[600], color: colors.grey[100], fontWeight:"bold",
              "& .MuiSelect-icon": { color: colors.grey[100] }, "& fieldset": { border: "none" },
            }}
          >
            <MenuItem value="">All SEs</MenuItem>
            {uniqueSEs.map(se => <MenuItem key={se} value={se}>{se}</MenuItem>)}
          </Select>
        </Box>
      </Box>

      {/* Heatmap + pager */}
      <div style={{ height: 540, display:"flex", flexDirection:"column", alignItems:"center" }}>
        <div style={{ height: 480, width: "100%" }}>
          <ResponsiveHeatMap
            data={data}
            valueFormat=">-.2f"
            margin={{ top: 60, right: 40, bottom: 70, left: 180 }}
            axisTop={{
              tickSize: 5, tickPadding: 5,
              legend: "Financial Indicators", legendOffset: -50, legendPosition: "middle",
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
        </div>

        {/* Pager or Clear Filter */}
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
            >Clear Filter</Button>
          )}
        </div>

        {/* Color legend (same style as your heatmap) */}
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