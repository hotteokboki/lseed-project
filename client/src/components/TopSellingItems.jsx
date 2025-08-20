// src/components/TopSellingItemsPie.jsx
import { useEffect, useMemo, useState } from "react";
import { ResponsivePie } from "@nivo/pie";
import {
  Box, Typography, Select, MenuItem, Button, Tooltip, IconButton, CircularProgress
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { useTheme } from "@mui/material";
import { tokens } from "../theme";
import axiosClient from "../api/axiosClient";

const PAGE_SIZE = 5;

const quarterRange = (year, q) => {
  switch (q) {
    case "Q1": return { from: `${year}-01-01`, to: `${year}-04-01` };
    case "Q2": return { from: `${year}-04-01`, to: `${year}-07-01` };
    case "Q3": return { from: `${year}-07-01`, to: `${year}-10-01` };
    case "Q4": return { from: `${year}-10-01`, to: `${Number(year)+1}-01-01` };
    default:   return { from: null, to: null };
  }
};

const TopSellingItemsPie = () => {
  const theme  = useTheme();
  const colors = tokens(theme.palette.mode);

  // period controls
  const now = new Date();
  const defaultYear = now.getFullYear();
  const [periodMode, setPeriodMode] = useState("overall");
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

  // metric / showAll / paging
  const [metric, setMetric] = useState("value"); // "value" | "qty"
  const [showAll, setShowAll] = useState(false);
  const [page, setPage] = useState(0);

  // data
  const [loading, setLoading] = useState(true);
  const [rows, setRows]       = useState([]);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const params = { metric };
        if (from) params.from = from;
        if (to)   params.to   = to;
        const { data } = await axiosClient.get("/api/get-top-items-overall", { params });
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        console.error("load top items:", e?.response?.data || e.message);
        setRows([]);
      } finally {
        setLoading(false);
      }
    })();
  }, [from, to, metric]);

  // reset page when data/toggles change
  useEffect(() => { setPage(0); }, [rows, showAll, metric, from, to]);

  // pagination
  const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  const sliceForPage = (arr, p) => arr.slice(p * PAGE_SIZE, p * PAGE_SIZE + PAGE_SIZE);

  // Top 5 by default; when showAll, paginate 5 per page
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

  return (
    <Box backgroundColor={colors.primary[400]} p="20px">
      {/* Header + controls */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Box display="flex" alignItems="center">
          <Box>
            <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]}>
              {loading ? "Loading…" : (pieData.length ? "Top-Selling Items (Overall)" : "No Data")}
            </Typography>
            {!loading && (
              <Typography variant="h6" color={colors.grey[300]} mt={0.5}>
                {label} • {showAll ? `All items (page ${page + 1}/${totalPages})` : "Top 5"} • Ranked by {metric === "qty" ? "quantity moved" : "estimated value moved"}
              </Typography>
            )}
          </Box>
          <Tooltip
            arrow placement="top"
            title={
              <Box sx={{ maxWidth: 360, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">How this is computed</Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Uses inventory movement as a proxy for sales: <i>moved_qty = max(begin_qty − final_qty, 0)</i>.
                  Value = moved_qty × item unit price.
                </Typography>
              </Box>
            }
          >
            <IconButton sx={{ ml: 1, color: colors.grey[300] }}>
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Box>

        <Box display="flex" gap={1} alignItems="center">
          {/* Period */}
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

          {/* Metric */}
          <Select
            value={metric}
            onChange={(e) => setMetric(e.target.value)}
            sx={{
              height: 40, minWidth: 160,
              backgroundColor: colors.blueAccent[600],
              color: colors.grey[100], fontWeight: "bold",
              "& .MuiSelect-icon": { color: colors.grey[100] },
              "& fieldset": { border: "none" },
            }}
          >
            <MenuItem value="value">By Value (₱)</MenuItem>
            <MenuItem value="qty">By Quantity</MenuItem>
          </Select>

          {/* Toggle Top5 / All */}
          <Button
            variant="outlined"
            onClick={() => setShowAll(s => !s)}
            sx={{
              height: 40, minWidth: 130,
              bordercolor: colors.grey[100],
              backgroundColor: colors.blueAccent[600],
              color: colors.grey[100],
              fontWeight: "bold",
              "&:hover": { backgroundColor: colors.blueAccent[700] },
            }}
          >
            {showAll ? "Show Top 5" : "Show All"}
          </Button>
        </Box>
      </Box>

      {/* Chart + pager */}
      <Box height="420px" display="flex" alignItems="center">
        {/* Prev button (only when showing all and not loading) */}
        {showAll && !loading && (
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

        <Box flexGrow={1} minWidth={0} height="100%" display="flex" alignItems="center" justifyContent="center">
          {loading ? (
            <CircularProgress sx={{ color: colors.greenAccent[500] }} />
          ) : pieData.length === 0 ? (
            <Typography color={colors.grey[300]}>No data to display.</Typography>
          ) : (
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
                const v  = Number(datum?.value || 0);
                const r  = datum?.data?.raw || {};
                return (
                  <div style={{
                    background: theme.palette.mode === "dark" ? "#333" : "#fff",
                    color: theme.palette.mode === "dark" ? "#fff" : "#333",
                    padding: "10px",
                    borderRadius: "6px",
                    boxShadow: "0 2px 6px rgba(0,0,0,0.2)"
                  }}>
                    <strong>{id}</strong><br/>
                    {metric === "qty" ? (
                      <>Moved Qty: <b>{v.toLocaleString()}</b><br/>
                        Unit price: ₱{Number(r.unit_price || 0).toLocaleString()}</>
                    ) : (
                      <>Value Moved: <b>₱{v.toLocaleString()}</b><br/>
                        Qty: {Number(r.moved_qty || 0).toLocaleString()}</>
                    )}
                  </div>
                );
              }}
            />
          )}
        </Box>

        {/* Next button */}
        {showAll && !loading && (
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
