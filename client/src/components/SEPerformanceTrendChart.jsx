import { useState, useEffect } from "react";
import {
  Box,
  Typography,
  Select,
  MenuItem,
  Tooltip,
  IconButton,
  Button,
  CircularProgress,
} from "@mui/material";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import LineChart from "./LineChart";
import { useTheme } from "@mui/material";
import { tokens } from "../theme";
import { useAuth } from "../context/authContext";
import axiosClient from "../api/axiosClient";

const SEPerformanceTrendChart = ({ selectedSEId = null }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [topPerformers, setTopPerformers] = useState([]);
  const [period, setPeriod] = useState("overall");
  const [topPerformer, setTopPerformer] = useState(null);
  const [showAll, setShowAll] = useState(false);
  const [currentPage, setCurrentPage] = useState(0); // for pagination
  const [loading, setLoading] = useState(true); // Add loading state
  const SEsPerPage = 5;
  const { user } = useAuth();
  const isCoordinator = user?.roles?.includes("LSEED-Coordinator");

  useEffect(() => {
    const fetchTopPerformers = async () => {
      setLoading(true); // Start loading
      try {
        let response;

        if (isCoordinator) {
          const res = await axiosClient.get(`/api/get-program-coordinator`, {});

          const data = res.data;
          const program = data[0]?.name;
          response = await axiosClient.get(
            `/api/top-se-performance?period=${period}&program=${program}`
          );
        } else {
          response = await axiosClient.get(
            `/api/top-se-performance?period=${period}&se_id=${selectedSEId}`
          );
        }

        const data = response.data;
        const formattedData = Array.isArray(data) ? data : [];

        setTopPerformers(formattedData);

        // Determine top performer dynamically for selected period
        if (formattedData.length > 0) {
          const averageRatings = formattedData.reduce(
            (acc, { social_enterprise, avg_rating }) => {
              if (!acc[social_enterprise]) {
                acc[social_enterprise] = { total: 0, count: 0 };
              }
              acc[social_enterprise].total += parseFloat(avg_rating);
              acc[social_enterprise].count += 1;
              return acc;
            },
            {}
          );

          const topSE = Object.entries(averageRatings).reduce(
            (top, [seName, { total, count }]) => {
              const avg = total / count;
              return !top || avg > top.avg ? { name: seName, avg } : top;
            },
            null
          );
          setTopPerformer(topSE ? topSE.name : null);
        } else {
          setTopPerformer(null);
        }
      } catch (error) {
        console.error("Error fetching top SE performance:", error);
        setTopPerformers([]);
        setTopPerformer(null);
      } finally {
        setLoading(false); // End loading
      }
    };
    fetchTopPerformers();
  }, [period]); // Only depends on period

  const formatChartData = (dataSlice, fullData) => {
    if (!dataSlice.length) return [];

    const groupedData = {};
    const colorList = [
      colors.blueAccent[500],
      colors.greenAccent[500],
      colors.redAccent[500],
      colors.primary[500],
      colors.grey[500],
    ];

    // 1. Extract all unique quarters
    const allPeriodsSet = new Set();
    fullData.forEach(({ quarter_start }) => {
      const date = new Date(quarter_start);
      const periodLabel = `${date.getFullYear()}-Q${
        Math.floor(date.getMonth() / 3) + 1
      }`;
      allPeriodsSet.add(periodLabel);
    });

    const allPeriods = Array.from(allPeriodsSet).sort((a, b) => {
      const [aYear, aQ] = a.split("-Q").map(Number);
      const [bYear, bQ] = b.split("-Q").map(Number);
      if (aYear !== bYear) return aYear - bYear;
      return aQ - bQ;
    });

    // 2. Group ratings by SE and quarter
    fullData.forEach(({ social_enterprise, quarter_start, avg_rating }) => {
      const date = new Date(quarter_start);
      const periodLabel = `${date.getFullYear()}-Q${
        Math.floor(date.getMonth() / 3) + 1
      }`;

      if (!groupedData[social_enterprise]) {
        groupedData[social_enterprise] = {};
      }
      groupedData[social_enterprise][periodLabel] = parseFloat(avg_rating);
    });

    // Get unique SEs in dataSlice
    const uniqueSEs = [...new Set(dataSlice.map((d) => d.social_enterprise))];

    // Get SE ranking order from fullData
    const fullSEOrder = [...new Set(fullData.map((d) => d.social_enterprise))];

    // Sort SEs based on rank order in fullData (ascending)
    const sortedSEs = [...uniqueSEs].sort((a, b) => {
      return fullSEOrder.indexOf(b) - fullSEOrder.indexOf(a);
    });

    const truncateName = (name, maxLength = 18) => {
      return name.length > maxLength
        ? name.slice(0, maxLength - 3) + "…"
        : name;
    };

    // Map sorted SEs to chart data
    return sortedSEs.map((seName, index) => {
      const rankIndex = fullSEOrder.findIndex((se) => se === seName) + 1;

      const seData = allPeriods.map((period) => ({
        x: period,
        y: groupedData[seName]?.[period] ?? 0,
      }));

      return {
        id: truncateName(`${rankIndex}. ${seName}`),
        color: colorList[index % colorList.length],
        data: seData,
      };
    });
  };

  const groupBySE = topPerformers.reduce((acc, item) => {
    if (!acc[item.social_enterprise]) {
      acc[item.social_enterprise] = [];
    }
    acc[item.social_enterprise].push(item);
    return acc;
  }, {});

  const seGroups = Object.values(groupBySE); // Array of arrays, one per SE

  const paginatedSEGroups = showAll
    ? seGroups.slice(currentPage * SEsPerPage, (currentPage + 1) * SEsPerPage)
    : seGroups.slice(0, SEsPerPage);

  const paginatedRows = paginatedSEGroups.flat();

  const chartData = formatChartData(paginatedRows, topPerformers);

  return (
    <Box
      gridColumn="span 12"
      gridRow="span 2"
      backgroundColor={colors.primary[400]}
      p="20px"
    >
      {/* Container for title and dropdown */}
      <Box
        display="flex"
        justifyContent="space-between"
        alignItems="center"
        mb={2}
      >
        {/* Left: Chart Title + Top Performer + Tooltip */}
        <Box display="flex" alignItems="center">
          <Box>
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
            >
              {loading
                ? "Loading..."
                : chartData.length === 0
                ? "No Data"
                : `SE Performance Trend (${showAll ? "All" : "Top 5"})`}
            </Typography>

            {!loading && topPerformer && (
              <Typography
                variant="h5"
                fontWeight="bold"
                color={colors.blueAccent[500]}
                mt={1}
              >
                {chartData.length === 0
                  ? "No Data"
                  : `Top Performer (${period}): ${topPerformer}`}
              </Typography>
            )}
          </Box>

          {/* Tooltip Icon */}
          <Tooltip
            title={
              <Box sx={{ maxWidth: 320, p: 1 }}>
                <Typography variant="body1" fontWeight="bold">
                  How to Read the SE Performance Trend 📈
                </Typography>

                <Typography variant="body2" sx={{ mt: 1 }}>
                  This chart visualizes how{" "}
                  <strong>Social Enterprises (SEs)</strong> perform across
                  quarters based on their mentor evaluation ratings.
                </Typography>

                <Box sx={{ mt: 1 }}>
                  <Typography variant="body2">
                    🔹{" "}
                    <strong style={{ color: colors.greenAccent[500] }}>
                      Rising Line
                    </strong>{" "}
                    – The SE's average ratings have improved over time.
                  </Typography>
                  <Typography variant="body2" sx={{ mt: 0.5 }}>
                    ⏸️{" "}
                    <strong style={{ color: colors.grey[300] }}>
                      Flat Line
                    </strong>{" "}
                    – Performance remained stable across periods.
                  </Typography>
                  <Typography variant="body2" sx={{ mt: 0.5 }}>
                    🔻{" "}
                    <strong style={{ color: "#f44336" }}>Falling Line</strong> –
                    Ratings have declined, indicating challenges or lack of
                    progress.
                  </Typography>
                </Box>

                <Typography variant="body2" sx={{ mt: 2 }}>
                  The performance is based on{" "}
                  <strong>average star ratings</strong> from mentor evaluations
                  across key categories like Finance, Marketing, Logistics, and
                  others.
                </Typography>

                <Typography variant="body2" sx={{ mt: 1 }}>
                  For each SE:
                </Typography>
                <Box sx={{ pl: 1 }}>
                  <Typography variant="body2">
                    • A <strong>weighted average score</strong> is calculated
                    using <em>average × number of evaluations</em>.
                  </Typography>
                  <Typography variant="body2">
                    • Only the <strong>Top 3 SEs</strong> (highest weighted
                    average) are shown.
                  </Typography>
                </Box>

                <Typography variant="body2" sx={{ mt: 2 }}>
                  Switch between <strong>Overall</strong>,{" "}
                  <strong>Quarterly</strong>, and <strong>Yearly</strong> to
                  compare performance over different time periods.
                </Typography>
              </Box>
            }
            arrow
            placement="top"
          >
            <IconButton sx={{ ml: 1, color: colors.grey[300] }}>
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Box>

        {/* Right side - Period Select + Show All */}
        <Box display="flex" alignItems="center" gap={1}>
          <Select
            value={period}
            onChange={(e) => setPeriod(e.target.value)}
            disabled={loading} // Disable during loading
            sx={{
              height: "40px", // Match button height
              minWidth: 120, // Match button width
              backgroundColor: loading
                ? colors.grey[600]
                : colors.blueAccent[600],
              color: colors.grey[100],
              bordercolor: colors.grey[100],
              fontWeight: "bold",
              "& .MuiSelect-icon": {
                color: colors.grey[100], // dropdown arrow color
              },
              "& fieldset": {
                border: "none", // remove default border
              },
            }}
          >
            <MenuItem value="overall">Overall</MenuItem>
            <MenuItem value="quarterly">Quarterly</MenuItem>
            <MenuItem value="yearly">Yearly</MenuItem>
          </Select>

          <Button
            variant="outlined"
            onClick={() => {
              setShowAll((prev) => !prev);
              setCurrentPage(0); // reset page when toggling
            }}
            disabled={loading} // Disable during loading
            sx={{
              height: "40px", // match Select height
              minWidth: 120, // match Select width
              bordercolor: colors.grey[100],
              backgroundColor: loading
                ? colors.grey[600]
                : colors.blueAccent[600],
              color: colors.grey[100],
              fontWeight: "bold",
              "&:hover": {
                backgroundColor: loading
                  ? colors.grey[600]
                  : colors.blueAccent[700],
              },
              "&:disabled": {
                backgroundColor: colors.grey[600],
                color: colors.grey[300],
              },
            }}
          >
            {showAll ? "Show Top 5" : "Show All"}
          </Button>
        </Box>
      </Box>

      <Box height="320px" display="flex" alignItems="center">
        {/* Prev Button - Only when showAll and not loading */}
        {showAll && !loading && (
          <Button
            variant="contained"
            color="secondary"
            disabled={currentPage === 0}
            onClick={() => setCurrentPage((prev) => prev - 1)}
            sx={{
              mx: 2,
              height: "fit-content",
              backgroundColor: colors.blueAccent[600],
              color: colors.grey[100],
              "&:disabled": {
                backgroundColor: colors.grey[600],
                color: colors.grey[300],
              },
            }}
          >
            ◀ Prev
          </Button>
        )}

        {/* Chart or Loading */}
        <Box
          flexGrow={1}
          minWidth={0}
          overflow="hidden"
          display="flex"
          justifyContent="center"
          alignItems="center"
          height="100%"
          sx={{
            pl: showAll && !loading ? 0 : 2,
            pr: showAll && !loading ? 0 : 2,
          }}
        >
          {loading ? (
            <Box
              display="flex"
              flexDirection="column"
              alignItems="center"
              gap={2}
            >
              <CircularProgress
                size={60}
                sx={{
                  color: colors.greenAccent[500],
                  "& .MuiCircularProgress-circle": {
                    strokeLinecap: "round",
                  },
                }}
              />
              <Typography
                variant="h6"
                color={colors.grey[300]}
                textAlign="center"
              >
                Loading chart data...
              </Typography>
            </Box>
          ) : chartData.length === 0 ? (
            <Typography
              variant="h6"
              color={colors.grey[300]}
              textAlign="center"
            >
              No data available for plotting.
            </Typography>
          ) : (
            <LineChart data={chartData} />
          )}
        </Box>

        {/* Next Button - Only when showAll and not loading */}
        {showAll && !loading && (
          <Button
            variant="contained"
            color="secondary"
            disabled={(currentPage + 1) * SEsPerPage >= seGroups.length}
            onClick={() => setCurrentPage((prev) => prev + 1)}
            sx={{
              mx: 2,
              height: "fit-content",
              backgroundColor: colors.blueAccent[600],
              color: colors.grey[100],
              "&:disabled": {
                backgroundColor: colors.grey[600],
                color: colors.grey[300],
              },
            }}
          >
            Next ▶
          </Button>
        )}
      </Box>
    </Box>
  );
};

export default SEPerformanceTrendChart;
