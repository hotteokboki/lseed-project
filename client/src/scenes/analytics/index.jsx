import {
  Box,
  useTheme,
  Typography,
  Tooltip,
  IconButton,
  Button,
  MenuItem,
  Menu,
} from "@mui/material";
import Header from "../../components/Header";
import HorizontalBarChart from "../../components/HorizontalBarChart";
import DualAxisLineChart from "../../components/DualAxisLineChart";
import StatBox from "../../components/StatBox";
import LeaderboardChart from "../../components/LeaderboardChart";
import SEPerformanceTrendChart from "../../components/SEPerformanceTrendChart";
import BarChart from "../../components/BarChart";
import EmailIcon from "@mui/icons-material/Email";
import PointOfSaleIcon from "@mui/icons-material/PointOfSale";
import PersonAddIcon from "@mui/icons-material/PersonAdd";
import StoreIcon from "@mui/icons-material/Store";
import GroupIcon from "@mui/icons-material/Group";
import PersonRemoveIcon from "@mui/icons-material/PersonRemove";
import TrendingUpIcon from "@mui/icons-material/TrendingUp";
import TrafficIcon from "@mui/icons-material/Traffic";
import HeatmapWrapper from "../../components/MyHeatMap";
import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { tokens } from "../../theme";
import { useEffect, useState, useRef, useMemo } from "react";
import { useAuth } from "../../context/authContext";
import { useParams, useNavigate } from "react-router-dom";
import DownloadIcon from "@mui/icons-material/Download";
import RadarChart from "../../components/RadarChart";
import axiosClient from "../../api/axiosClient";

const Analytics = ({}) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [stats, setStats] = useState(null);
  const [leaderboardData, setLeaderboardData] = useState([]);
  const [showAll, setShowAll] = useState(false);
  const { user } = useAuth();
  const { id } = useParams(); // Extract the `id` from the URL
  const isLSEEDCoordinator = user?.roles?.includes("LSEED-Coordinator");
  const [currentPage, setCurrentPage] = useState(0);
  const SEsPerPage = 10;
  const [selectedSEId, setSelectedSEId] = useState(id); // State to manage selected SE

  const performanceOverviewChart = useRef(null);
  const painPointsChart = useRef(null);
  const scoreDistributionChart = useRef(null);
  const revenueVSexpensesChart = useRef(null);
  const cashFlowAnalysisChart = useRef(null);
  const equityChart = useRef(null);

  const [inventoryData, setInventoryData] = useState([]);
  const [cashFlowRaw, setCashFlowRaw] = useState([]);
  const [isExporting, setIsExporting] = useState(false);
  const [selectedSE, setSelectedSE] = useState(null); // Selected social enterprise object
  const [anchorEl, setAnchorEl] = useState(null);

  const [overallRadarData, setOverallRadarData] = useState([]);
  const [overallCategoryStats, setOverallCategoryStats] = useState([]);
  const overallRadarChart = useRef(null);
  const [isDataLoaded, setIsDataLoaded] = useState(false);

  const open = Boolean(anchorEl);

  const currentSEFinancialMetrics = {
    totalRevenue: 0,
    totalExpenses: 0,
    netIncome: 0,
    totalAssets: 0,
    totalLiabilities: 0,
    ownerEquity: 0,
    revenueVsExpenses: [],
    equityTrend: [],
  };

  const filteredInventoryData = inventoryData.filter(
    (item) => item.se_abbr === selectedSE?.abbr
  );

  const selectedSECashFlowQuarterly = useMemo(() => {
    if (!selectedSEId || !Array.isArray(cashFlowRaw)) return [];

    const filtered = cashFlowRaw.filter((item) => item.se_id === selectedSEId);
    const quarterBuckets = {};

    filtered.forEach((item) => {
      if (!item?.date) return;
      const date = new Date(item.date);
      const quarter = getQuarterLabel(date);

      if (!quarterBuckets[quarter]) {
        quarterBuckets[quarter] = { inflows: [], outflows: [] };
      }

      quarterBuckets[quarter].inflows.push(Number(item.inflow) || 0);
      quarterBuckets[quarter].outflows.push(Number(item.outflow) || 0);
    });

    const inflowData = [];
    const outflowData = [];

    Object.entries(quarterBuckets).forEach(
      ([quarter, { inflows, outflows }]) => {
        const avgInflow = inflows.length
          ? Math.round(inflows.reduce((sum, v) => sum + v, 0) / inflows.length)
          : 0;

        const avgOutflow = outflows.length
          ? Math.round(
              outflows.reduce((sum, v) => sum + v, 0) / outflows.length
            )
          : 0;

        inflowData.push({ x: quarter, y: avgInflow });
        outflowData.push({ x: quarter, y: avgOutflow });
      }
    );

    return [
      { id: "Inflow", data: inflowData },
      { id: "Outflow", data: outflowData },
    ];
  }, [cashFlowRaw, selectedSEId]);

  const getQuarterLabel = (date) => {
    const month = date.getMonth();
    const year = date.getFullYear();
    if (month >= 0 && month <= 2) return `Q1 ${year}`;
    if (month >= 3 && month <= 5) return `Q2 ${year}`;
    if (month >= 6 && month <= 8) return `Q3 ${year}`;
    return `Q4 ${year}`;
  };

  const allItemsInventoryTurnover = {};
  filteredInventoryData.forEach(({ item_name, qty, price, amount }) => {
    const priceNum = Number(price);
    const qtyNum = Number(qty);
    const totalValue = qtyNum * priceNum; // This is average inventory value for the item

    if (!allItemsInventoryTurnover[item_name]) {
      allItemsInventoryTurnover[item_name] = {
        totalCOGS: 0,
        totalInventoryValue: 0,
      };
    }
    allItemsInventoryTurnover[item_name].totalCOGS += Number(amount); // Sum of 'amount' as COGS
    allItemsInventoryTurnover[item_name].totalInventoryValue += totalValue; // Sum of inventory value
  });

  const netProfitMargin = currentSEFinancialMetrics.totalRevenue
    ? (
        (currentSEFinancialMetrics.netIncome /
          currentSEFinancialMetrics.totalRevenue) *
        100
      ).toFixed(2)
    : "0.00";
  const grossProfitMargin = currentSEFinancialMetrics.totalRevenue
    ? (
        ((currentSEFinancialMetrics.totalRevenue -
          currentSEFinancialMetrics.totalExpenses) /
          currentSEFinancialMetrics.totalRevenue) *
        100
      ).toFixed(2)
    : "0.00";
  const debtToAssetRatio = currentSEFinancialMetrics.totalAssets
    ? (
        currentSEFinancialMetrics.totalLiabilities /
        currentSEFinancialMetrics.totalAssets
      ).toFixed(2)
    : "0.00";

  const inventoryTurnoverByItemData = Object.entries(allItemsInventoryTurnover)
    .map(([itemName, data]) => {
      const cogs = data.totalCOGS;
      const avgInventory = data.totalInventoryValue; // Using total inventory value as avg for simplicity
      const turnover =
        avgInventory === 0 ? 0 : parseFloat((cogs / avgInventory).toFixed(2));
      return { name: itemName, turnover };
    })
    .sort((a, b) => b.turnover - a.turnover)
    .slice(0, 5); // Top 5 items by turnover

  const selectedSEEquityTrendData = useMemo(() => {
    if (!currentSEFinancialMetrics?.equityTrend?.length) return [];

    const quarterBuckets = {};

    currentSEFinancialMetrics.equityTrend.forEach(({ x, y }) => {
      const date = new Date(x);
      const quarter = getQuarterLabel(date);

      if (!quarterBuckets[quarter]) {
        quarterBuckets[quarter] = [];
      }

      quarterBuckets[quarter].push(Number(y) || 0);
    });

    const formattedData = Object.entries(quarterBuckets).map(
      ([quarter, values]) => {
        const avg = values.reduce((sum, val) => sum + val, 0) / values.length;
        return {
          x: quarter,
          y: Math.round(avg),
        };
      }
    );

    return [
      {
        id: "Equity",
        color: colors.blueAccent[500],
        data: formattedData,
      },
    ];
  }, [currentSEFinancialMetrics]);

  const transformedCashFlowData = useMemo(() => {
    const inflowMap = new Map();
    const outflowMap = new Map();

    selectedSECashFlowQuarterly.forEach((entry) => {
      if (entry.id === "Inflow") {
        entry.data.forEach(({ x, y }) => inflowMap.set(x, y));
      } else if (entry.id === "Outflow") {
        entry.data.forEach(({ x, y }) => outflowMap.set(x, y));
      }
    });

    const allQuarters = new Set([...inflowMap.keys(), ...outflowMap.keys()]);

    return Array.from(allQuarters).map((quarter) => ({
      quarter,
      Inflow: inflowMap.get(quarter) || 0,
      Outflow: outflowMap.get(quarter) || 0,
    }));
  }, [selectedSECashFlowQuarterly]);

  const selectedSERevenueVsExpensesData = useMemo(() => {
    if (!currentSEFinancialMetrics?.revenueVsExpenses?.length) return [];

    const quarterBuckets = {};

    currentSEFinancialMetrics.revenueVsExpenses.forEach(
      ({ x, revenue, expenses }) => {
        const date = new Date(x); // Ensure x is parsed as a Date
        const quarter = getQuarterLabel(date);

        if (!quarterBuckets[quarter]) {
          quarterBuckets[quarter] = { revenues: [], expenses: [] };
        }

        quarterBuckets[quarter].revenues.push(Number(revenue) || 0);
        quarterBuckets[quarter].expenses.push(Number(expenses) || 0);
      }
    );

    const revenueData = [];
    const expenseData = [];

    Object.entries(quarterBuckets).forEach(
      ([quarter, { revenues, expenses }]) => {
        const avgRevenue = revenues.length
          ? Math.round(
              revenues.reduce((sum, val) => sum + val, 0) / revenues.length
            )
          : null;

        const avgExpense = expenses.length
          ? Math.round(
              expenses.reduce((sum, val) => sum + val, 0) / expenses.length
            )
          : null;

        revenueData.push({ x: quarter, y: avgRevenue });
        expenseData.push({ x: quarter, y: avgExpense });
      }
    );

    return [
      {
        id: "Revenue",
        color: colors.greenAccent[500],
        data: revenueData,
      },
      {
        id: "Expenses",
        color: colors.redAccent[500],
        data: expenseData,
      },
    ];
  }, [currentSEFinancialMetrics]);

  const handleDownloadStakeholderReport = () => {
    setIsExporting(true);

    setTimeout(async () => {
      const revenueSVG = revenueVSexpensesChart.current?.querySelector("svg");
      const cashFlowSVG = cashFlowAnalysisChart.current?.querySelector("svg");
      const equitySVG = equityChart.current?.querySelector("svg");

      if (
        !revenueSVG ||
        !selectedSEId ||
        !currentSEFinancialMetrics ||
        !cashFlowSVG ||
        !equitySVG
      ) {
        setIsExporting(false);
        return alert("Revenue chart or data not found");
      }

      const serialize = (svg) => new XMLSerializer().serializeToString(svg);

      const svgToBase64 = async (svgData, bbox) => {
        const scale = 3;
        const canvas = document.createElement("canvas");
        canvas.width = bbox.width * scale;
        canvas.height = bbox.height * scale;
        const ctx = canvas.getContext("2d");
        ctx.scale(scale, scale); // upscale before drawing

        const img = new Image();
        const blob = new Blob([svgData], {
          type: "image/svg+xml;charset=utf-8",
        });
        const url = URL.createObjectURL(blob);

        return new Promise((resolve) => {
          img.onload = () => {
            ctx.drawImage(img, 0, 0);
            URL.revokeObjectURL(url);
            resolve(canvas.toDataURL("image/png"));
          };
          img.src = url;
        });
      };

      try {
        const revenueSVGData = serialize(revenueSVG);
        const cashFlowSVGData = serialize(cashFlowSVG);
        const equitySVGData = serialize(equitySVG);

        const bbox = revenueSVG.getBoundingClientRect();
        const cashFlowSVGBBox = cashFlowSVG.getBoundingClientRect();
        const equitySVGBBox = equitySVG.getBoundingClientRect();

        const chartImageBase64 = await svgToBase64(revenueSVGData, bbox);
        const cashFlowImageBase64 = await svgToBase64(
          cashFlowSVGData,
          cashFlowSVGBBox
        );
        const equityImageBase64 = await svgToBase64(
          equitySVGData,
          equitySVGBBox
        );

        const response = await axiosClient.post(
          `/api/financial-report`,
          {
            chartImage: chartImageBase64,
            cashFlowImage: cashFlowImageBase64,
            equityImage: equityImageBase64,
            selectedSEId,
            totalRevenue: currentSEFinancialMetrics.totalRevenue,
            totalExpenses: currentSEFinancialMetrics.totalExpenses,
            netIncome: currentSEFinancialMetrics.netIncome,
            totalAssets: currentSEFinancialMetrics.totalAssets,
            selectedSERevenueVsExpensesData,
            transformedCashFlowData,
            selectedSEEquityTrendData,
            inventoryTurnoverByItemData,
            netProfitMargin,
            grossProfitMargin,
            debtToAssetRatio,
          },
          {
            responseType: "blob",
          }
        );

        const blobUrl = URL.createObjectURL(
          new Blob([response.data], { type: "application/pdf" })
        );
        const a = document.createElement("a");
        a.href = blobUrl;
        a.download = `Stakeholder_Report_${selectedSE?.abbr || "Report"}.pdf`;
        a.click();
      } catch (err) {
        console.error("❌ Failed to generate stakeholder report:", err);
        alert("Failed to generate report");
      } finally {
        setIsExporting(false);
      }
    }, 100);
  };

  const handleGenerateCollaborationReport = () => {
    setIsExporting(true);

    setTimeout(async () => {
      const radarSVG = performanceOverviewChart.current?.querySelector("svg");
      const pieSVG = painPointsChart.current?.querySelector("svg");
      const likertSVG = scoreDistributionChart.current?.querySelector("svg");

      if (!radarSVG || !pieSVG || !likertSVG) {
        setIsExporting(false);
        return alert("One or both charts not found");
      }

      const serialize = (svg) => new XMLSerializer().serializeToString(svg);

      const svgToBase64 = async (svgData, bbox) => {
        const canvas = document.createElement("canvas");
        canvas.width = bbox.width;
        canvas.height = bbox.height;
        const ctx = canvas.getContext("2d");
        const img = new Image();
        const blob = new Blob([svgData], {
          type: "image/svg+xml;charset=utf-8",
        });
        const url = URL.createObjectURL(blob);

        return new Promise((resolve) => {
          img.onload = () => {
            ctx.drawImage(img, 0, 0);
            URL.revokeObjectURL(url);
            resolve(canvas.toDataURL("image/png"));
          };
          img.src = url;
        });
      };

      try {
        const radarData = serialize(radarSVG);
        const pieData = serialize(pieSVG);
        const likertData = serialize(likertSVG);

        const radarBBox = radarSVG.getBoundingClientRect();
        const pieBBox = pieSVG.getBoundingClientRect();
        const likertBBox = likertSVG.getBoundingClientRect();

        const radarBase64 = await svgToBase64(radarData, radarBBox);
        const pieBase64 = await svgToBase64(pieData, pieBBox);
        const likertBase64 = await svgToBase64(likertData, likertBBox);

        const response = await axiosClient.post(
          `/api/adhoc-report`,
          {
            chartImageRadar: radarBase64,
            chartImagePie: pieBase64,
            scoreDistributionLikert: likertBase64,
            se_id: selectedSE?.id,
            period: "quarterly",
          },
          {
            responseType: "blob",
          }
        );

        const blobUrl = URL.createObjectURL(
          new Blob([response.data], { type: "application/pdf" })
        );
        const a = document.createElement("a");
        a.href = blobUrl;
        a.download = `Adhoc_Report_${selectedSE?.abbr || "Report"}.pdf`;
        a.click();
      } catch (err) {
        console.error("❌ Failed to generate report:", err);
        alert("Failed to generate report");
      } finally {
        setIsExporting(false);
      }
    }, 100);
  };

  const handleGenerateReport = (type) => {
    handleClose();
    if (type === "collaboration") {
      handleGenerateCollaborationReport(); // or handleCollaborationReport()
    } else if (type === "stakeholder") {
      handleDownloadStakeholderReport();
    }
  };

  const handleMenuClick = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  // Update the useEffect that fetches overall data
  useEffect(() => {
    const fetchOverallData = async () => {
      try {
        // Fetch overall radar data (all SEs combined)
        const overallRadarResponse = await axiosClient.get(`/api/overall-radar-data`);
        const overallRadarResult = overallRadarResponse.data;
        setOverallRadarData(overallRadarResult);

        // Fetch overall category statistics
        const overallStatsResponse = await axiosClient.get(`/api/overall-category-stats`);
        const overallStatsResult = overallStatsResponse.data;
        setOverallCategoryStats(overallStatsResult);

        // Mark data as loaded after both requests complete
        setIsDataLoaded(true);
      } catch (error) {
        console.error("Error fetching overall data:", error);
        setIsDataLoaded(false);
      }
    };

    fetchOverallData();
  }, []);

  // Updated handleGenerateOverallEvaluationReport function
  const handleGenerateOverallEvaluationReport = () => {
    // Check if data is loaded first
    if (!isDataLoaded || !overallRadarData.length) {
      alert("Data is still loading. Please wait a moment and try again.");
      return;
    }

    setIsExporting(true);

    // Increase timeout to ensure chart is fully rendered
    setTimeout(async () => {
      const radarSVG = overallRadarChart.current?.querySelector("svg");

      if (!radarSVG) {
        setIsExporting(false);
        console.error("Radar chart elements:", {
          chartRef: overallRadarChart.current,
          svg: radarSVG,
          dataLength: overallRadarData.length,
          isDataLoaded,
        });
        return alert(
          "Radar chart not found. Please ensure the chart has loaded completely."
        );
      }

      const serialize = (svg) => new XMLSerializer().serializeToString(svg);

      const svgToBase64 = async (svgData, bbox) => {
        const scale = 2; // Reduce scale if needed
        const canvas = document.createElement("canvas");
        canvas.width = bbox.width * scale;
        canvas.height = bbox.height * scale;
        const ctx = canvas.getContext("2d");
        ctx.scale(scale, scale);

        const img = new Image();
        const blob = new Blob([svgData], {
          type: "image/svg+xml;charset=utf-8",
        });
        const url = URL.createObjectURL(blob);

        return new Promise((resolve, reject) => {
          img.onload = () => {
            ctx.drawImage(img, 0, 0);
            URL.revokeObjectURL(url);
            resolve(canvas.toDataURL("image/png"));
          };
          img.onerror = () => {
            URL.revokeObjectURL(url);
            reject(new Error("Failed to load image"));
          };
          img.src = url;
        });
      };

      try {
        const radarData = serialize(radarSVG);
        const radarBBox = radarSVG.getBoundingClientRect();

        // Validate bounding box
        if (radarBBox.width === 0 || radarBBox.height === 0) {
          throw new Error("Chart has no dimensions");
        }

        const radarBase64 = await svgToBase64(radarData, radarBBox);

        const response = await axiosClient.post(
          `/api/overall-evaluation-report`,
          {
            chartImageRadar: radarBase64,
            overallCategoryStats: overallCategoryStats,
            overallRadarData: overallRadarData,
          },
          {
            responseType: "blob",
          }
        );

        // Generate filename with current date on frontend
        const currentDate = new Date();
        const dateString = currentDate.toISOString().split("T")[0]; // YYYY-MM-DD
        const filename = `overall_evaluation_report_${dateString}.pdf`;

        const blobUrl = URL.createObjectURL(
          new Blob([response.data], { type: "application/pdf" })
        );
        const a = document.createElement("a");
        a.href = blobUrl;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(blobUrl);
      } catch (err) {
        console.error("❌ Failed to generate overall evaluation report:", err);
        alert(`Failed to generate report: ${err.message}`);
      } finally {
        setIsExporting(false);
      }
    }, 500); // Increased timeout
  };

  // Group data by SE
  const groupBySE = leaderboardData.reduce((acc, item) => {
    if (!acc[item.social_enterprise]) {
      acc[item.social_enterprise] = [];
    }
    acc[item.social_enterprise].push(item);
    return acc;
  }, {});

  const seGroups = Object.values(groupBySE);

  const paginatedData = showAll
    ? seGroups
        .slice(currentPage * SEsPerPage, (currentPage + 1) * SEsPerPage)
        .flat()
    : leaderboardData
        .slice()
        .sort(
          (a, b) =>
            parseFloat(b.overall_weighted_avg_rating) -
            parseFloat(a.overall_weighted_avg_rating)
        )
        .slice(0, 10);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        let response;
        if (isLSEEDCoordinator) {
          const res = await axiosClient.get(
            `/api/get-program-coordinator`,
          );

          const data = res.data;
          const program = data[0]?.name;

          response = await axiosClient.get(
            `/api/analytics-stats?program=${program}`
          );
        } else {
          response = await axiosClient.get(
            `/api/analytics-stats`
          );
        }
        const data = response.data;

        const fullLeaderboard = data.leaderboardData || [];

        // Store everything in state, but only show top 10 initially
        setLeaderboardData(fullLeaderboard);

        setStats(data);
      } catch (error) {
        console.error("Error fetching analytics stats:", error);
        setStats({ heatmapStats: [] }); // Fallback to empty array
      }
    };
    fetchStats();
  }, []);

  if (!stats) return <Typography>Loading...</Typography>;

  return (
    <Box m="20px">
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Header title="Evaluation Analytics" subtitle="Welcome to Evaluation Analytics" />
        {/* Right: Export Button */}
        <Button
          variant="contained"
          color="primary"
          startIcon={<DownloadIcon />}
          onClick={handleGenerateOverallEvaluationReport}
          disabled={isExporting || !isDataLoaded || !overallRadarData.length}
        >
          {isExporting
            ? "Generating..."
            : !isDataLoaded
            ? "Loading Data..."
            : "Generate Overall Evaluation Report"}
        </Button>
      </Box>

      <Box
        sx={{
          position: "fixed",
          top: "-1000px",
          left: "-1000px",
          width: "600px",
          height: "400px",
          visibility: "hidden", // Hide visually but keep in layout
          pointerEvents: "none",
        }}
      >
        <Box ref={overallRadarChart} width="600px" height="400px">
          {isDataLoaded && overallRadarData.length > 0 && (
            <RadarChart
              radarData={overallRadarData}
              isExporting={isExporting}
            />
          )}
        </Box>
      </Box>

      {/* Row 1 - StatBoxes */}
      <Box
        display="flex"
        flexWrap="wrap"
        gap="20px"
        justifyContent="space-between"
        mt="20px"
      >
        {/* SE's Enrolled */}
        <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <StatBox
            title={stats.totalSocialEnterprises}
            subtitle="SE's Enrolled"
            progress={
              stats.totalSocialEnterprises / (stats.previousMonthSECount || 1)
            }
            increase={`${
              stats.previousMonthSECount > 0
                ? (
                    ((stats.totalSocialEnterprises -
                      stats.previousMonthSECount) /
                      stats.previousMonthSECount) *
                    100
                  ).toFixed(2)
                : 0
            }%`}
            icon={
              <EmailIcon
                sx={{ fontSize: "26px", color: colors.greenAccent[500] }} // 🔄 Updated icon
              />
            }
          />
        </Box>

        {/* SE's with Mentors */}
        <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <StatBox
            title={stats.withMentorship}
            subtitle="SE's with Mentors"
            progress={
              stats.withMentorship / (stats.totalSocialEnterprises || 1)
            }
            increase={`${(
              (stats.withMentorship / (stats.totalSocialEnterprises || 1)) *
              100
            ).toFixed(2)}%`}
            icon={
              <GroupIcon
                sx={{ fontSize: "26px", color: colors.blueAccent[500] }} // 🔄 Updated icon
              />
            }
          />
        </Box>

        {/* SE's without Mentors */}
        <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <StatBox
            title={stats.withoutMentorship}
            subtitle="SEs without mentors"
            progress={
              stats.withoutMentorship / (stats.totalSocialEnterprises || 1)
            }
            increase={`${(
              (stats.withoutMentorship / (stats.totalSocialEnterprises || 1)) *
              100
            ).toFixed(2)}%`}
            icon={
              <PersonRemoveIcon
                sx={{ fontSize: "26px", color: colors.redAccent[500] }} // 🔄 Updated icon
              />
            }
          />
        </Box>

        {/* SE with Significant Growth */}
        <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <StatBox
            title={`${stats.growthScoreTotal} ${
              stats.growthScore?.[0]?.abbr || "N/A"
            }`}
            subtitle="SE with Significant Growth"
            progress={Math.min(stats.cumulativeGrowth / 100, 1)} // ✅ Cap at 100%
            increase={`${stats.cumulativeGrowth}%`}
            icon={
              <TrendingUpIcon
                sx={{ fontSize: "26px", color: colors.blueAccent[500] }} // 🔄 Updated icon
              />
            }
          />
        </Box>
      </Box>

      {/* SE Performance Trend*/}
      <Box
        gridColumn="span 12"
        gridRow="span 2"
        backgroundColor={colors.primary[400]}
        paddingTop="5px"
        marginTop="20px"
      >
        <SEPerformanceTrendChart />
      </Box>

      {/* Row 2 - Horizontal Bar Charts */}
      <Box
        display="flex"
        flexWrap="wrap"
        gap="20px"
        justifyContent="space-between"
        mt="20px"
      >
        {/* Overall SE Performance */}
        <Box
          flex="1 1 100%"
          height="400px"
          backgroundColor={colors.primary[400]}
          p="20px"
        >
          <Box
            display="flex"
            justifyContent="space-between"
            alignItems="center"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
            >
              {stats.categoricalScoreForAllSE?.length > 0
                ? "Evaluation Score Distribution"
                : ""}
            </Typography>

            {/* Tooltip Icon */}
            <Tooltip
              title={
                <Box sx={{ maxWidth: 300, p: 1 }}>
                  <Typography variant="body1" fontWeight="bold">
                    What does this chart show? 📊
                  </Typography>
                  <Typography variant="body2" sx={{ mt: 1 }}>
                    This bar chart displays the <strong>average scores</strong>{" "}
                    given to Social Enterprises (SEs) across various evaluation
                    categories.
                  </Typography>
                  <Typography variant="body2" sx={{ mt: 1 }}>
                    The scores are based on <strong>mentor evaluations</strong>,
                    reflecting SE performance in specific business areas such
                    as:
                  </Typography>
                  <Box sx={{ pl: 2, mt: 1 }}>
                    <Typography variant="body2">• Finance</Typography>
                    <Typography variant="body2">• Marketing</Typography>
                    <Typography variant="body2">• Product Design</Typography>
                    <Typography variant="body2">• Logistics</Typography>
                    <Typography variant="body2">• HR, etc.</Typography>
                  </Box>
                  <Typography variant="body2" sx={{ mt: 2 }}>
                    Higher scores indicate stronger performance in that area.
                    Use this chart to{" "}
                    <strong>compare strengths and weaknesses</strong> across all
                    SEs.
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
          <Box
            height="90%"
            display="flex"
            justifyContent="center"
            alignItems="center"
          >
            {stats.categoricalScoreForAllSE?.length > 0 ? (
              <HorizontalBarChart
                data={stats.categoricalScoreForAllSE.map((score) => ({
                  category: score.category,
                  score: parseFloat(score.score) || 0,
                }))}
                type="categoricalScoreForAllSE"
              />
            ) : (
              <Typography
                variant="h6"
                color={colors.grey[300]}
                textAlign="center"
              >
                No Available Data
              </Typography>
            )}
          </Box>
        </Box>
      </Box>

      {/* Main container */}
      <Box display="flex" flexDirection="column" gap="20px" mt="20px">
        {/* Row 3 - Social Enterprise Performance Heatmap */}

        <Box
          flexGrow={1} // ✅ Makes sure the heatmap takes available space
          gridColumn="span 12"
          width="100%"
          backgroundColor={colors.primary[400]}
          p="20px"
          alignSelf="center"
          paddingleft="20px"
          paddingRight="20px"
        >
          <HeatmapWrapper />
        </Box>

        {/* Row 4 - Leaderboard */}
        <Box display="flex" flexDirection="column" gap="20px">
          <Box
            width="100%"
            height="300px"
            backgroundColor={colors.primary[400]}
            p="20px"
          >
            <Box
              display="flex"
              justifyContent="space-between"
              alignItems="center"
            >
              <Typography
                variant="h4"
                fontWeight="bold"
                color={colors.greenAccent[500]}
              >
                {leaderboardData.length > 0 ? "Leaderboard - Ratings" : ""}
              </Typography>

              {/* Right-side controls: Show All button + Tooltip */}
              <Box display="flex" alignItems="center" gap={1}>
                <Button
                  variant="outlined"
                  onClick={() => {
                    setShowAll((prev) => !prev);
                    setCurrentPage(0); // reset page when toggling
                  }}
                  sx={{
                    height: "40px",
                    minWidth: 120,
                    bordercolor: colors.grey[100],
                    backgroundColor: colors.blueAccent[600],
                    color: colors.grey[100],
                    fontWeight: "bold",
                    "&:hover": {
                      backgroundColor: colors.blueAccent[700],
                    },
                  }}
                >
                  {showAll ? "Show Top 10" : "Show All"}
                </Button>

                {/* Tooltip beside button */}
                <Tooltip
                  title={
                    <Box sx={{ maxWidth: 300, p: 1 }}>
                      <Typography variant="body1" fontWeight="bold">
                        What is this leaderboard? 🏆
                      </Typography>
                      <Typography variant="body2" sx={{ mt: 1 }}>
                        This chart ranks Social Enterprises (SEs) based on their{" "}
                        <strong>evaluation performance</strong> over the last 12
                        months.
                      </Typography>
                      <Box sx={{ mt: 1 }}>
                        <Typography variant="body2">
                          🔹 <strong>Weighted Average Rating</strong> –
                          Emphasizes recent evaluations more heavily. SEs that
                          performed well consistently and recently are ranked
                          higher.
                        </Typography>
                        <Typography variant="body2" sx={{ mt: 0.5 }}>
                          🔹 <strong>Simple Average Rating</strong> – Straight
                          average of monthly scores, used to show trend
                          differences.
                        </Typography>
                      </Box>
                      <Typography variant="body2" sx={{ mt: 2 }}>
                        Color indicators:
                      </Typography>
                      <Box sx={{ mt: 1, pl: 1 }}>
                        <Typography
                          variant="body2"
                          sx={{ color: colors.greenAccent[500] }}
                        >
                          🟩 Green – SE is improving (recent rating higher than
                          overall average)
                        </Typography>
                        <Typography
                          variant="body2"
                          sx={{ color: colors.redAccent[500], mt: 0.5 }}
                        >
                          🟥 Red – SE’s recent performance is declining
                        </Typography>
                        <Typography
                          variant="body2"
                          sx={{ color: colors.grey[400], mt: 0.5 }}
                        >
                          ◻️ Grey – Performance is stable
                        </Typography>
                      </Box>
                      <Typography variant="body2" sx={{ mt: 2 }}>
                        Only SEs with <strong>3 or more evaluations</strong> in
                        the past year are included to ensure fair comparisons.
                      </Typography>
                      <Typography variant="body2" sx={{ mt: 1 }}>
                        Use this leaderboard to identify{" "}
                        <strong>top-performing</strong> SEs and monitor shifts
                        in performance over time.
                      </Typography>
                    </Box>
                  }
                  arrow
                  placement="top"
                >
                  <IconButton sx={{ color: colors.grey[300] }}>
                    <HelpOutlineIcon />
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>

            <Box height="100%" display="flex" alignItems="center">
              {/* Prev Button - Only when showAll */}
              {showAll && (
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

              {/* Chart in the center */}
              <Box
                flexGrow={1}
                minWidth={0}
                overflow="hidden"
                display="flex"
                justifyContent="center"
                alignItems="center"
                height="100%"
                sx={{
                  pl: showAll ? 0 : 2,
                  pr: showAll ? 0 : 2,
                }}
              >
                {leaderboardData.length === 0 ? (
                  <Typography
                    variant="h6"
                    color={colors.grey[300]}
                    textAlign="center"
                  >
                    No data available for plotting.
                  </Typography>
                ) : (
                  <LeaderboardChart data={paginatedData} />
                )}
              </Box>

              {/* Next Button - Only when showAll */}
              {showAll && (
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
        </Box>
      </Box>
      {/* Row 5 - Improvement Score Over Time */}
      <Box display="flex" flexDirection="column" gap="20px" mt="20px">
        <Box height="300px" backgroundColor={colors.primary[400]} p="20px">
          {/* Title and Tooltip Container */}
          <Box
            display="flex"
            justifyContent="space-between"
            alignItems="center"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
            >
              {stats?.improvementScore?.length > 0
                ? "Improvement Score Trends Over Time"
                : ""}
            </Typography>

            {/* Tooltip for explanation */}
            <Tooltip
              title={
                <Box sx={{ maxWidth: 300, p: 1 }}>
                  <Typography variant="body1" fontWeight="bold">
                    Understanding the Improvement Score Trends 📊
                  </Typography>
                  <Typography variant="body2" sx={{ mt: 1 }}>
                    This chart tracks the{" "}
                    <strong>progress of Social Enterprises (SEs)</strong> over
                    time using two key indicators:
                  </Typography>
                  <Box sx={{ mt: 1 }}>
                    <Typography variant="body2">
                      🔹{" "}
                      <strong style={{ color: colors.greenAccent[500] }}>
                        Overall Avg Improvement
                      </strong>{" "}
                      → Measures the <strong>average improvement score</strong>{" "}
                      across all SEs.
                    </Typography>
                    <Typography variant="body2" sx={{ mt: 0.5 }}>
                      🔹{" "}
                      <strong style={{ color: colors.blueAccent[500] }}>
                        Median Improvement
                      </strong>{" "}
                      → Represents the <strong>middle improvement score</strong>{" "}
                      to reduce the impact of outliers.
                    </Typography>
                  </Box>
                  <Typography variant="body1" fontWeight="bold" sx={{ mt: 2 }}>
                    📌 How to Read the Chart:
                  </Typography>
                  <Box sx={{ mt: 1 }}>
                    <Typography variant="body2">
                      📈 <strong>Upward trends</strong> → SEs are improving over
                      time.
                    </Typography>
                    <Typography variant="body2" sx={{ mt: 0.5 }}>
                      ⏸️ <strong>Flat trends</strong> → Growth is slow but
                      stable.
                    </Typography>
                    <Typography variant="body2" sx={{ mt: 0.5 }}>
                      📉 <strong>Declining trends</strong> → SEs are facing
                      challenges.
                    </Typography>
                  </Box>
                </Box>
              }
              arrow
              placement="top"
            >
              <IconButton sx={{ color: colors.grey[300] }}>
                <HelpOutlineIcon />
              </IconButton>
            </Tooltip>
          </Box>

          {/* Chart Container - Properly Centered */}
          <Box
            height="100%"
            display="flex"
            justifyContent="center"
            alignItems="center"
            padding="20px"
          >
            {(() => {
              try {
                console.log(
                  "Raw stats.improvementScore:",
                  stats?.improvementScore
                );

                if (!stats?.improvementScore) {
                  console.error("Data not found");
                  throw new Error("Data not found");
                }
                if (stats.improvementScore.length === 0) {
                  console.warn("No Available Data");
                  return (
                    <Typography
                      variant="h6"
                      color={colors.grey[300]}
                      textAlign="center"
                    >
                      No Available Data
                    </Typography>
                  );
                }

                // Function to convert full date (YYYY-MM-DD) into "QX YYYY" format
                const getQuarterLabel = (dateString) => {
                  if (!dateString) return "Unknown";
                  const date = new Date(dateString);
                  if (isNaN(date.getTime())) return "Unknown"; // Handle invalid dates

                  const year = date.getFullYear();
                  const month = date.getMonth() + 1; // Months are 0-based
                  const quarter = Math.ceil(month / 3);
                  return `Q${quarter} ${year}`;
                };

                // Group data by quarters dynamically
                const formattedData = stats.improvementScore.reduce(
                  (acc, point) => {
                    const quarterLabel = getQuarterLabel(point.month);
                    if (!acc[quarterLabel]) {
                      acc[quarterLabel] = {
                        overall_avg_improvement: 0,
                        median_improvement: 0,
                        count: 0,
                      };
                    }
                    acc[quarterLabel].overall_avg_improvement +=
                      parseFloat(point.overall_avg_improvement) || 0;
                    acc[quarterLabel].median_improvement +=
                      parseFloat(point.median_improvement) || 0;
                    acc[quarterLabel].count += 1;
                    return acc;
                  },
                  {}
                );

                console.log(
                  "Formatted data grouped by quarters:",
                  formattedData
                );

                // Convert grouped data into chart format
                const sortedQuarters = Object.keys(formattedData).sort(
                  (a, b) => {
                    const [qa, ya] = a.split(" ");
                    const [qb, yb] = b.split(" ");
                    const quarterToMonth = { Q1: 1, Q2: 4, Q3: 7, Q4: 10 };
                    const dateA = new Date(
                      parseInt(ya),
                      quarterToMonth[qa] - 1
                    );
                    const dateB = new Date(
                      parseInt(yb),
                      quarterToMonth[qb] - 1
                    );
                    return dateA - dateB;
                  }
                );

                const chartData = [
                  {
                    id: "Overall Avg Improvement",
                    data: sortedQuarters.map((quarter) => ({
                      x: quarter,
                      y:
                        formattedData[quarter].overall_avg_improvement /
                        formattedData[quarter].count,
                    })),
                  },
                  {
                    id: "Median Improvement",
                    data: sortedQuarters.map((quarter) => ({
                      x: quarter,
                      y:
                        formattedData[quarter].median_improvement /
                        formattedData[quarter].count,
                    })),
                  },
                ];

                console.log("Final chart data:", chartData);

                return <DualAxisLineChart data={chartData} />;
              } catch (error) {
                console.error("Error processing data:", error);
                return (
                  <Typography variant="h6" color="red" textAlign="center">
                    Error loading data
                  </Typography>
                );
              }
            })()}
          </Box>
        </Box>

        {/* Row 5 - Social Enterprise Performance Comparison */}
        <Box height="500px" backgroundColor={colors.primary[400]} p="20px">
          <Typography
            variant="h3"
            fontWeight="bold"
            color={colors.greenAccent[500]}
          >
            Social Enterprise Performance Comparison
          </Typography>
          <Box
            height="100%"
            display="flex"
            justifyContent="center"
            alignItems="center"
          >
            <BarChart />{" "}
            {/* No need for conditional rendering, BarChart fetches its own data */}
          </Box>
        </Box>
      </Box>
    </Box>
  );
};

export default Analytics;