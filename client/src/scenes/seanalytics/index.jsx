import AccountBalanceWalletOutlinedIcon from "@mui/icons-material/AccountBalanceWalletOutlined";
import AssessmentOutlinedIcon from "@mui/icons-material/AssessmentOutlined";
import AssignmentIcon from "@mui/icons-material/Assignment";
import AttachMoneyOutlinedIcon from "@mui/icons-material/AttachMoneyOutlined";
import DonutSmallOutlinedIcon from "@mui/icons-material/DonutSmallOutlined";
import DownloadIcon from "@mui/icons-material/Download";
import PeopleIcon from "@mui/icons-material/People";
import PercentOutlinedIcon from "@mui/icons-material/PercentOutlined";
import StarIcon from "@mui/icons-material/Star";
import TrendingUpOutlinedIcon from "@mui/icons-material/TrendingUpOutlined";
import {
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Grid,
  Menu,
  MenuItem,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
} from "@mui/material";
import { alpha, useTheme } from "@mui/material/styles";
import { DataGrid, GridToolbar } from "@mui/x-data-grid";
import { useEffect, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import axiosClient from "../../api/axiosClient.js";
import CapitalFlowsColumns from "../../components/CapitalFlowsColumns.jsx";
import CashFlowBarChart from "../../components/CashflowBarChart.jsx";
import CumulativeCashPosition from "../../components/CumulativeCashPosition.jsx";
import FinanceRiskHeatmap from "../../components/FinanceRiskHeatMap.jsx";
import FinancialPerformanceTrendChart from "../../components/FinancialPerformanceTrendChart.jsx";
import InventoryTurnoverTrend from "../../components/InventoryTurnoverTrend.jsx";
import LikertChart from "../../components/LikertChart";
import PieChart from "../../components/PieChart";
import RadarChart from "../../components/RadarChart";
import RevenueSeasonalityHeatmap from "../../components/RevenueSeasonalityHeatmap.jsx";
import SEPerformanceTrendChart from "../../components/SEPerformanceTrendChart";
import StatBox from "../../components/StatBox";
import TopSellingItemsPie from "../../components/TopSellingItems.jsx";
import { tokens } from "../../theme";

const SEAnalytics = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { id } = useParams(); // Extract the `id` from the URL
  const [selectedSEId, setSelectedSEId] = useState(id); // State to manage selected SE
  const [socialEnterprises, setSocialEnterprises] = useState([]); // List of all social enterprises
  const performanceOverviewChart = useRef(null);
  const painPointsChart = useRef(null);
  const scoreDistributionChart = useRef(null);

  // TODO: Determine the charts to render in reports
  // const revenueVSexpensesChart = useRef(null);
  // const cashFlowAnalysisChart = useRef(null);
  // const equityChart = useRef(null);

  const [isExporting, setIsExporting] = useState(false);
  const [selectedSE, setSelectedSE] = useState(null); // Selected social enterprise object
  const [pieData, setPieData] = useState([]); // Real common challenges data
  const [likertData, setLikertData] = useState([]); // Real Likert scale data
  const [radarData, setRadarData] = useState([]); // Real radar chart data
  const [isLoadingEvaluations, setIsLoadingEvaluations] = useState(false);
  const [seApplication, setSEApplication] = useState(null);
  const [evaluationsData, setEvaluationsData] = useState([]);
  const [selectedEvaluation, setSelectedEvaluation] = useState(null);
  const [openDialog, setOpenDialog] = useState(false);
  const [activeTab, setActiveTab] = useState("evaluation"); // "evaluation" | "financial"
  const evaluationRef = useRef(null);
  const financialRef = useRef(null);
  const pendingScrollRef = useRef(null);
  const navigate = useNavigate(); // Initialize useNavigate
  const [stats, setStats] = useState({
    registeredUsers: 0,
    totalEvaluations: 0,
    pendingEvaluations: 0,
    avgRating: 0,
    acknowledgedEvaluations: 0,
  });
  const [criticalAreas, setCriticalAreas] = useState([]);
  const [moreOpen, setMoreOpen] = useState(false);
  const [anchorEl, setAnchorEl] = useState(null);
  const open = Boolean(anchorEl);
  const STICKY_OFFSET = 0;

  useEffect(() => {
    const fetchApplicationDetails = async () => {
      if (!selectedSE || !selectedSE.accepted_application_id) {
        setSEApplication(null);
        return;
      }

      try {
        const res = await axiosClient.get(
          `/api/get-accepted-application/${selectedSE.accepted_application_id}`
        );

        const data = res.data;
        setSEApplication(data);
      } catch (error) {
        console.error("Error fetching application details:", error);
        setSEApplication(null);
      }
    };

    fetchApplicationDetails();
  }, [selectedSE]);

  function usePortfolioKPIs({ from, to, program, seId } = {}) {
    const [kpis, setKpis] = useState(null);
    const [loading, setLoading] = useState(true);
    const [err, setErr] = useState(null);

    useEffect(() => {
      let cancelled = false;
      const params = {};
      if (from) params.from = from;
      if (to) params.to = to;
      if (program) params.program = program;
      if (seId) params.se_id = seId; // NEW: filter by SE

      (async () => {
        try {
          setLoading(true);
          const { data } = await axiosClient.get("/api/finance-kpis", { params });
          if (!cancelled) setKpis(data);
        } catch (e) {
          if (!cancelled) setErr(e);
        } finally {
          if (!cancelled) setLoading(false);
        }
      })();

      return () => { cancelled = true; };
    }, [from, to, program, seId]); // include seId

    return { kpis, loading, err };
  }

  const fmtMoney = (n) => `₱${Number(n ?? 0).toLocaleString()}`;
  const fmtPct = (n) => (n == null ? "—" : `${(Number(n) * 100).toFixed(1)}%`);
  const fmtNum = (n) => (n == null ? "—" : Number(n).toLocaleString());

  const { kpis, loading, err } = usePortfolioKPIs({
    seId: selectedSEId
  });

  useEffect(() => {
    const sections = [
      ["evaluation", evaluationRef],
      ["financial", financialRef],
    ];

    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            const key = entry.target.getAttribute("data-section");
            if (key) setActiveTab(key);
          }
        });
      },
      {
        // top margin accounts for the sticky pills; bottom leaves room
        rootMargin: `-${STICKY_OFFSET + 160}px 0px -60% 0px`,
        threshold: 0.01,
      }
    );

    sections.forEach(([key, ref]) => {
      if (ref.current) {
        ref.current.setAttribute("data-section", key);
        observer.observe(ref.current);
      }
    });

    return () => observer.disconnect();
  }, [STICKY_OFFSET]);

  // Fetch all necessary data for the page
  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch SE list
        const seResponse = await axiosClient.get(
          `/api/get-all-social-enterprises`
        );
        const seData = seResponse.data;

        const formattedSEData = seData.map((se) => ({
          id: se?.se_id ?? "",
          name: se?.team_name ?? "Unnamed SE",
          abbr: se?.abbr ?? "",
          description: se?.description ?? "",
          sdgs:
            Array.isArray(se?.sdgs) && se.sdgs.length > 0
              ? se.sdgs
              : ["No SDG listed"],
          accepted_application_id: se?.accepted_application_id ?? "",
        }));

        // Set selected SE
        if (id) {
          const initialSE = formattedSEData.find((se) => se.id === id);
          setSelectedSE(initialSE);
          setSelectedSEId(id);
        }

        // Fetch SE-specific analytics (with fallbacks)
        if (id) {
          const analyticsResults = await Promise.allSettled([
            axiosClient.get(`/api/se-analytics-stats/${id}`),
            axiosClient.get(`/api/critical-areas/${id}`),
            axiosClient.get(`/api/common-challenges/${id}`),
            axiosClient.get(`/api/likert-data/${id}`),
            axiosClient.get(`/api/radar-data/${id}`),
            axiosClient.get(`/api/get-mentor-evaluations-by-seid/${id}`),
          ]);

          const [
            statsResult,
            criticalAreasResult,
            pieResult,
            likertResult,
            radarResult,
            evaluationsResult,
          ] = analyticsResults;

          // Evaluations
          if (evaluationsResult.status === "fulfilled") {
            const rawEvaluations = await evaluationsResult.value.data;

            const formattedEvaluationsData = rawEvaluations.map(
              (evaluation) => ({
                id: evaluation.evaluation_id,
                evaluator_id: evaluation.evaluation_id,
                evaluator_name: evaluation.evaluator_name,
                social_enterprise: evaluation.social_enterprise,
                evaluation_date: evaluation.evaluation_date,
                acknowledged: evaluation.acknowledged ? "Yes" : "No",
              })
            );

            setEvaluationsData(formattedEvaluationsData);
          } else {
            console.warn("No evaluations found or failed to fetch.");
          }

          // Stats
          if (statsResult.status === "fulfilled") {
            const statsData = await statsResult.value.data;
            setStats({
              registeredUsers:
                Number(statsData.registeredUsers?.[0]?.total_users) || 0,
              totalEvaluations:
                statsData.totalEvaluations?.[0]?.total_evaluations || "0",
              pendingEvaluations:
                statsData.pendingEvaluations?.[0]?.pending_evaluations || "0",
              acknowledgedEvaluations:
                statsData.acknowledgedEvaluations?.[0]
                  ?.acknowledged_evaluations || "0",
              avgRating: statsData.avgRating?.[0]?.avg_rating || "N/A",
            });
          } else {
            console.warn("Stats data failed:", statsResult.reason);
          }

          // Critical Areas
          if (criticalAreasResult.status === "fulfilled") {
            const criticalAreasData = await criticalAreasResult.value.data;
            setCriticalAreas(criticalAreasData);
          }

          // Pie Chart
          if (pieResult.status === "fulfilled") {
            const rawPieData = await pieResult.value.data;
            const formattedPieData = Array.from(
              new Map(
                rawPieData.map((item, index) => [
                  item.category || `Unknown-${index}`,
                  {
                    id: item.category || `Unknown-${index}`,
                    label:
                      item.percentage && !isNaN(item.percentage)
                        ? `${parseInt(item.percentage, 10)}%`
                        : "0%",
                    value:
                      item.count && !isNaN(item.count)
                        ? parseInt(item.count, 10)
                        : 0,
                    comment: item.comment || "No comment available",
                  },
                ])
              ).values()
            );
            setPieData(formattedPieData);
          }

          // Likert
          if (likertResult.status === "fulfilled") {
            const rawLikertData = await likertResult.value.data;
            setLikertData(rawLikertData);
          }

          // Radar
          if (radarResult.status === "fulfilled") {
            const radarChartData = await radarResult.value.data;
            if (Array.isArray(radarChartData)) {
              setRadarData(radarChartData);
            } else {
              console.error("Invalid radar data format", radarChartData);
            }
          }
        }
      } catch (error) {
        console.error("Error fetching data:", error);
      } finally {
        setIsLoadingEvaluations(false);
      }
    };

    fetchData();
  }, [id]);

  const handleMenuClick = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleGenerateReport = (type) => {
    handleClose();
    if (type === "collaboration") {
      handleGenerateCollaborationReport(); // or handleCollaborationReport()
    } else if (type === "stakeholder") {
      handleDownloadStakeholderReport();
    }
  };

  const handleTabChange = (_e, next) => {
    if (!next) return;
    setActiveTab(next);
    pendingScrollRef.current = next;   // mark that this change came from a click
  };

  useEffect(() => {
    if (!pendingScrollRef.current) return;  // ignore route reload / IO updates
    const ref =
      pendingScrollRef.current === "evaluation" ? evaluationRef : financialRef;

    requestAnimationFrame(() => {
      ref.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      pendingScrollRef.current = null; // reset
    });
  }, [activeTab]);

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

  const columns = [
    {
      field: "social_enterprise",
      headerName: "Social Enterprise",
      flex: 1,
      minWidth: 150,
    },
    {
      field: "evaluator_name",
      headerName: "Evaluator",
      flex: 1,
      minWidth: 150,
    },
    {
      field: "acknowledged",
      headerName: "Acknowledged",
      flex: 1,
      minWidth: 150,
    },
    {
      field: "evaluation_date",
      headerName: "Evaluation Date",
      flex: 1,
      minWidth: 150,
    },
    {
      field: "action",
      headerName: "Action",
      flex: 1,
      minWidth: 150,
      renderCell: (params) => (
        <Button
          variant="contained"
          style={{ backgroundColor: colors.primary[600], color: "white" }}
          onClick={() => handleViewExistingEvaluation(params.row.id)}
        >
          View
        </Button>
      ),
    },
  ];

  const handleViewExistingEvaluation = async (evaluation_id) => {
    try {
      const response = await axiosClient.get(`/api/get-evaluation-details`, {
        params: { evaluation_id },
      });

      if (!response.data || response.data.length === 0) {
        console.warn("⚠️ No evaluation details found.");
        return;
      }

      const groupedEvaluation = response.data.reduce((acc, evalItem) => {
        const {
          evaluation_date,
          evaluator_name,
          social_enterprise,
          category_name,
          star_rating,
          selected_comments,
          additional_comment,
        } = evalItem;

        if (!acc.id) {
          acc.id = evaluation_id;
          acc.evaluator_name = evaluator_name;
          acc.social_enterprise = social_enterprise;
          acc.evaluation_date = evaluation_date;
          acc.categories = [];
        }

        acc.categories.push({
          category_name,
          star_rating,
          selected_comments: Array.isArray(selected_comments)
            ? selected_comments
            : [],
          additional_comment,
        });

        return acc;
      }, {});

      setSelectedEvaluation(groupedEvaluation);
      setOpenDialog(true);
    } catch (error) {
      console.error("❌ Error fetching evaluation details:", error);
    }
  };

  // TODO: implement generation of financial reports.
  // const handleDownloadStakeholderReport = () => {
  //   setIsExporting(true);

  //   setTimeout(async () => {
  //     const revenueSVG = revenueVSexpensesChart.current?.querySelector("svg");
  //     const cashFlowSVG = cashFlowAnalysisChart.current?.querySelector("svg");
  //     const equitySVG = equityChart.current?.querySelector("svg");

  //     if (
  //       !revenueSVG ||
  //       !selectedSEId ||
  //       !currentSEFinancialMetrics ||
  //       !cashFlowSVG ||
  //       !equitySVG
  //     ) {
  //       setIsExporting(false);
  //       return alert("Revenue chart or data not found");
  //     }

  //     const serialize = (svg) => new XMLSerializer().serializeToString(svg);

  //     const svgToBase64 = async (svgData, bbox) => {
  //       const scale = 3;
  //       const canvas = document.createElement("canvas");
  //       canvas.width = bbox.width * scale;
  //       canvas.height = bbox.height * scale;
  //       const ctx = canvas.getContext("2d");
  //       ctx.scale(scale, scale); // upscale before drawing

  //       const img = new Image();
  //       const blob = new Blob([svgData], {
  //         type: "image/svg+xml;charset=utf-8",
  //       });
  //       const url = URL.createObjectURL(blob);

  //       return new Promise((resolve) => {
  //         img.onload = () => {
  //           ctx.drawImage(img, 0, 0);
  //           URL.revokeObjectURL(url);
  //           resolve(canvas.toDataURL("image/png"));
  //         };
  //         img.src = url;
  //       });
  //     };

  //     try {
  //       const revenueSVGData = serialize(revenueSVG);
  //       const cashFlowSVGData = serialize(cashFlowSVG);
  //       const equitySVGData = serialize(equitySVG);

  //       const bbox = revenueSVG.getBoundingClientRect();
  //       const cashFlowSVGBBox = cashFlowSVG.getBoundingClientRect();
  //       const equitySVGBBox = equitySVG.getBoundingClientRect();

  //       const chartImageBase64 = await svgToBase64(revenueSVGData, bbox);
  //       const cashFlowImageBase64 = await svgToBase64(
  //         cashFlowSVGData,
  //         cashFlowSVGBBox
  //       );
  //       const equityImageBase64 = await svgToBase64(
  //         equitySVGData,
  //         equitySVGBBox
  //       );

  //       const response = await axiosClient.post(
  //         `/api/financial-report`,
  //         {
  //           chartImage: chartImageBase64,
  //           cashFlowImage: cashFlowImageBase64,
  //           equityImage: equityImageBase64,
  //           selectedSEId,
  //           totalRevenue: currentSEFinancialMetrics.totalRevenue,
  //           totalExpenses: currentSEFinancialMetrics.totalExpenses,
  //           netIncome: currentSEFinancialMetrics.netIncome,
  //           totalAssets: currentSEFinancialMetrics.totalAssets,
  //           selectedSERevenueVsExpensesData,
  //           transformedCashFlowData,
  //           selectedSEEquityTrendData,
  //           inventoryTurnoverByItemData,
  //           netProfitMargin,
  //           grossProfitMargin,
  //           debtToAssetRatio,
  //         },
  //         {
  //           responseType: "blob",
  //         }
  //       );

  //       const blobUrl = URL.createObjectURL(
  //         new Blob([response.data], { type: "application/pdf" })
  //       );
  //       const a = document.createElement("a");
  //       a.href = blobUrl;
  //       a.download = `Stakeholder_Report_${selectedSE?.abbr || "Report"}.pdf`;
  //       a.click();
  //     } catch (err) {
  //       console.error("❌ Failed to generate stakeholder report:", err);
  //       alert("Failed to generate report");
  //     } finally {
  //       setIsExporting(false);
  //     }
  //   }, 100);
  // };

  // If no social enterprise is found, show an error message
  if (!selectedSE && socialEnterprises.length > 0) {
    return <Box>No Social Enterprise found</Box>;
  }

  return (
    <Box m="20px">
      {/* Header */}
      <Box
        display="flex"
        justifyContent="space-between"
        alignItems="center"
        mb={2}
      >

        {/* Left: SE Name */}
        <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
          {selectedSE ? `${selectedSE.name} Analytics` : "Loading..."}
        </Typography>

        {/* Right: Export Button */}
        <Button
          variant="contained"
          color="primary"
          startIcon={<DownloadIcon />}
          onClick={handleMenuClick}
        >
          Generate Report
        </Button>

        <Menu anchorEl={anchorEl} open={open} onClose={handleClose}>
          <MenuItem onClick={() => handleGenerateReport("collaboration")}>
            Evaluation Report
          </MenuItem>
          {/* TODO: Implement generation of financial report */}
          {/* <MenuItem onClick={() => handleGenerateReport("stakeholder")}>
            Financial Report
          </MenuItem> */}
        </Menu>
      </Box>

      <Box
        mt="10px"
        p="10px"
        backgroundColor={colors.primary[500]}
        borderRadius="8px"
      >
        {/* Description Section */}
        <Box sx={{ mb: 4 }}>
          <Typography
            variant="h6"
            color={colors.grey[100]}
            gutterBottom
            sx={{ fontWeight: "bold" }}
          >
            Description
          </Typography>
          <Typography
            variant="body1"
            color={colors.grey[300]}
            sx={{ lineHeight: 1.6 }}
          >
            {selectedSE?.description?.trim()
              ? selectedSE.description
              : "No description provided."}
          </Typography>
        </Box>

        {/* SDGs Involved */}
        {selectedSE?.sdgs?.length > 0 && (
          <Box sx={{ mb: 4 }}>
            <Typography
              variant="h6"
              color={colors.grey[100]}
              gutterBottom
              sx={{ fontWeight: "bold" }}
            >
              SDGs Involved
            </Typography>
            <TableContainer
              sx={{
                maxWidth: 400,
                backgroundColor: colors.primary[500],
                borderRadius: 2,
                boxShadow: 2,
              }}
            >
              <Table size="small">
                <TableBody>
                  {selectedSE.sdgs.map((sdg, index) => (
                    <TableRow key={index}>
                      <TableCell
                        sx={{
                          color: colors.grey[100],
                          borderBottom: "none",
                          py: 1.5,
                        }}
                      >
                        {sdg}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
        )}

        {/* More Info Button */}
        <Box sx={{ mt: 2 }}>
          <Button
            variant="outlined"
            color="secondary"
            onClick={() => setMoreOpen(true)}
            sx={{
              borderColor: colors.grey[300],
              color: colors.grey[100],
              "&:hover": {
                backgroundColor: colors.grey[800],
                borderColor: colors.grey[100],
              },
              textTransform: "none",
              fontWeight: "bold",
              px: 3,
              py: 1,
            }}
          >
            More Info
          </Button>
        </Box>

        <Dialog
          open={moreOpen}
          onClose={() => setMoreOpen(false)}
          maxWidth="md"
          fullWidth
          PaperProps={{
            sx: {
              backgroundColor: "#fff",
              color: "#000",
              border: "2px solid #1E4D2B",
              borderRadius: "12px",
            },
          }}
        >
          <DialogTitle
            sx={{
              backgroundColor: "#1E4D2B",
              color: "#fff",
              textAlign: "center",
              fontSize: "1.75rem",
              fontWeight: "bold",
              py: 2,
            }}
          >
            More Information
          </DialogTitle>

          <DialogContent
            sx={{
              padding: 3,
              maxHeight: "70vh",
              overflowY: "auto",
              backgroundColor: "#f9f9f9",
            }}
          >
            {seApplication ? (
              <Grid container spacing={2}>
                {/* Meta */}
                <Grid item xs={12}>
                  <Typography
                    variant="subtitle2"
                    color="text.secondary"
                    textAlign="right"
                  >
                    Submitted At:{" "}
                    {new Date(seApplication.submitted_at).toLocaleString()}
                  </Typography>
                </Grid>

                {/* SECTION: About the Team */}
                <Grid item xs={12}>
                  <Typography
                    variant="h6"
                    sx={{ color: "#1E4D2B", fontWeight: 700 }}
                    gutterBottom
                  >
                    🧭 About the Team
                  </Typography>
                </Grid>
                <Grid item xs={6}>
                  <strong>Team Name:</strong> {seApplication.team_name}
                </Grid>
                <Grid item xs={6}>
                  <strong>Abbreviation:</strong> {seApplication.abbr}
                </Grid>
                <Grid item xs={12}>
                  <strong>Description:</strong>{" "}
                  {seApplication.description || <i>Not provided</i>}
                </Grid>
                <Grid item xs={6}>
                  <strong>Started:</strong>{" "}
                  {seApplication.enterprise_idea_start}
                </Grid>
                <Grid item xs={6}>
                  <strong>Meeting Frequency:</strong>{" "}
                  {seApplication.meeting_frequency}
                </Grid>
                <Grid item xs={12}>
                  <strong>Communication Modes:</strong>{" "}
                  {(seApplication.communication_modes || []).join(", ")}
                </Grid>

                <Grid item xs={12}>
                  <Divider />
                </Grid>

                {/* SECTION: Problem & Solution */}
                <Grid item xs={12}>
                  <Typography
                    variant="h6"
                    sx={{ color: "#1E4D2B", fontWeight: 700 }}
                    gutterBottom
                  >
                    🎯 Problem & Solution
                  </Typography>
                </Grid>
                <Grid item xs={12}>
                  <strong>Social Problem:</strong>{" "}
                  {seApplication.social_problem || <i>Not provided</i>}
                </Grid>
                <Grid item xs={12}>
                  <strong>Nature:</strong> {seApplication.se_nature}
                </Grid>
                <Grid item xs={12}>
                  <strong>Critical Areas:</strong>{" "}
                  {(seApplication.critical_areas || []).join(", ")}
                </Grid>
                <Grid item xs={12}>
                  <strong>Action Plans:</strong> {seApplication.action_plans}
                </Grid>

                <Grid item xs={12}>
                  <Divider />
                </Grid>

                {/* SECTION: Team Details */}
                <Grid item xs={12}>
                  <Typography
                    variant="h6"
                    sx={{ color: "#1E4D2B", fontWeight: 700 }}
                    gutterBottom
                  >
                    👥 Team Details
                  </Typography>
                </Grid>
                <Grid item xs={12}>
                  <strong>Team Characteristics:</strong>{" "}
                  {seApplication.team_characteristics}
                </Grid>
                <Grid item xs={12}>
                  <strong>Challenges:</strong> {seApplication.team_challenges}
                </Grid>

                <Grid item xs={12}>
                  <Divider />
                </Grid>

                {/* SECTION: Mentoring Preferences */}
                <Grid item xs={12}>
                  <Typography
                    variant="h6"
                    sx={{ color: "#1E4D2B", fontWeight: 700 }}
                    gutterBottom
                  >
                    📌 Mentoring Details
                  </Typography>
                </Grid>
                <Grid item xs={12}>
                  <strong>Team Members:</strong>{" "}
                  {seApplication.mentoring_team_members}
                </Grid>
                <Grid item xs={6}>
                  <strong>Preferred Time:</strong>{" "}
                  {(seApplication.preferred_mentoring_time || []).join(", ")}
                </Grid>
                <Grid item xs={6}>
                  <strong>Time Notes:</strong>{" "}
                  {seApplication.mentoring_time_note}
                </Grid>

                <Grid item xs={12}>
                  <Divider />
                </Grid>

                {/* SECTION: Contact */}
                <Grid item xs={12}>
                  <Typography
                    variant="h6"
                    sx={{ color: "#1E4D2B", fontWeight: 700 }}
                    gutterBottom
                  >
                    📞 Contact Information
                  </Typography>
                </Grid>
                <Grid item xs={6}>
                  <strong>Email:</strong>{" "}
                  {seApplication.focal_email || <i>Not provided</i>}
                </Grid>
                <Grid item xs={6}>
                  <strong>Phone:</strong>{" "}
                  {seApplication.focal_phone || <i>Not provided</i>}
                </Grid>
                <Grid item xs={12}>
                  <strong>Social Media:</strong>{" "}
                  {seApplication.social_media_link}
                </Grid>
                <Grid item xs={12}>
                  <strong>Focal Person Contact:</strong>{" "}
                  {seApplication.focal_person_contact}
                </Grid>

                <Grid item xs={12}>
                  <Divider />
                </Grid>

                {/* SECTION: Pitch Deck */}
                <Grid item xs={12}>
                  <Typography
                    variant="h6"
                    sx={{ color: "#1E4D2B", fontWeight: 700 }}
                    gutterBottom
                  >
                    📄 Pitch Deck
                  </Typography>
                  {seApplication.pitch_deck_url ? (
                    <a
                      href={seApplication.pitch_deck_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{
                        color: "#1E4D2B",
                        fontWeight: "bold",
                        textDecoration: "underline",
                      }}
                    >
                      View Document
                    </a>
                  ) : (
                    <i>No pitch deck provided</i>
                  )}
                </Grid>
              </Grid>
            ) : (
              <Typography>Loading...</Typography>
            )}
          </DialogContent>

          <DialogActions
            sx={{
              padding: 2,
              borderTop: "1px solid #1E4D2B",
              justifyContent: "center",
            }}
          >
            <Button
              onClick={() => setMoreOpen(false)}
              variant="outlined"
              sx={{
                color: "#1E4D2B",
                borderColor: "#1E4D2B",
                "&:hover": { backgroundColor: "#E0F2E9" },
              }}
            >
              Close
            </Button>
          </DialogActions>
        </Dialog>
      </Box>

      {/* Section switcher (sticky, pill-style) */}
      <Box
        sx={{
          position: "sticky",
          top: STICKY_OFFSET,                       // sits just below your app bar
          zIndex: (t) => t.zIndex.appBar + 1,
          mb: 2,
          mt: 2,
          py: 0.5,
          backgroundColor: "transparent",
          pointerEvents: "none",                    // wrapper never intercepts clicks
        }}
      >
        {/* Pill bar: the only element with bg/blur and click handling */}
        <Box
          sx={{
            display: "inline-flex",
            alignItems: "center",
            gap: 1,
            px: 1.25,
            py: 1,
            borderRadius: "999px",
            width: "fit-content",                   // no full-width belt
            backgroundColor: alpha(colors.primary[500], 0.6),
            border: `1px solid ${alpha(colors.blueAccent[700], 0.35)}`,
            backdropFilter: "saturate(120%) blur(6px)", // remove if you don't want blur
            boxShadow: "0 8px 22px rgba(0,0,0,.25)",
            pointerEvents: "auto",                  // this box is clickable
          }}
        >
          <Typography variant="subtitle2" sx={{ color: colors.grey[300], mr: 1 }}>
            Analytics
          </Typography>

          <ToggleButtonGroup
            value={activeTab}
            exclusive
            onChange={handleTabChange}
            size="small"
            aria-label="Analytics section switcher"
            sx={{
              "& .MuiToggleButtonGroup-grouped": {
                border: "none",
                mx: 0.5,
                px: 1.75,
                py: 0.75,
                borderRadius: "999px !important",
              },
              "& .MuiToggleButton-root": {
                textTransform: "none",
                color: colors.grey[200],
                backgroundColor: alpha(colors.blueAccent[800], 0.25),
                transition: "all .15s ease",
                "&:hover": { backgroundColor: alpha(colors.blueAccent[700], 0.45) },
                "&.Mui-selected": {
                  color: `${colors.grey[100]} !important`,
                  backgroundColor: `${colors.blueAccent[600]} !important`,
                  boxShadow: "0 4px 12px rgba(0,0,0,.2)",
                },
              },
            }}
          >
            <ToggleButton value="evaluation">Evaluation Analytics</ToggleButton>
            <ToggleButton value="financial">Financial Analytics</ToggleButton>
          </ToggleButtonGroup>
        </Box>
      </Box>

      {activeTab === "evaluation" && (
        <>
          {/* Section Header */}
          <Typography
            variant="h2"
            fontWeight="bold"
            color={colors.greenAccent[500]}
            ref={evaluationRef}
            sx={{ scrollMarginTop: `${STICKY_OFFSET + 100}px` }}
          >
            Evaluation Analytics
          </Typography>
          {/* Row 1 - StatBoxes */}
          <Box
            display="flex"
            flexWrap="wrap"
            gap="20px"
            justifyContent="space-between"
            mt="20px"
          >
            <Box
              flex="1 1 22%"
              backgroundColor={colors.primary[400]}
              display="flex"
              alignItems="center"
              justifyContent="center"
              p="20px"
            >
              <Chip
                label={
                  <Box sx={{ textAlign: "center" }}>
                    <Typography sx={{ fontSize: "20px", lineHeight: 1.2 }}>
                      {stats.registeredUsers}
                    </Typography>
                    <Typography sx={{ fontSize: "16px", lineHeight: 1.2 }}>
                      Registered
                    </Typography>
                    <Typography sx={{ fontSize: "16px", lineHeight: 1.2 }}>
                      Telegram {stats.registeredUsers === 1 ? "User" : "Users"}
                    </Typography>
                  </Box>
                }
                icon={
                  <PeopleIcon
                    sx={{ fontSize: "26px", color: colors.greenAccent[500] }}
                  />
                }
                sx={{
                  p: "10px",
                  backgroundColor: colors.primary[400],
                  color: colors.grey[100],
                  "& .MuiChip-icon": { color: colors.greenAccent[500] },
                  maxWidth: "160px",
                }}
              />
            </Box>

            <Box
              flex="1 1 22%"
              backgroundColor={colors.primary[400]}
              display="flex"
              alignItems="center"
              justifyContent="center"
              p="20px"
            >
              <StatBox
                title={stats.acknowledgedEvaluations}
                subtitle="Acknowledged Evaluations"
                progress={
                  stats.acknowledgedEvaluations / (stats.totalEvaluations || 1)
                }
                increase={
                  isNaN(stats.acknowledgedEvaluations / stats.totalEvaluations)
                    ? "0%"
                    : `${(
                      (stats.acknowledgedEvaluations / stats.totalEvaluations) *
                      100
                    ).toFixed(2)}%`
                }
                icon={
                  <AssignmentIcon
                    sx={{ fontSize: "26px", color: colors.blueAccent[500] }}
                  />
                }
              />
            </Box>

            <Box
              flex="1 1 22%"
              backgroundColor={colors.primary[400]}
              display="flex"
              alignItems="center"
              justifyContent="center"
              p="20px"
            >
              <StatBox
                title={stats.pendingEvaluations}
                subtitle="Pending Evaluations"
                progress={
                  stats.totalEvaluations > 0
                    ? stats.pendingEvaluations / stats.totalEvaluations
                    : 0
                }
                increase={
                  stats.totalEvaluations > 0
                    ? `${(
                      (stats.pendingEvaluations / stats.totalEvaluations) *
                      100
                    ).toFixed(2)}%`
                    : "0%"
                }
                icon={
                  <AssignmentIcon
                    sx={{ fontSize: "26px", color: colors.redAccent[500] }}
                  />
                }
              />
            </Box>

            <Box
              flex="1 1 22%"
              backgroundColor={colors.primary[400]}
              display="flex"
              alignItems="center"
              justifyContent="center"
              p="20px"
            >
              <StatBox
                title={stats.avgRating}
                subtitle="Average rating"
                progress={null}
                sx={{ "& .MuiBox-root.css-1ntui4p": { display: "none" } }}
                icon={
                  <StarIcon
                    sx={{ fontSize: "26px", color: colors.blueAccent[500] }}
                  />
                }
              />
            </Box>
          </Box>

          {/* Evaluation Analytics Tab */}
          <Box
            display="flex"
            flexDirection="column"
            gap={3}
            marginBottom={2}
            marginTop={2}
          >
            <SEPerformanceTrendChart selectedSEId={selectedSEId} />
          </Box>

          <Box display="flex" gap="20px" width="100%" mt="20px" height="500px">
            {/* Evaluations Table */}
            <Box
              sx={{
                backgroundColor: colors.primary[400],
                padding: "20px",
                boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
                display: "flex",
                flexDirection: "column",
                overflow: "hidden",
                "& .MuiDataGrid-root": { border: "none" },
                "& .MuiDataGrid-cell": { borderBottom: "none" },
                "& .name-column--cell": { color: colors.greenAccent[300] },
                "& .MuiDataGrid-columnHeaders, & .MuiDataGrid-columnHeader": {
                  backgroundColor: colors.blueAccent[700] + " !important",
                },
                "& .MuiDataGrid-virtualScroller": {
                  backgroundColor: colors.primary[400],
                },
                "& .MuiDataGrid-footerContainer": {
                  borderTop: "none",
                  backgroundColor: colors.blueAccent[700],
                },
              }}
            >
              <Typography
                variant="h5"
                fontWeight="bold"
                color={colors.grey[100]}
                mb={2}
              >
                Evaluations
              </Typography>
              <DataGrid
                rows={evaluationsData}
                columns={columns}
                getRowId={(row) => row.id}
                getRowHeight={() => "auto"}
                sx={{
                  "& .MuiDataGrid-cell": {
                    display: "flex",
                    alignItems: "center", // vertical centering
                    paddingTop: "12px",
                    paddingBottom: "12px",
                  },
                  "& .MuiDataGrid-columnHeader": {
                    alignItems: "center", // optional: center header label vertically
                  },
                  "& .MuiDataGrid-cellContent": {
                    whiteSpace: "normal",
                    wordBreak: "break-word",
                    overflowWrap: "break-word",
                  },
                  "& .MuiDataGrid-toolbarContainer .MuiButton-text": {
                    color: `${colors.grey[100]} !important`,
                  },
                }}
                slots={{ toolbar: GridToolbar }}
              />
            </Box>
            {/* AREAS OF FOCUS TABLE */}
            <Box
              flex="1"
              backgroundColor={colors.primary[400]}
              height="500px"
              display="flex"
              flexDirection="column"
            >
              {/* Fixed Header */}
              <Box
                display="flex"
                justifyContent="space-between"
                alignItems="center"
                borderBottom={`4px solid ${colors.primary[500]}`}
                p="15px"
                flexShrink={0}
              >
                <Typography
                  color={colors.greenAccent[500]}
                  variant="h3"
                  fontWeight="600"
                >
                  Critical Areas of Focus
                </Typography>
              </Box>

              {/* Scrollable List */}
              <Box
                sx={{
                  flex: 1,
                  overflowY: "auto",
                }}
              >
                {criticalAreas.map((area, i) => (
                  <Box
                    key={i}
                    display="flex"
                    alignItems="center"
                    borderBottom={`4px solid ${colors.primary[500]}`}
                    p="15px"
                  >
                    {/* Icon */}
                    <Box sx={{ pr: 2, fontSize: "24px" }}>📌</Box>

                    {/* Area Name */}
                    <Typography
                      color={colors.grey[100]}
                      variant="h5"
                      fontWeight="500"
                      sx={{
                        whiteSpace: "normal",
                        wordBreak: "break-word",
                      }}
                    >
                      {area}
                    </Typography>
                  </Box>
                ))}
              </Box>
            </Box>
          </Box>
          {/* Evaluation Details Dialog - Read-Only */}
          <Dialog
            open={openDialog}
            onClose={() => setOpenDialog(false)}
            maxWidth="md"
            fullWidth
            PaperProps={{
              style: {
                backgroundColor: "#fff",
                color: "#000",
                border: "1px solid #000",
              },
            }}
          >
            {/* Title with DLSU Green Background */}
            <DialogTitle
              sx={{
                backgroundColor: "#1E4D2B",
                color: "#fff",
                textAlign: "center",
                fontSize: "1.5rem",
                fontWeight: "bold",
              }}
            >
              View Evaluation
            </DialogTitle>

            {/* Content Section */}
            <DialogContent
              sx={{
                padding: "24px",
                maxHeight: "70vh",
                overflowY: "auto",
              }}
            >
              {selectedEvaluation ? (
                <>
                  {/* Evaluator, Social Enterprise, and Evaluation Date */}
                  <Box
                    sx={{
                      marginBottom: "16px",
                      display: "flex",
                      flexDirection: "column",
                      gap: "8px",
                    }}
                  >
                    <Typography
                      variant="h6"
                      sx={{
                        fontWeight: "bold",
                        borderBottom: "1px solid #000",
                        paddingBottom: "8px",
                      }}
                    >
                      Evaluator: {selectedEvaluation.evaluator_name}{" "}
                    </Typography>
                    <Typography
                      variant="h6"
                      sx={{
                        fontWeight: "bold",
                        borderBottom: "1px solid #000",
                        paddingBottom: "8px",
                      }}
                    >
                      Social Enterprise Evaluated:{" "}
                      {selectedEvaluation.social_enterprise}
                    </Typography>
                    <Typography variant="subtitle1" sx={{ color: "#000" }}>
                      Evaluation Date: {selectedEvaluation.evaluation_date}
                    </Typography>
                  </Box>

                  {/* Categories Section */}
                  {selectedEvaluation.categories &&
                    selectedEvaluation.categories.length > 0 ? (
                    selectedEvaluation.categories.map((category, index) => (
                      <Box
                        key={index}
                        sx={{
                          marginBottom: "24px",
                          padding: "16px",
                          border: "1px solid #000",
                          borderRadius: "8px",
                        }}
                      >
                        {/* Category Name and Rating */}
                        <Typography
                          variant="subtitle1"
                          sx={{
                            fontWeight: "bold",
                            marginBottom: "8px",
                          }}
                        >
                          {category.category_name} - Rating: {category.star_rating}{" "}
                          ★
                        </Typography>

                        {/* Selected Comments */}
                        <Typography variant="body1" sx={{ marginBottom: "8px" }}>
                          Comments:{" "}
                          {category.selected_comments.length > 0 ? (
                            category.selected_comments.join(", ")
                          ) : (
                            <i>No comments</i>
                          )}
                        </Typography>

                        {/* Additional Comment */}
                        <Typography variant="body1">
                          Additional Comment:{" "}
                          {category.additional_comment || (
                            <i>No additional comments</i>
                          )}
                        </Typography>
                      </Box>
                    ))
                  ) : (
                    <Typography variant="body1" sx={{ fontStyle: "italic" }}>
                      No categories found for this evaluation.
                    </Typography>
                  )}
                </>
              ) : (
                <Typography variant="body1" sx={{ fontStyle: "italic" }}>
                  Loading evaluation details...
                </Typography>
              )}
            </DialogContent>

            {/* Action Buttons */}
            <DialogActions sx={{ padding: "16px", borderTop: "1px solid #000" }}>
              <Button
                onClick={() => setOpenDialog(false)}
                sx={{
                  color: "#000",
                  border: "1px solid #000",
                  "&:hover": { backgroundColor: "#f0f0f0" },
                }}
              >
                Close
              </Button>
            </DialogActions>
          </Dialog>

          {/* Common Challenges & Performance Score */}
          <Box
            mt="20px"
            sx={{
              backgroundColor: colors.primary[400],
              padding: "20px",
              boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
            }}
          >
            <Typography variant="h5" fontWeight="bold" color={colors.grey[100]}>
              Recurring Issues
            </Typography>
            <Box
              height="250px"
              display="flex"
              justifyContent="center"
              alignItems="center"
              ref={painPointsChart}
            >
              {pieData.length === 0 ? (
                <Typography variant="h6" color={colors.grey[300]}>
                  No common challenges found.
                </Typography>
              ) : (
                <PieChart data={pieData} isExporting={isExporting} />
              )}
            </Box>
          </Box>
          {/* Performance Score */}
          <Box
            mt="20px"
            sx={{
              backgroundColor: colors.primary[400],
              padding: "20px",
              boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
            }}
          >
            <Typography variant="h5" fontWeight="bold" color={colors.grey[100]}>
              Performance Score
            </Typography>
            <Box
              height="250px"
              display="flex"
              justifyContent="center"
              alignItems="center"
              ref={scoreDistributionChart}
            >
              {likertData.length === 0 ? (
                <Typography variant="h6" color={colors.grey[300]}>
                  No performance ratings available.
                </Typography>
              ) : (
                <LikertChart data={likertData} isExporting={isExporting} />
              )}
            </Box>
          </Box>

          {/* Performance Overview */}
          <Box
            mt="20px"
            sx={{
              backgroundColor: colors.primary[400],
              padding: "20px",
              boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
            }}
          >
            <Typography variant="h5" fontWeight="bold" color={colors.grey[100]}>
              Performance Overview
            </Typography>
            <Box
              height="300px"
              display="flex"
              justifyContent="center"
              alignItems="center"
              ref={performanceOverviewChart}
            >
              {radarData.length === 0 ? (
                <Typography variant="h6" color={colors.grey[300]}>
                  Performance Overview Unavailable.
                </Typography>
              ) : (
                <RadarChart radarData={radarData} isExporting={isExporting} />
              )}
            </Box>
          </Box>
        </>
      )}
      {activeTab === "financial" && (
        <>
          {/* Financial Analytics Tab */}
          <Box mt="40px" display="flex" flexDirection="column" gap="20px">
            {/* Section Header */}
            <Typography
              variant="h2"
              fontWeight="bold"
              color={colors.greenAccent[500]}
              ref={financialRef}
              sx={{ scrollMarginTop: `${STICKY_OFFSET + 100}px` }}   // ← add this
            >
              Financial Analytics
            </Typography>

            {/* Row 1 - Aggregate Financial StatBoxes */}
            <Box
              display="flex"
              flexWrap="wrap"
              gap="20px"
              justifyContent="space-between"
              mt="20px"
            >
              {/* Total Revenue */}
              <Box
                flex="1 1 22%"
                backgroundColor={colors.primary[400]}
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                p="20px"
                sx={{ minHeight: 140, borderRadius: 2 }}
              >
                <AttachMoneyOutlinedIcon sx={{ fontSize: 40, color: colors.greenAccent[500], mb: 1 }} />
                <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
                  {loading ? "…" : fmtMoney(kpis?.total_revenue)}
                </Typography>
                <Typography variant="subtitle2" color={colors.grey[300]}>
                  Total Revenue
                </Typography>
              </Box>

              {/* Operating Profit + OM hint */}
              <Box
                flex="1 1 22%"
                backgroundColor={colors.primary[400]}
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                p="20px"
                sx={{ minHeight: 140, borderRadius: 2 }}
              >
                <TrendingUpOutlinedIcon sx={{ fontSize: 40, color: colors.greenAccent[500], mb: 1 }} />
                <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
                  {loading ? "…" : fmtMoney(kpis?.total_operating_profit)}
                </Typography>
                <Typography variant="subtitle2" color={colors.grey[300]}>
                  Operating Profit
                </Typography>
                {!loading && (
                  <Typography variant="caption" sx={{ mt: 0.5, fontStyle: "italic", color: colors.greenAccent[400] }}>
                    OM {fmtPct(kpis?.operating_margin_pct)}
                  </Typography>
                )}
              </Box>

              {/* Net Cash Flow */}
              <Box
                flex="1 1 22%"
                backgroundColor={colors.primary[400]}
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                p="20px"
                sx={{ minHeight: 140, borderRadius: 2 }}
              >
                <AccountBalanceWalletOutlinedIcon
                  sx={{
                    fontSize: 40,
                    color: (kpis?.net_cash_flow ?? 0) >= 0 ? colors.blueAccent[400] : colors.redAccent[400],
                    mb: 1
                  }}
                />
                <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
                  {loading ? "…" : fmtMoney(kpis?.net_cash_flow)}
                </Typography>
                <Typography variant="subtitle2" color={colors.grey[300]}>
                  Net Cash Flow
                </Typography>
              </Box>

              {/* Inventory Turnover + DIO hint */}
              <Box
                flex="1 1 22%"
                backgroundColor={colors.primary[400]}
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                p="20px"
                sx={{ minHeight: 140, borderRadius: 2 }}
              >
                <DonutSmallOutlinedIcon
                  sx={{
                    fontSize: 40,
                    mb: 1,
                    color:
                      (kpis?.overall_turnover ?? 0) >= 3.5 ? colors.greenAccent[500] :
                        (kpis?.overall_turnover ?? 0) >= 2.0 ? colors.blueAccent[400] :
                          colors.redAccent[400]
                  }}
                />
                <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
                  {loading ? "…" : `${fmtNum(kpis?.overall_turnover)}x`}
                </Typography>
                <Typography variant="subtitle2" color={colors.grey[300]}>
                  Inventory Turnover
                </Typography>
                {!loading && (
                  <Typography variant="caption" sx={{ mt: 0.5, fontStyle: "italic", color: colors.greenAccent[400] }}>
                    {fmtNum(kpis?.overall_dio_days)} days DIO
                  </Typography>
                )}
              </Box>

              {/* Gross Margin % */}
              <Box
                flex="1 1 22%"
                backgroundColor={colors.primary[400]}
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                p="20px"
                sx={{ minHeight: 140, borderRadius: 2 }}
              >
                <PercentOutlinedIcon
                  sx={{
                    fontSize: 40,
                    mb: 1,
                    color:
                      (kpis?.gross_margin_pct ?? 0) >= 0.5 ? colors.greenAccent[500] :
                        (kpis?.gross_margin_pct ?? 0) >= 0.3 ? colors.blueAccent[400] :
                          colors.redAccent[400]
                  }}
                />
                <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
                  {loading ? "…" : fmtPct(kpis?.gross_margin_pct)}
                </Typography>
                <Typography variant="subtitle2" color={colors.grey[300]}>
                  Gross Margin
                </Typography>
              </Box>

              {/* Reporting Completeness */}
              <Box
                flex="1 1 22%"
                backgroundColor={colors.primary[400]}
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                p="20px"
                sx={{ minHeight: 140, borderRadius: 2 }}
              >
                <AssessmentOutlinedIcon
                  sx={{
                    fontSize: 40,
                    mb: 1,
                    color:
                      (kpis?.reporting_rate ?? 0) >= 0.9 ? colors.greenAccent[500] :
                        (kpis?.reporting_rate ?? 0) >= 0.7 ? colors.blueAccent[400] :
                          colors.redAccent[400]
                  }}
                />
                <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
                  {loading ? "…" : fmtPct(kpis?.reporting_rate)}
                </Typography>
                <Typography variant="subtitle2" color={colors.grey[300]}>
                  Reporting Rate
                </Typography>
              </Box>
            </Box>

            {/* Row 3 - Star Trend Chart */}
            <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
              <FinancialPerformanceTrendChart
                isDashboard={false}
                selectedSEId={selectedSEId}
              />
            </Box>

            <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
              <CumulativeCashPosition selectedSEId={selectedSEId} />
            </Box>

            {/* Row 4 - Cash Flow Analysis (Overall) */}
            <Box backgroundColor={colors.primary[400]} p="20px" paddingBottom={8} mt="20px">
              <CashFlowBarChart selectedSEId={selectedSEId} />
            </Box>

            {/* Row 5 - Top Selling Items (Overall) */}
            <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
              <TopSellingItemsPie selectedSEId={selectedSEId} />
            </Box>

            {/* Row 6 - Inventory Turnover (Overall) */}
            <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
              <InventoryTurnoverTrend selectedSEId={selectedSEId} />
            </Box>

            <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
              <RevenueSeasonalityHeatmap selectedSEId={selectedSEId} />
            </Box>

            {/* Row 7 - Financial Heatmap */}
            <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
              <FinanceRiskHeatmap selectedSEId={selectedSEId} />
            </Box>
            {/* Row 8 - Capital Flows */}
            <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
              <CapitalFlowsColumns selectedSEId={selectedSEId} />
            </Box>
          </Box>
        </>
      )}
      {/* Back Button with Spacing */}
      <Box mt="20px" display="flex" justifyContent="start">
        <Button
          variant="contained"
          sx={{
            backgroundColor: colors.blueAccent[500],
            color: "black",
            "&:hover": {
              backgroundColor: colors.blueAccent[800],
            },
            width: "2/12",
            maxWidth: "150px",
          }}
          onClick={() => navigate(-1)}
        >
          Back
        </Button>
      </Box>
    </Box>
  );
};

export default SEAnalytics;