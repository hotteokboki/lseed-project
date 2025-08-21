import { Box, useTheme, Typography, Button } from "@mui/material";
import Header from "../../components/Header";
import CashFlowBarChart from "../../components/CashflowBarChart.jsx";
import FinancialPerformanceTrendChart from "../../components/FinancialPerformanceTrendChart.jsx";
import { tokens } from "../../theme";
import React, { useEffect, useMemo, useState } from "react";
import TopSellingItemsPie from "../../components/TopSellingItems.jsx";
import InventoryTurnoverTrend from "../../components/InventoryTurnoverTrend.jsx";
import axiosClient from "../../api/axiosClient.js";
import FinanceRiskHeatmap from "../../components/FinanceRiskHeatMap.jsx";
import AttachMoneyOutlinedIcon from "@mui/icons-material/AttachMoneyOutlined";
import TrendingUpOutlinedIcon from "@mui/icons-material/TrendingUpOutlined";
import AccountBalanceWalletOutlinedIcon from "@mui/icons-material/AccountBalanceWalletOutlined";
import DonutSmallOutlinedIcon from "@mui/icons-material/DonutSmallOutlined";
import PercentOutlinedIcon from "@mui/icons-material/PercentOutlined";
import AssessmentOutlinedIcon from "@mui/icons-material/AssessmentOutlined";
import CapitalFlowsColumns from "../../components/CapitalFlowsColumns.jsx";
import CumulativeCashPosition from "../../components/CumulativeCashPosition.jsx";
import RevenueSeasonalityHeatmap from "../../components/RevenueSeasonalityHeatmap.jsx";

const FinancialAnalytics = ({ }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  function usePortfolioKPIs({ from, to, program } = {}) {
    const [kpis, setKpis] = useState(null);
    const [loading, setLoading] = useState(true);
    const [err, setErr] = useState(null);

    useEffect(() => {
      let cancelled = false;
      const params = {};
      if (from) params.from = from;
      if (to) params.to = to;
      if (program) params.program = program;

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
    }, [from, to, program]);

    return { kpis, loading, err };
  }

  const fmtMoney = (n) => `₱${Number(n ?? 0).toLocaleString()}`;
  const fmtPct = (n) => (n == null ? "—" : `${(Number(n) * 100).toFixed(1)}%`);
  const fmtNum = (n) => (n == null ? "—" : Number(n).toLocaleString());

  const { kpis, loading, err } = usePortfolioKPIs({
    // from: "2025-01-01",
    // to: "2025-07-01",
    // program: selectedProgram,
  });

  return (
    <Box m="20px">
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Header
          title="Financial Analytics"
          subtitle="Welcome to Financial Analytics"
        />
      </Box>

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

      {err && (
        <Typography color="error" mt={1}>
          Failed to load KPIs: {String(err)}
        </Typography>
      )}

      {/* Row 3 - Star Trend Chart */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <FinancialPerformanceTrendChart
          isDashboard={false}
        />
      </Box>

      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <CumulativeCashPosition />
      </Box>

      {/* Row 4 - Cash Flow Analysis (Overall) */}
      <Box backgroundColor={colors.primary[400]} p="20px" paddingBottom={8} mt="20px">
        <CashFlowBarChart />
      </Box>

      {/* Row 5 - Top Selling Items (Overall) */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <TopSellingItemsPie />
      </Box>

      {/* Row 6 - Inventory Turnover (Overall) */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <InventoryTurnoverTrend />
      </Box>

      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <RevenueSeasonalityHeatmap /* from="2025-01-01" to="2025-07-01" program={selectedProgram} */ />
      </Box>

      {/* Row 7 - Financial Heatmap */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <FinanceRiskHeatmap />
      </Box>
      {/* Row 8 - Capital Flows */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <CapitalFlowsColumns />
      </Box>
    </Box>
  );
};

export default FinancialAnalytics;