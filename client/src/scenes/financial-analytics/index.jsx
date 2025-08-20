import { Box, useTheme, Typography, Button } from "@mui/material";
import Header from "../../components/Header";
import StatBox from "../../components/StatBox";
import LineChart from "../../components/LineChart";
import BarChart from "../../components/BarChart";
import FinancialBarChart from "../../components/FinancialBarChart";
import CashFlowBarChart from "../../components/CashflowBarChart.jsx";
import FinancialPerformanceTrendChart from "../../components/FinancialPerformanceTrendChart.jsx";
import PieChart from "../../components/PieChart";
import { tokens } from "../../theme";
import React, { useEffect, useMemo, useState } from "react";
import { DataGrid } from "@mui/x-data-grid";
import { FormControl, InputLabel, Select, MenuItem } from "@mui/material";
import MoneyOffOutlinedIcon from "@mui/icons-material/MoneyOffOutlined";
import TopSellingItemsPie from "../../components/TopSellingItems.jsx";
import InventoryTurnoverBar from "../../components/InventoryTurnoverBarChart.jsx";
import InventoryTurnoverTrend from "../../components/InventoryTurnoverTrend.jsx";
import { useAuth } from "../../context/authContext";
import axiosClient from "../../api/axiosClient.js";
import FinanceRiskHeatmap from "../../components/FinanceRiskHeatMap.jsx";

const FinancialAnalytics = ({ }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [financialData, setFinancialData] = useState([]);
  const [cashFlowRaw, setCashFlowRaw] = useState([]);
  const { user } = useAuth();
  const [firstAverageProfit, setFirstAverageProfit] = useState(0);
  const [firstAverageEquity, setFirstAverageEquity] = useState(0);
  const [showTop5Mode, setShowTop5Mode] = useState(false);
  const [showTop5ModeEquity, setShowTop5ModeEquity] = useState(false); // New state for equity chart

  const [trendRows, setTrendRows] = useState([]);
  const [seNameMap, setSeNameMap] = useState({});
  const [trendLoading, setTrendLoading] = useState(true);
  const [trendErr, setTrendErr] = useState("");

  useEffect(() => {
    let alive = true;
    (async () => {
      setTrendLoading(true);
      setTrendErr("");
      try {
        const limit = showTop5Mode ? "5" : "all";
        const [trendRes, seRes] = await Promise.all([
          axiosClient.get(`/api/ratings/top-star-trend?limit=${limit}`),
          axiosClient.get(`/api/get-all-social-enterprises`),
        ]);

        if (!alive) return;

        setTrendRows(Array.isArray(trendRes.data) ? trendRes.data : []);

        const nm = {};
        (Array.isArray(seRes.data) ? seRes.data : []).forEach((se) => {
          nm[se.se_id] = se.abbr ? `${se.abbr}` : (se.team_name || se.se_id);
        });
        setSeNameMap(nm);
      } catch (err) {
        if (!alive) return;
        console.error(err);
        setTrendErr("Failed to load star trend.");
        setTrendRows([]);
        setSeNameMap({});
      } finally {
        if (alive) setTrendLoading(false);
      }
    })();
    return () => { alive = false; };
  }, [showTop5Mode]);

  // rows -> Nivo series [{id, data:[{x,y}]}]
  const starTrendSeries = useMemo(() => {
    const bySe = new Map();
    for (const r of trendRows) {
      if (!bySe.has(r.se_id)) bySe.set(r.se_id, []);
      bySe.get(r.se_id).push(r);
    }

    const arr = [];
    for (const [se_id, list] of bySe.entries()) {
      list.sort((a, b) => new Date(a.month) - new Date(b.month));
      const avg = list.reduce((s, v) => s + Number(v.stars_half || 0), 0) / Math.max(1, list.length);
      arr.push({
        id: seNameMap[se_id] || se_id,
        avg,
        data: list.map(r => ({ x: r.month.slice(0, 7), y: Number(r.stars_half || 0) }))
      });
    }
    arr.sort((a, b) => b.avg - a.avg);
    return arr.map(({ id, data }) => ({ id, data }));
  }, [trendRows, seNameMap]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [financialResult, cashFlowResult] = await Promise.allSettled([
          axiosClient.get('/api/financial-statements'),
          axiosClient.get('/api/cashflow'),
        ]);

        if (financialResult.status === 'fulfilled') {
          setFinancialData(financialResult.value.data);
        } else {
          console.error('Failed to fetch financial statements:', financialResult.reason);
          setFinancialData([]); // fallback to empty
        }

        if (cashFlowResult.status === 'fulfilled') {
          setCashFlowRaw(cashFlowResult.value.data);
        } else {
          console.error('Failed to fetch cashflow:', cashFlowResult.reason);
          setCashFlowRaw([]); // fallback to empty
        }
      } catch (error) {
        console.error('Unexpected error:', error);
      }
    };

    fetchData();
  }, []);

  useEffect(() => {
    if (financialData.length > 0) {
      let totalProfitAllSEs = 0;
      let totalEquityAllSEs = 0;
      let totalDataPoints = 0;

      financialData.forEach(item => {
        const totalRevenue = Number(item.total_revenue ?? 0);
        const totalExpenses = Number(item.total_expenses ?? 0);
        const ownerEquity = Number(item.owner_equity ?? 0);

        totalProfitAllSEs += (totalRevenue - totalExpenses);
        totalEquityAllSEs += ownerEquity;
        totalDataPoints++;
      });

      if (totalDataPoints > 0) {
        setFirstAverageProfit(totalProfitAllSEs / totalDataPoints);
        setFirstAverageEquity(totalEquityAllSEs / totalDataPoints);
      }
    }
  }, [financialData]);

  const [inventoryData, setInventoryData] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axiosClient.get(
          `/api/inventory-distribution`
        );
        setInventoryData(response.data);
      } catch (error) {
        console.error("Error fetching inventory data:", error);
      }
    };

    fetchData();
  }, []);

  const inventoryBySE = {};

  inventoryData.forEach(({ se_abbr, qty, price, amount }) => {
    const priceNum = Number(price);
    const amountNum = Number(amount);
    if (!inventoryBySE[se_abbr]) {
      inventoryBySE[se_abbr] = { totalValue: 0, totalCOGS: 0 };
    }
    inventoryBySE[se_abbr].totalValue += qty * priceNum;
    inventoryBySE[se_abbr].totalCOGS += amountNum;
  });

  const inventoryValueData = Object.entries(inventoryBySE)
    .map(([se_id, data]) => ({
      id: se_id,
      value: data.totalValue,
    }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 5);

  const inventoryTurnoverData = Object.entries(inventoryBySE)
    .map(([se_id, data]) => {
      const cogs = data.totalCOGS;
      const avgInventory = data.totalValue;
      const turnover =
        avgInventory === 0 ? 0 : parseFloat((cogs / avgInventory).toFixed(2));
      return { name: se_id, turnover };
    })
    .sort((a, b) => b.turnover - a.turnover)
    .slice(0, 5);

  const seMap = new Map();
  financialData.forEach((item) => {
    const abbr = item.se_abbr ?? "Unknown";
    const parsedDate = item.date
      ? new Date(item.date).toLocaleDateString()
      : "Unknown Date";

    const dataPoint = {
      date: parsedDate,
      totalRevenue: Number(item.total_revenue ?? 0),
      totalExpenses: Number(item.total_expenses ?? 0),
      netIncome: Number(item.net_income ?? 0),
      totalAssets: Number(item.total_assets ?? 0),
      totalLiabilities: Number(item.total_liabilities ?? 0),
      ownerEquity: Number(item.owner_equity ?? 0),
    };

    if (!seMap.has(abbr)) {
      seMap.set(abbr, {
        name: abbr,
        totalRevenue: 0,
        totalExpenses: 0,
        netIncome: 0,
        totalAssets: 0,
        revenueVsExpenses: [],
        cashFlow: [],
        equityTrend: [],
        inventoryBreakdown: [],
      });
    }

    const se = seMap.get(abbr);
    se.totalRevenue += dataPoint.totalRevenue;
    se.totalExpenses += dataPoint.totalExpenses;
    se.netIncome += dataPoint.netIncome;
    se.totalAssets += dataPoint.totalAssets;

    se.revenueVsExpenses.push({
      x: dataPoint.date,
      revenue: dataPoint.totalRevenue,
      expenses: dataPoint.totalExpenses,
    });

    se.cashFlow.push({
      x: dataPoint.date,
      inflow: dataPoint.totalRevenue,
      outflow: dataPoint.totalExpenses,
    });

    se.equityTrend.push({
      x: dataPoint.date,
      equity: dataPoint.ownerEquity,
    });

    se.netProfitMargin = se.totalRevenue
      ? ((se.netIncome / se.totalRevenue) * 100).toFixed(2)
      : "0.00";
    se.grossProfitMargin = se.totalRevenue
      ? (
        ((se.totalRevenue - se.totalExpenses) / se.totalRevenue) *
        100
      ).toFixed(2)
      : "0.00";
    se.debtToAssetRatio = se.totalAssets
      ? (dataPoint.totalLiabilities / dataPoint.totalAssets).toFixed(2)
      : "0.00";
    se.equityRatio = se.totalAssets
      ? (dataPoint.ownerEquity / dataPoint.totalAssets).toFixed(2)
      : "0.00";
  });

  const socialEnterprises = Array.from(seMap.values());

  const profitOverTimeSeries = socialEnterprises.map((se) => {
    const seenQuarters = new Map();

    se.revenueVsExpenses.forEach((point) => {
      if (!point || typeof point.x !== "string" || typeof point.revenue !== "number" || typeof point.expenses !== "number") return;

      const date = new Date(point.x);
      if (isNaN(date)) return;

      const month = date.getMonth(); // 0-11
      const year = date.getFullYear();
      let quarterKey = '';
      let quarterNum = 0;

      if (month >= 0 && month <= 2) {
        quarterKey = `Q1 ${year}`;
        quarterNum = 1;
      } else if (month >= 3 && month <= 5) {
        quarterKey = `Q2 ${year}`;
        quarterNum = 2;
      } else if (month >= 6 && month <= 8) {
        quarterKey = `Q3 ${year}`;
        quarterNum = 3;
      } else {
        quarterKey = `Q4 ${year}`;
        quarterNum = 4;
      }

      const profit = point.revenue - point.expenses;

      if (!seenQuarters.has(quarterKey)) {
        seenQuarters.set(quarterKey, { totalProfit: profit, count: 1, year: year, quarterNum: quarterNum });
      } else {
        const current = seenQuarters.get(quarterKey);
        seenQuarters.set(quarterKey, {
          totalProfit: current.totalProfit + profit,
          count: current.count + 1,
          year: year,
          quarterNum: quarterNum
        });
      }
    });

    const averagedData = Array.from(seenQuarters.entries())
      .map(([key, { totalProfit, count, year, quarterNum }]) => {
        const averageQuarterlyProfit = totalProfit / count;
        let starRating = 0;

        if (firstAverageProfit === 0) {
          if (averageQuarterlyProfit < 0) {
            starRating = 1;
          } else if (averageQuarterlyProfit > 0) {
            starRating = 3;
          } else {
            starRating = 0;
          }
        } else {
          if (averageQuarterlyProfit < 0) {
            starRating = 1;
          } else if (averageQuarterlyProfit >= 0 && averageQuarterlyProfit < firstAverageProfit) {
            starRating = 2;
          } else if (averageQuarterlyProfit >= firstAverageProfit && averageQuarterlyProfit < 1.5 * firstAverageProfit) {
            starRating = 3;
          } else if (averageQuarterlyProfit >= 1.5 * firstAverageProfit && averageQuarterlyProfit < 2 * firstAverageProfit) {
            starRating = 4;
          } else if (averageQuarterlyProfit >= 2 * firstAverageProfit) {
            starRating = 5;
          }
        }

        starRating = Math.max(0, Math.min(5, Math.round(starRating)));

        return {
          x: `${year} Q${quarterNum}`,
          y: starRating,
          year: year,
          quarterNum: quarterNum
        };
      })
      .sort((a, b) => {
        if (a.year !== b.year) {
          return a.year - b.year;
        }
        return a.quarterNum - b.quarterNum;
      });

    // Calculate overall average raw profit for ranking
    const overallAvgRawProfit = se.revenueVsExpenses.reduce((sum, point) => {
      return sum + (Number(point.revenue ?? 0) - Number(point.expenses ?? 0));
    }, 0) / se.revenueVsExpenses.length;


    return {
      id: se.name,
      data: averagedData,
      overallAvgRawProfit: overallAvgRawProfit,
    };
  });

  const currentYear = new Date().getFullYear();

  // Filter all data to include only quarters up to and including the current year
  const allSocialEnterprisesDataFormatted = profitOverTimeSeries.map(series => {
    return {
      ...series,
      data: series.data.filter(point => point.year <= currentYear)
    };
  });

  // Calculate an overall profitability score for each social enterprise
  const rankedSocialEnterprises = [...allSocialEnterprisesDataFormatted].sort((a, b) => {
    return b.overallAvgRawProfit - a.overallAvgRawProfit;
  });

  // Select the top 5 social enterprises, showing their last 5 quarters
  const top5SocialEnterprisesData = rankedSocialEnterprises.slice(0, 5).map(series => {
    const sortedData = [...series.data].sort((a, b) => {
      if (a.year !== b.year) {
        return a.year - b.year;
      }
      return a.quarterNum - b.quarterNum;
    });

    return {
      id: series.id,
      data: sortedData.slice(-5)
    };
  });

  // Determine which data to display based on showTop5Mode
  let displayedProfitOverTimeSeries;
  let chartTitle = "Profit Over Time (by Social Enterprise)";

  if (showTop5Mode) {
    displayedProfitOverTimeSeries = top5SocialEnterprisesData;
    chartTitle = "Top 5 Social Enterprises by Profit Over Time (Last 5 Quarters)";
  } else {
    displayedProfitOverTimeSeries = allSocialEnterprisesDataFormatted;
    chartTitle = "All Social Enterprises by Profit Over Time (Full History)";
  }


  const highestExpenseSE = socialEnterprises.reduce(
    (max, se) => (se.totalExpenses > (max?.totalExpenses || 0) ? se : max),
    null
  );

  const getExpenseLevel = (amount) => {
    if (amount > 100000) return "High";
    if (amount > 50000) return "Medium";
    return "Low";
  };

  const getExpenseLevelColor = (amount) => {
    if (amount > 100000) return colors?.redAccent?.[400] ?? "#ff5252";
    if (amount > 50000) return colors?.yellowAccent?.[400] ?? "#EDED00";
    return colors?.greenAccent?.[400] ?? "#4caf50";
  };

  const avg = (arr, key) =>
    (arr.reduce((acc, item) => acc + item[key], 0) / arr.length).toFixed(2);

  const revenueVsExpensesData = socialEnterprises
    .map((se) => ({
      id: `${se.name} Revenue`,
      data: se.revenueVsExpenses.map((point) => ({
        x: point.x,
        y: point.revenue,
      })),
    }))
    .concat(
      socialEnterprises.map((se) => ({
        id: `${se.name} Expenses`,
        data: se.revenueVsExpenses.map((point) => ({
          x: point.x,
          y: point.expenses,
        })),
      }))
    );

  // Modified equityTrendData to use quarterly star ratings
  const equityTrendData = socialEnterprises.map((se) => {
    const seenQuartersEquity = new Map();

    se.equityTrend.forEach((point) => {
      if (!point || typeof point.x !== "string" || typeof point.equity !== "number") return;

      const date = new Date(point.x);
      if (isNaN(date)) return;

      const month = date.getMonth();
      const year = date.getFullYear();
      let quarterKey = '';
      let quarterNum = 0;

      if (month >= 0 && month <= 2) {
        quarterKey = `Q1 ${year}`;
        quarterNum = 1;
      } else if (month >= 3 && month <= 5) {
        quarterKey = `Q2 ${year}`;
        quarterNum = 2;
      } else if (month >= 6 && month <= 8) {
        quarterKey = `Q3 ${year}`;
        quarterNum = 3;
      } else {
        quarterKey = `Q4 ${year}`;
        quarterNum = 4;
      }

      const equity = point.equity;

      if (!seenQuartersEquity.has(quarterKey)) {
        seenQuartersEquity.set(quarterKey, { totalEquity: equity, count: 1, year: year, quarterNum: quarterNum });
      } else {
        const current = seenQuartersEquity.get(quarterKey);
        seenQuartersEquity.set(quarterKey, {
          totalEquity: current.totalEquity + equity,
          count: current.count + 1,
          year: year,
          quarterNum: quarterNum
        });
      }
    });

    const averagedEquityData = Array.from(seenQuartersEquity.entries())
      .map(([key, { totalEquity, count, year, quarterNum }]) => {
        const averageQuarterlyEquity = totalEquity / count;
        let starRating = 0;

        // Star rating logic for equity
        if (firstAverageEquity === 0) {
          if (averageQuarterlyEquity < 0) {
            starRating = 1;
          } else if (averageQuarterlyEquity > 0) {
            starRating = 3;
          } else {
            starRating = 0;
          }
        } else {
          if (averageQuarterlyEquity < 0) {
            starRating = 1;
          } else if (averageQuarterlyEquity >= 0 && averageQuarterlyEquity < firstAverageEquity * 0.5) {
            starRating = 2;
          } else if (averageQuarterlyEquity >= firstAverageEquity * 0.5 && averageQuarterlyEquity < firstAverageEquity * 1.0) {
            starRating = 3;
          } else if (averageQuarterlyEquity >= firstAverageEquity * 1.0 && averageQuarterlyEquity < firstAverageEquity * 1.5) {
            starRating = 4;
          } else if (averageQuarterlyEquity >= firstAverageEquity * 1.5) {
            starRating = 5;
          }
        }

        starRating = Math.max(0, Math.min(5, Math.round(starRating)));

        return {
          x: `${year} Q${quarterNum}`,
          y: starRating,
          year: year,
          quarterNum: quarterNum
        };
      })
      .sort((a, b) => {
        if (a.year !== b.year) {
          return a.year - b.year;
        }
        return a.quarterNum - b.quarterNum;
      });

    // Calculate overall average raw equity for ranking
    const overallAvgRawEquity = se.equityTrend.reduce((sum, point) => {
      return sum + (Number(point.equity ?? 0));
    }, 0) / se.equityTrend.length;

    return {
      id: se.name,
      data: averagedEquityData,
      overallAvgRawEquity: overallAvgRawEquity, // Add this for ranking
    };
  });

  // Filter all equity data to include only quarters up to and including the current year
  const allSocialEnterprisesEquityDataFormatted = equityTrendData.map(series => {
    return {
      ...series,
      data: series.data.filter(point => point.year <= currentYear)
    };
  });

  // Calculate an overall equity score for each social enterprise and rank them
  const rankedSocialEnterprisesByEquity = [...allSocialEnterprisesEquityDataFormatted].sort((a, b) => {
    return b.overallAvgRawEquity - a.overallAvgRawEquity;
  });

  // Select the top 5 social enterprises by equity, showing their last 5 quarters
  const top5SocialEnterprisesEquityData = rankedSocialEnterprisesByEquity.slice(0, 5).map(series => {
    const sortedData = [...series.data].sort((a, b) => {
      if (a.year !== b.year) {
        return a.year - b.year;
      }
      return a.quarterNum - b.quarterNum;
    });

    return {
      id: series.id,
      data: sortedData.slice(-5) // Last 5 quarters
    };
  });

  // Determine which data to display based on showTop5ModeEquity
  let displayedEquityOverTimeSeries;
  let equityChartTitle = "Equity Star Rating Over Time (by Social Enterprise)";

  if (showTop5ModeEquity) {
    displayedEquityOverTimeSeries = top5SocialEnterprisesEquityData;
    equityChartTitle = "Top 5 Social Enterprises by Equity Trend";
  } else {
    displayedEquityOverTimeSeries = allSocialEnterprisesEquityDataFormatted;
    equityChartTitle = "Equity Trend by Social Enterprises";
  }


  const cashFlowMap = new Map();
  const [selectedSE1, setSelectedSE1] = useState("");
  const [selectedSE2, setSelectedSE2] = useState("");

  cashFlowRaw.forEach((item) => {
    const name = item.se_abbr ?? item.se_id;
    const date = new Date(item.date).toLocaleDateString();

    if (!cashFlowMap.has(name)) {
      cashFlowMap.set(name, {
        inflow: [],
        outflow: [],
      });
    }

    const entry = cashFlowMap.get(name);
    entry.inflow.push({ x: date, y: Number(item.inflow) });
    entry.outflow.push({ x: date, y: Number(item.outflow) });
  });

  const cashFlowData = [];

  if (selectedSE1 && cashFlowMap.has(selectedSE1)) {
    const value = cashFlowMap.get(selectedSE1);
    cashFlowData.push(
      { id: `${selectedSE1} Inflow`, data: value.inflow },
      { id: `${selectedSE1} Outflow`, data: value.outflow }
    );
  }

  if (
    selectedSE2 &&
    cashFlowMap.has(selectedSE2) &&
    selectedSE2 !== selectedSE1
  ) {
    const value = cashFlowMap.get(selectedSE2);
    cashFlowData.push(
      { id: `${selectedSE2} Inflow`, data: value.inflow },
      { id: `${selectedSE2} Outflow`, data: value.outflow }
    );
  }

  const inventoryPerSE = {};
  inventoryData.forEach(({ se_abbr, item_name, qty }) => {
    if (!inventoryPerSE[se_abbr]) {
      inventoryPerSE[se_abbr] = 0;
    }
    inventoryPerSE[se_abbr] += qty;
  });

  const inventoryBreakdownData = Object.entries(inventoryPerSE)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([se_abbr, qty]) => ({
      id: se_abbr,
      value: qty,
    }));

  const validDates = financialData
    .map((item) => item.date)
    .filter((date) => date != null)
    .map((date) => new Date(date).toISOString().split("T")[0]);

  const latestDateOnly =
    validDates.length > 0
      ? validDates.sort().reverse()[0]
      : null;

  const latestRecordsMap = new Map();

  financialData.forEach((item) => {
    const name = item.se_abbr ?? item.se_id;
    const date = new Date(item.date);

    if (
      !latestRecordsMap.has(name) ||
      date > new Date(latestRecordsMap.get(name).date)
    ) {
      latestRecordsMap.set(name, item);
    }
  });

  const latestRecords = Array.from(latestRecordsMap.values());

  const latestRevenueRecords = latestRecords.map((item) => ({
    name: item.se_abbr ?? item.se_id,
    revenue: Number(item.total_revenue || 0),
  }));

  const aggregatedLatestSEs = new Map();

  latestRecords.forEach((item) => {
    const name = item.se_abbr ?? item.se_id;
    const revenue = Number(item.total_revenue || 0);
    const expenses = Number(item.total_expenses || 0);

    if (!aggregatedLatestSEs.has(name)) {
      aggregatedLatestSEs.set(name, {
        name,
        revenue,
        profit: revenue - expenses,
      });
    } else {
      const existing = aggregatedLatestSEs.get(name);
      existing.revenue += revenue;
      existing.profit += revenue - expenses;
    }
  });

  const topRevenueSEsData = [...aggregatedLatestSEs.values()]
    .sort((a, b) => b.revenue - a.revenue)
    .slice(0, 10);

  const worstRevenueSEsData = [...aggregatedLatestSEs.values()]
    .sort((a, b) => a.revenue - b.revenue)
    .slice(0, 10);

  const latestProfitRecords = latestRecords.map((item) => ({
    name: item.se_abbr ?? item.se_id,
    profit: Number(item.total_revenue || 0) - Number(item.total_expenses || 0),
  }));

  const mostProfitSEsData = latestProfitRecords
    .sort((a, b) => b.profit - a.profit)
    .slice(0, 3);

  const individualInventoryData = Object.entries(
    inventoryData.reduce((acc, { item_name, qty }) => {
      if (item_name && typeof qty === "number" && !isNaN(qty)) {
        acc[item_name] = (acc[item_name] || 0) + qty;
      }
      return acc;
    }, {})
  )
    .map(([name, value]) => ({
      id: name,
      value: typeof value === "number" && !isNaN(value) ? value : 0,
    }))
    .filter((item) => item.value > 0)
    .sort((a, b) => b.value - a.value)
    .slice(0, 10);

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
        <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <StatBox
            title={`₱${socialEnterprises
              .reduce((sum, se) => sum + Number(se.totalRevenue || 0), 0)
              .toLocaleString()}`}
            subtitle="Total Revenue (All SEs)"
            progress={1}
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
            title={`₱${socialEnterprises
              .reduce((sum, se) => sum + Number(se.totalExpenses || 0), 0)
              .toLocaleString()}`}
            subtitle="Total Expenses (All SEs)"
            progress={1}
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
            title={`₱${socialEnterprises
              .reduce((sum, se) => sum + Number(se.netIncome || 0), 0)
              .toLocaleString()}`}
            subtitle="Net Income (All SEs)"
            progress={1}
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
            title={`₱${socialEnterprises
              .reduce((sum, se) => sum + Number(se.totalAssets || 0), 0)
              .toLocaleString()}`}
            subtitle="Total Assets (All SEs)"
            progress={1}
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
            title={`₱${Number(
              highestExpenseSE?.totalExpenses || 0
            ).toLocaleString()}`}
            subtitle={`Highest Expenses: ${highestExpenseSE?.name || "N/A"
              } (${getExpenseLevel(highestExpenseSE?.totalExpenses)})`}
            progress={0.9}
            increase={
              <Typography
                sx={{
                  color: getExpenseLevelColor(
                    highestExpenseSE?.totalExpenses || 0
                  ),
                  fontWeight: "bold",
                }}
              >
                ↑ {getExpenseLevel(highestExpenseSE?.totalExpenses)}
              </Typography>
            }
            icon={
              <MoneyOffOutlinedIcon
                sx={{
                  fontSize: "32px",
                  color: getExpenseLevelColor(
                    highestExpenseSE?.totalExpenses || 0
                  ),
                }}
              />
            }
          />
        </Box>
      </Box>

      {/* Row 3 - Star Trend Chart */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        {/* Let the child own its header, button, loading, and empty states */}
        <FinancialPerformanceTrendChart
          // optionally pass a range; omit if you want all history
          // from="2025-01-01"
          // to="2025-07-01"   // end-exclusive on your backend
          isDashboard={false}
        />
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

      {/* Row 7 - Financial Heatmap */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <FinanceRiskHeatmap />
      </Box>

      {/* Row 7 - Inventory Turnover Ratio by SE */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <Typography
          variant="h3"
          fontWeight="bold"
          color={colors.greenAccent[500]}
        >
          Inventory Turnover Ratio (Worst 5)
        </Typography>
        <Box height="400px">
          <InventoryTurnoverBar data={inventoryTurnoverData} />
        </Box>
      </Box>

      {/* Row 8 - Equity Trend Comparison */}
      <Box backgroundColor={colors.primary[400]} p="20px" mt="20px">
        <Box display="flex" justifyContent="space-between" alignItems="center" mb="10px">
          <Typography
            variant="h3"
            fontWeight="bold"
            color={colors.greenAccent[500]}
          >
            {equityChartTitle}
          </Typography>
          <Button
            onClick={() => setShowTop5ModeEquity(!showTop5ModeEquity)}
            variant="contained"
            sx={{
              backgroundColor: colors.blueAccent[700],
              color: colors.grey[100],
              "&:hover": {
                backgroundColor: colors.blueAccent[800],
              },
            }}
          >
            {showTop5ModeEquity ? "Show All History" : "Show Top 5"}
          </Button>
        </Box>
        <Box height="400px">
          <LineChart data={displayedEquityOverTimeSeries} />
        </Box>
      </Box>

      {/* Top 10 SEs by Revenue */}
      <Box backgroundColor={colors.primary[400]} p="40px" mt="20px">
        <Typography
          variant="h3"
          fontWeight="bold"
          color={colors.greenAccent[500]}
        >
          Top 10 Social Enterprises by Revenue
        </Typography>
        <Box height="400px">
          <FinancialBarChart
            data={topRevenueSEsData}
            dataKey="revenue"
            label="Top Revenue"
          />
        </Box>
      </Box>

      {/* Worst 5 SEs by Revenue */}
      <Box backgroundColor={colors.primary[400]} p="40px" mt="20px">
        <Typography
          variant="h3"
          fontWeight="bold"
          color={colors.greenAccent[500]}
        >
          Worst 10 Social Enterprises by Revenue
        </Typography>
        <Box height="400px">
          <FinancialBarChart
            data={worstRevenueSEsData}
            dataKey="revenue"
            label="Worst Revenue"
          />
        </Box>
      </Box>

      {/* Most Profitable SEs */}
      <Box backgroundColor={colors.primary[400]} p="40px" mt="20px">
        <Typography
          variant="h3"
          fontWeight="bold"
          color={colors.greenAccent[500]}
        >
          Leaderboard: Most Profitable Social Enterprises
        </Typography>
        <Box height="400px">
          <FinancialBarChart
            data={mostProfitSEsData}
            dataKey="profit"
            label="Most Profit"
          />
        </Box>
      </Box>
    </Box>
  );
};

export default FinancialAnalytics;