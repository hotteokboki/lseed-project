nn// src/pages/AnalyticsHub.jsx
import { Box, Tab, Tabs, useTheme } from "@mui/material";
import React, { Suspense } from "react";
import { useSearchParams } from "react-router-dom";
import { tokens } from "../../theme";

// Lazy-load the heavy pages
const EvaluationAnalytics = React.lazy(() => import("../analytics/index"));
const FinancialAnalytics = React.lazy(() => import("../financial-analytics/index"));

const AnalyticsHub = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [searchParams, setSearchParams] = useSearchParams();

  // Default to "evaluation" when no query ?view= is present
  const viewParam = searchParams.get("view");
  const view = viewParam === "financial" ? "financial" : "evaluation";

  const handleTabChange = (_e, next) => {
    if (!next) return;
    // Keep URLs clean: omit the param for the default "evaluation"
    setSearchParams(next === "evaluation" ? {} : { view: next });
  };

  return (
    <Box m="20px">
      <Box
        sx={{
          backgroundColor: colors.primary[400],
          borderRadius: 2,
          px: 2,
          pt: 1,
        }}
      >
        <Tabs
          value={view}
          onChange={handleTabChange}
          textColor="inherit"
          indicatorColor="secondary"
          aria-label="Select analytics type"
        >
          <Tab label="Evaluation" value="evaluation" />
          <Tab label="Financial" value="financial" />
        </Tabs>
      </Box>

      <Box mt={2}>
        <Suspense fallback={<Box p={3}>Loading…</Box>}>
          {view === "evaluation" ? <EvaluationAnalytics /> : <FinancialAnalytics />}
        </Suspense>
      </Box>
    </Box>
  );
};

export default AnalyticsHub;