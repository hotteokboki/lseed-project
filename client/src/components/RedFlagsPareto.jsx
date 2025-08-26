// components/RedFlagsPareto.jsx   (Category Health overview, program_id-based)
import { Box, CircularProgress, Typography, useTheme } from "@mui/material";
import { ResponsiveBar } from "@nivo/bar";
import { useEffect, useState, useMemo } from "react";
import axiosClient from "../api/axiosClient";
import { useAuth } from "../context/authContext";
import { tokens } from "../theme";

const RedFlagsPareto = ({ height = 240 }) => {
  const [cats, setCats] = useState([]);
  const [meta, setMeta] = useState({
    seCount: 0,
    eligible: 0,  // seWithFinancials
    noData: 0,    // noDataSeCount
    flaggedCount: 0,
    healthySeCount: 0,
    moderateSeCount: 0,
  });
  const [scopeLabel, setScopeLabel] = useState("All programs");
  const [loading, setLoading] = useState(true);

  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth();
  const roles = user?.roles ?? [];
  const isCoordinator = roles.includes("LSEED-Coordinator");

  useEffect(() => {
    let cancelled = false;

    (async () => {
      setLoading(true);
      try {
        let query = "";
        let label = "All programs";

        if (isCoordinator) {
          const r = await axiosClient.get("/api/get-program-coordinator");
          const rec = r?.data?.[0] ?? {};
          const programId = rec.program_id || rec.programId || rec.id;
          if (programId) {
            query = `?program_id=${encodeURIComponent(programId)}`;
            label = rec.name || rec.program_name || "Your program";
          } else {
            label = "Your program";
          }
        }

        setScopeLabel(label);

        const resp = await axiosClient.get(`/api/red-flags-overview${query}`);
        if (cancelled) return;

        const p = resp?.data ?? {};
        const categories = Array.isArray(p.categories) ? p.categories : [];

        setCats(categories);
        setMeta({
          seCount: Number(p.seCount || 0),
          eligible: Number(p.seWithFinancials || 0),
          noData:
            p.noDataSeCount != null
              ? Number(p.noDataSeCount)
              : Math.max(Number(p.seCount || 0) - Number(p.seWithFinancials || 0), 0),
          flaggedCount: Number(p.flaggedCount || 0),
          healthySeCount: Number(p.healthySeCount || 0),
          moderateSeCount: Number(p.moderateSeCount || 0),
        });
      } catch (e) {
        if (!cancelled) {
          console.error("❌ overview fetch failed:", e);
          setCats([]);
          setMeta({ seCount: 0, eligible: 0, noData: 0, flaggedCount: 0, healthySeCount: 0, moderateSeCount: 0 });
          setScopeLabel(isCoordinator ? "Your program" : "All programs");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => { cancelled = true; };
  }, [isCoordinator]);

  // Build chart rows safely
  const data = useMemo(() => {
    return (cats || []).map(c => {
      const redPct = Number(c.redPct || 0);
      const modPct = Number(c.moderatePct || 0);
      const healPct = Number(c.healthyPct || 0);
      return {
        category: String(c.category ?? ""),
        red: redPct,
        moderate: modPct,
        healthy: healPct,
        _redCount: Number(c.red || 0),
        _modCount: Number(c.moderate || 0),
        _healthyCount: Number(c.healthy || 0),
      };
    });
  }, [cats]);

  // Detect empty/zero data states
  const hasAnyCategory = data.length > 0;
  const hasEligible = meta.eligible > 0;
  const hasAnyBars = data.some(d =>
    (d.red ?? 0) > 0 || (d.moderate ?? 0) > 0 || (d.healthy ?? 0) > 0
  );

  const isEmpty = !hasAnyCategory || !hasEligible || !hasAnyBars;

  if (loading) {
    return (
      <Box sx={{ height, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 2 }}>
        <CircularProgress size={60} sx={{ color: colors.greenAccent[500], "& .MuiCircularProgress-circle": { strokeLinecap: "round" } }} />
        <Typography variant="h6" color={colors.grey[300]}>Loading category health…</Typography>
      </Box>
    );
  }

  if (isEmpty) {
    return (
      <Box sx={{ width: "100%", height, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", textAlign: "center", px: 2 }}>
        <Typography variant="h6" color="white">
          Financial Category Overview Unavailable
        </Typography>
        <Typography variant="body2" color={colors.grey[300]} sx={{ mt: 1 }}>
          Scope: {scopeLabel} • Total SEs: {meta.seCount} • With financials: {meta.eligible} • No data: {meta.noData}
        </Typography>
        <Typography variant="caption" color={colors.grey[400]} sx={{ mt: 0.5 }}>
          Tip: Upload complete reports (cash-in, cash-out, inventory) for at least one month in the period.
        </Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ width: "100%", height }}>
      <Typography variant="h6" textAlign="center" sx={{ mb: 1 }} color="white">
        Category Health Across SEs (last 3 complete months)
      </Typography>

      <Box sx={{ width: "100%", height: `calc(${height}px - 40px)` }}>
        <ResponsiveBar
          data={data}
          keys={["red", "moderate", "healthy"]}
          indexBy="category"
          layout="horizontal"
          margin={{ top: 6, right: 12, bottom: 60, left: 120 }}
          padding={0.28}
          groupMode="stacked"
          enableGridX
          maxValue={100}
          colors={({ id }) =>
            id === "red" ? colors.redAccent[500]
            : id === "moderate" ? "#f59e0b"
            : colors.greenAccent[500]
          }
          borderRadius={6}
          enableLabel
          labelSkipWidth={30}
          labelSkipHeight={20}
          label={d => `${Math.round(Number(d.value || 0))}%`}
          labelTextColor="#111827"
          axisBottom={{ legend: "% of SEs (with financials)", legendOffset: 42, legendPosition: "middle" }}
          axisLeft={{ tickPadding: 6 }}
          theme={{
            axis: {
              ticks: { text: { fill: colors.primary[100] } },
              legend: { text: { fill: colors.primary[100] } },
            },
            tooltip: { container: { background: "#111827", color: "#fff" } },
          }}
          tooltip={({ data, id, color }) => (
            <Box
              sx={{
                p: 1.25,
                bgcolor: "#0b1220",
                color: "#fff",
                borderRadius: "10px",
                border: "1px solid rgba(255,255,255,0.12)",
                boxShadow: "0 8px 24px rgba(0,0,0,.4)",
                minWidth: 240,
              }}
            >
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 0.5 }}>
                <Box sx={{ width: 10, height: 10, bgcolor: color, borderRadius: 2 }} />
                <Typography variant="body2" sx={{ fontWeight: 700, color: "#fff" }}>
                  {data.category}
                </Typography>
              </Box>
              <Typography variant="caption" sx={{ color: "#fff" }}>
                {id === "red"      && `${data._redCount} of ${meta.eligible} SEs with financials (${Math.round(Number(data.red || 0))}%) red (≤ 1.5)`}
                {id === "moderate" && `${data._modCount} of ${meta.eligible} SEs with financials (${Math.round(Number(data.moderate || 0))}%) moderate (1.5–3.0)`}
                {id === "healthy"  && `${data._healthyCount} of ${meta.eligible} SEs with financials (${Math.round(Number(data.healthy || 0))}%) healthy (> 3.0)`}
              </Typography>
            </Box>
          )}
          animate
          motionConfig="gentle"
        />
      </Box>

      <Typography variant="body2" color={colors.grey[300]} sx={{ mt: 1 }}>
        Scope: {scopeLabel} • Total SEs: {meta.seCount} • With financials: {meta.eligible} • No data: {meta.noData} •
        Flagged (≥3 red): {meta.flaggedCount} • Healthy (all &gt; 3): {meta.healthySeCount} • Moderate: {meta.moderateSeCount}
      </Typography>
    </Box>
  );
};

export default RedFlagsPareto;