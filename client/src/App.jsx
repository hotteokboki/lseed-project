import { Box } from "@mui/material";
import { Routes, Route, Navigate, Outlet } from "react-router-dom";
import { AuthContextProvider, useAuth } from "./context/authContext";
import Login from "./scenes/login";
import Dashboard from "./scenes/dashboard";
import SocialEnterprise from "./scenes/socialenterprise";
import Mentors from "./scenes/mentors";
import Admin from "./scenes/admin";
import ProgramPage from "./scenes/programs";
import Reports from "./scenes/reports";
import Scheduling from "./scenes/scheduling";
import EvaluatePage from "./scenes/assess";
import SEAnalytics from "./scenes/seanalytics";
import MentorAnalytics from "./scenes/mentoranalytics";
import Mentorships from "./scenes/mentorships";
import AuditLogs from "./scenes/audit-logs";
import Unauthorized from "./scenes/unauthorized";
import ProfilePage from "./scenes/profile";
import { ColorModeContext, useMode } from "./theme";
import { ThemeProvider } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
import LSEEDSignup from "./scenes/lseed-signup";
import { GoogleOAuthProvider } from "@react-oauth/google";
import ForgotPassword from "./scenes/forgotpassword";
import ResetPassword from "./scenes/resetpassword";
import PublicLayout from "./layouts/PublicLayout";
import AppLayout from "./layouts/AppLayout";
import CollaborationDashboard from "./scenes/collaborationdashboard";
import AnalyticsHub from "./scenes/analytics-hub";

const App = () => {
  const [theme, colorMode] = useMode();

  return (
    <GoogleOAuthProvider clientId="1025918978584-niisk93pun37oujtrjdkpra1cn1b8esv.apps.googleusercontent.com">
      <AuthContextProvider>
        <ColorModeContext.Provider value={colorMode}>
          <ThemeProvider theme={theme}>
            <CssBaseline />
            <Box sx={{ display: "flex", minHeight: "100vh" }}>
              <MainContent />
            </Box>
          </ThemeProvider>
        </ColorModeContext.Provider>
      </AuthContextProvider>
    </GoogleOAuthProvider>
  );
};

const ProtectedRoute = ({ allowedRoles }) => {
  const { user } = useAuth();

  if (!user) {
    return <Navigate to="/" replace />;
  }

  // Assuming user.roles is an array from the backend
  const userRoles = Array.isArray(user.roles) ? user.roles : user.roles.split(',').map(role => role.trim());


  // Define roles that have full access to all protected routes
  const privilegedRoles = ["LSEED-Director"];

  const hasPrivilegedAccess = userRoles.some((role) =>
    privilegedRoles.includes(role)
  );

  const isAllowed = hasPrivilegedAccess || userRoles.some((role) =>
    allowedRoles.includes(role)
  );


  if (!isAllowed) {
    return <Navigate to="/unauthorized" replace />;
  }

  return <Outlet />;
};

const MainContent = () => {
  const { user, loading, isMentorView } = useAuth();
  if (loading) return <div>Loading...</div>;

  return (
    <Routes>

      {/* PUBLIC ROUTES WITHOUT SIDEBAR/TOPBAR */}
      <Route element={<PublicLayout />}>
        <Route path="/lseed-signup" element={<LSEEDSignup />} />
        <Route
          path="/"
          element={
            user ? (
              user.roles.includes("Administrator") ? (
                <Navigate to="/admin" />
              ) : isMentorView ? (
                <Navigate to="/dashboard/mentor" />
              ) : (
                <Navigate to="/dashboard/lseed" />
              )
            ) : (
              <Login />
            )
          }
        />
        <Route path="/forgot-password" element={<ForgotPassword />} />
        <Route path="/reset-password" element={<ResetPassword />} />
        <Route path="/unauthorized" element={<Unauthorized />} />
      </Route>

      {/* PROTECTED ROUTES WITH SIDEBAR/TOPBAR */}
      <Route element={<AppLayout />}>
        <Route path="/profile" element={<ProfilePage />} />
        <Route path="/dashboard" element={<Navigate to={isMentorView ? "/dashboard/mentor" : "/dashboard/lseed"} replace />} />
        <Route path="/dashboard/lseed" element={<Dashboard />} />
        <Route path="/dashboard/mentor" element={<Dashboard />} />

        <Route element={<ProtectedRoute allowedRoles={["LSEED-Director", "LSEED-Coordinator"]} />}>
          <Route path="/socialenterprise" element={<SocialEnterprise />} />
          <Route path="/mentors" element={<Mentors />} />
          <Route path="/programs" element={<ProgramPage />} />
          <Route path="/reports" element={<Reports />} />
          <Route path="/evaluate" element={<EvaluatePage />} />
          <Route path="/analytics" element={<AnalyticsHub />} />
        </Route>

        <Route element={<ProtectedRoute allowedRoles={["LSEED-Director", "LSEED-Coordinator", "Mentor"]} />}>
          <Route path="/assess" element={<EvaluatePage />} />
        </Route>

        <Route element={<ProtectedRoute allowedRoles={["Mentor"]} />}>
          <Route path="/mentorships" element={<Mentorships />} />
          <Route path="/mentor-analytics/:id" element={<MentorAnalytics />} />
        </Route>

        <Route element={<ProtectedRoute allowedRoles={["Administrator", "LSEED-Director",]} />}>
          <Route path="/admin" element={<Admin />} />
          <Route path="/audit-logs" element={<AuditLogs />} />
        </Route>

        <Route element={<ProtectedRoute allowedRoles={["LSEED-Director", "LSEED-Coordinator", "Mentor"]} />}>
          <Route path="/scheduling" element={<Scheduling />} />
        </Route>

        <Route element={<ProtectedRoute allowedRoles={["LSEED-Coordinator", "Mentor", "Guest User", "LSEED-Director"]} />}>
          <Route path="/se-analytics/:id" element={<SEAnalytics />} />
        </Route>

        <Route element={<ProtectedRoute allowedRoles={["Mentor"]} />}>
          <Route path="/collaboration-dashboard" element={<CollaborationDashboard />} />
        </Route>

        <Route path="*" element={<Navigate to={user ? "/dashboard" : "/"} />} />
      </Route>

    </Routes>
  );
};


export default App;