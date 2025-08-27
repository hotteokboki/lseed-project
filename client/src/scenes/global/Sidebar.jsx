import AdminPanelSettingsOutlinedIcon from "@mui/icons-material/AdminPanelSettingsOutlined";
import AnalyticsOutlinedIcon from "@mui/icons-material/AnalyticsOutlined";
import AssignmentTurnedInOutlinedIcon from "@mui/icons-material/AssignmentTurnedInOutlined";
import CalendarMonthOutlinedIcon from "@mui/icons-material/CalendarMonthOutlined";
import DescriptionOutlinedIcon from "@mui/icons-material/DescriptionOutlined";
import Diversity2OutlinedIcon from "@mui/icons-material/Diversity2Outlined";
import ExitToAppOutlinedIcon from "@mui/icons-material/ExitToAppOutlined";
import FactCheckOutlinedIcon from "@mui/icons-material/FactCheckOutlined";
import GradingOutlinedIcon from "@mui/icons-material/GradingOutlined";
import GridViewOutlinedIcon from "@mui/icons-material/GridViewOutlined";
import HandshakeOutlinedIcon from "@mui/icons-material/HandshakeOutlined";
import MenuOutlinedIcon from "@mui/icons-material/MenuOutlined";
import SettingsAccessibilityOutlinedIcon from "@mui/icons-material/SettingsAccessibilityOutlined";
import SupervisorAccountOutlinedIcon from "@mui/icons-material/SupervisorAccountOutlined";
import {
  Avatar,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogTitle,
  IconButton,
  Stack,
  Typography,
  useTheme
} from "@mui/material";
import { alpha } from "@mui/material/styles";
import { useGoogleLogin } from "@react-oauth/google";
import { useEffect, useState } from "react";
import { Menu, MenuItem, ProSidebar } from "react-pro-sidebar";
import "react-pro-sidebar/dist/css/styles.css";
import { Link, useLocation, useNavigate } from "react-router-dom";
import axiosClient from "../../api/axiosClient";
import { createCalendarEvents } from "../../components/googleCalendar";
import { useAuth } from "../../context/authContext";
import { tokens } from "../../theme";

const Item = ({ title, to, icon, selected, setSelected }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  return (
    <MenuItem
      active={selected === title}
      style={{
        color: colors.grey[100],
        fontWeight: selected === title ? "bold" : "normal",
      }}
      onClick={() => setSelected(title)}
      icon={icon}
    >
      <Typography variant="body1">{title}</Typography>
      <Link to={to} />
    </MenuItem>
  );
};

// ⭐️
const Sidebar = ({ }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [isCollapsed, setIsCollapsed] = useState(true);
  const navigate = useNavigate();
  const location = useLocation();
  const [googleUser, setGoogleUser] = useState(null);
  const [openDialog, setOpenDialog] = useState(false);

  const { user, logout, isMentorView } = useAuth();

  const isCoordinatorView = !isMentorView;

  // ⭐️ STEP 2: Correctly check roles from the `user.role` array
  const userRoles = user?.roles || []; // Fallback to an empty array for safety
  const isLSEEDCoordinator = userRoles.includes("LSEED-Coordinator");
  const hasMentorRole = userRoles.includes("Mentor");
  const isAdministrator = userRoles.includes("Administrator");
  const isLSEEDUser = userRoles.some((role) => role.startsWith("LSEED"));
  const isLSEEDDirector = userRoles.includes("LSEED-Director");
  const shouldShowMinimalSidebar = isAdministrator && !isLSEEDDirector;
  const [assignedPrograms, setAssignedPrograms] = useState([]);

  // ⭐️ Add this useEffect hook for debugging
  useEffect(() => {
    // Set up the interval to log the prop every 5 seconds (5000 milliseconds)
    const intervalId = setInterval(() => {
      console.log(
        "Sidebar prop received: isCoordinatorView =",
        isCoordinatorView
      );
    }, 5000);

    return () => {
      clearInterval(intervalId);
    };
  }, [isCoordinatorView]); // Add isCoordinatorView as a dependency

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        if (!isLSEEDCoordinator) { setAssignedPrograms([]); return; }
        const { data } = await axiosClient.get("/api/users/me/program-assignments");
        if (alive) setAssignedPrograms(Array.isArray(data) ? data : []);
      } catch (e) {
        console.error("load assigned programs:", e?.response?.data || e.message);
        if (alive) setAssignedPrograms([]);
      }
    })();
    return () => { alive = false; };
  }, [isLSEEDCoordinator]);

  // Determine the default selected item based on the current route
  const getSelectedTitle = () => {
    const routeMap = {
      "/dashboard/lseed": "Dashboard",
      "/dashboard/mentor": "Dashboard",
      "/assess": "Evaluate",
      "/socialenterprise": "Manage SE",
      "/mentors": "LSEED Mentors",
      "/analytics": "Show Analytics",
      "/reports": "Show Reports",
      "/scheduling": "Scheduling Matrix",
      "/admin": "Manage Users",
      "/mentorships": "Manage Mentorships",
      "/analytics-mentorship": "Show Analytics",
      "/programs": "Manage Programs",
      "/signup": "Register Mentor",
      "/collaboration-dashboard": "Collaboration Portal",
    };
    return routeMap[location.pathname] || "Dashboard";
  };

  const login = useGoogleLogin({
    onSuccess: (response) => {
      console.log("Google Login Success:", response);
      setGoogleUser(response);
    },
    onError: (error) => console.error("Google Login Failed", error),
    scope: "https://www.googleapis.com/auth/calendar.events",
  });

  const handleCloseDialog = () => {
    setOpenDialog(false);
  };

  const handleConfirmNavigation = async () => {
    if (!googleUser) {
      login();
    } else {
      await createCalendarEvents(googleUser, user);
      navigate("/scheduling");
      handleCloseDialog();
    }
  };

  const confirmNavNoSync = async () => {
    navigate("/scheduling");
    handleCloseDialog();
  };

  const [selected, setSelected] = useState("");
  useEffect(() => {
    setSelected(getSelectedTitle());
  }, [location.pathname]);

  return (
    <Box
      sx={{
        height: "100vh",
        position: "sticky",
        top: 0,
        left: 0,
        background: colors.primary[400],
        "& .pro-sidebar-inner": {
          background: `${colors.primary[400]} !important`,
        },
        "& .pro-icon-wrapper": {
          backgroundColor: "transparent !important",
        },
        "& .pro-inner-item": {
          padding: "4px 25px 8px 20px !important",
          borderRadius: "8px",
          transition: "all 0.3s ease-in-out",
        },
        "& .pro-inner-item:hover": {
          backgroundColor: colors.grey[700],
          color: "#fff !important",
        },
        "& .pro-menu-item.active": {
          backgroundColor: colors.greenAccent[600],
          color: "#fff !important",
          borderRadius: "8px",
        },
      }}
    >
      <ProSidebar collapsed={isCollapsed}>
        <Menu iconShape="square">
          {/* Header Section */}
          <MenuItem
            onClick={() => setIsCollapsed(!isCollapsed)}
            icon={isCollapsed ? <MenuOutlinedIcon /> : undefined}
            style={{ margin: "15px 0", color: colors.grey[100] }}
          >
            {!isCollapsed && (
              <Box
                display="flex"
                justifyContent="space-between"
                alignItems="center"
                ml="15px"
              >
                <Typography variant="h3" color={colors.grey[100]}>
                  LSEED INSIGHT
                </Typography>
                <IconButton onClick={() => setIsCollapsed(!isCollapsed)}>
                  <MenuOutlinedIcon />
                </IconButton>
              </Box>
            )}
          </MenuItem>

          {/* Profile Section */}
          {!isCollapsed && user && (
            <Box px={2.5} py={2} textAlign="center">
              {/* Avatar */}
              <Avatar
                alt={`${user.firstName || "User"} ${user.lastName || ""}`.trim()}
                src={user.photoUrl || "/assets/Picture.png"} // put Picture.png in /public/assets if using Vite/CRA
                sx={{
                  width: 92,
                  height: 92,
                  mx: "auto",
                  border: `2px solid ${colors.grey[100]}`,
                  boxShadow: "0 6px 18px rgba(0,0,0,.25)",
                }}
              />

              {/* Name */}
              <Typography
                variant="h6"
                sx={{
                  mt: 1,
                  fontWeight: 700,
                  color: colors.grey[100],
                  lineHeight: 1.2,
                  maxWidth: 240,
                  mx: "auto",
                }}
              >
                {(user.firstName || "User") + " " + (user.lastName || "User")}
              </Typography>

              {/* Role (its own row) */}
              <Typography variant="body2" sx={{ mt: 0.5, color: colors.greenAccent[500] }}>
                {userRoles?.length ? userRoles.join(" / ") : "No Role Assigned"}
              </Typography>

              {/* Programs (separate row, wraps cleanly) */}
              {isLSEEDCoordinator && assignedPrograms?.length > 0 && (
                <Stack
                  direction="row"
                  spacing={0.75}
                  justifyContent="center"
                  alignItems="center"
                  useFlexGap
                  flexWrap="wrap"
                  sx={{ mt: 1 }}
                >
                  {assignedPrograms.map((p) => (
                    <Chip
                      key={p.program_id}
                      size="small"
                      label={p.program_name} // make sure your query returns program_name
                      variant="outlined"
                      sx={{
                        borderColor: alpha(colors.blueAccent[500], 0.6),
                        color: colors.blueAccent[300],
                        bgcolor: alpha(colors.blueAccent[500], 0.15),
                        fontWeight: 700,
                        height: 22,
                        "& .MuiChip-label": { px: 1.25, py: 0.25 },
                      }}
                    />
                  ))}
                </Stack>
              )}
            </Box>
          )}

          {/* Navigation Items */}
          <Box paddingLeft={isCollapsed ? undefined : "10%"}>
            {/* ⭐️ Conditional rendering based on the isCoordinatorView state from context */}
            {shouldShowMinimalSidebar ? (
              // 🔒 Minimal Sidebar for Administrator
              <>
                <Item
                  title="Manage Users"
                  to="/admin"
                  icon={<AdminPanelSettingsOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Show Audit Logs"
                  to="/audit-logs"
                  icon={<DescriptionOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
              </>
            ) : isCoordinatorView ? (
              // 🧑‍💼 Coordinator or Director View
              <>
                <Item
                  title="Dashboard"
                  to="/dashboard/lseed"
                  icon={<GridViewOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Evaluate"
                  to="/assess"
                  icon={<AssignmentTurnedInOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Manage SE"
                  to="/socialenterprise"
                  icon={<Diversity2OutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="LSEED Mentors"
                  to="/mentors"
                  icon={<SettingsAccessibilityOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Analytics Hub"
                  to="/analytics"
                  icon={<AnalyticsOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Show Reports"
                  to="/reports"
                  icon={<GradingOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Scheduling Matrix"
                  to="/scheduling"
                  icon={<CalendarMonthOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                {isLSEEDDirector && (
                  <Item
                    title="Manage Programs"
                    to="/programs"
                    icon={<FactCheckOutlinedIcon />}
                    selected={selected}
                    setSelected={setSelected}
                  />
                )}
                {isLSEEDDirector && (
                  <Item
                    title="Manage Users"
                    to="/admin"
                    icon={<AdminPanelSettingsOutlinedIcon />}
                    selected={selected}
                    setSelected={setSelected}
                  />
                )}
              </>
            ) : hasMentorRole ? (
              // 🧑‍🏫 Mentor View
              <>
                <Item
                  title="Dashboard"
                  to="/dashboard/mentor"
                  icon={<GridViewOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Evaluate"
                  to="/assess"
                  icon={<AssignmentTurnedInOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <Item
                  title="Manage Mentorships"
                  to="/mentorships"
                  icon={<SupervisorAccountOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
                <MenuItem
                  active={selected === "Scheduling Matrix"}
                  icon={<CalendarMonthOutlinedIcon />}
                  onClick={() => setOpenDialog(true)}
                  style={{
                    color: colors.grey[100],
                    fontWeight:
                      selected === "Scheduling Matrix" ? "bold" : "normal",
                  }}
                >
                  <Typography variant="body1">Scheduling Matrix</Typography>
                </MenuItem>
                <Item
                  title="Collaboration Portal"
                  to="/collaboration-dashboard"
                  icon={<HandshakeOutlinedIcon />}
                  selected={selected}
                  setSelected={setSelected}
                />
              </>
            ) : null}

            {/* Logout Button */}
            <Box mt="20px">
              <MenuItem
                onClick={logout}
                icon={<ExitToAppOutlinedIcon />}
                style={{ color: colors.grey[100], transition: "all 0.3s ease" }}
              >
                <Typography variant="body1">Logout</Typography>
              </MenuItem>
            </Box>
          </Box>
        </Menu>
      </ProSidebar>

      {/* MUI Dialog for Confirmation */}
      <Dialog open={openDialog} onClose={handleCloseDialog}>
        <DialogTitle>
          Logging in to google. This will sync mentorship schedules with your
          Google Calendar. Continue?
        </DialogTitle>
        <DialogActions>
          <Button
            onClick={handleCloseDialog}
            sx={{
              color: "white",
              backgroundColor: "red",
              "&:hover": { backgroundColor: "darkred" },
            }}
          >
            Cancel
          </Button>
          <Button
            onClick={confirmNavNoSync}
            sx={{
              color: "white",
              backgroundColor: "green",
              "&:hover": { backgroundColor: "darkgreen" },
            }}
          >
            Continue without Sync
          </Button>
          <Button
            onClick={handleConfirmNavigation}
            sx={{
              color: "white",
              backgroundColor: "green",
              "&:hover": { backgroundColor: "darkgreen" },
            }}
          >
            Continue
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default Sidebar;