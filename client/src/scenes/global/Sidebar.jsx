import { useState, useEffect } from "react";
import { ProSidebar, Menu, MenuItem } from "react-pro-sidebar";
import "react-pro-sidebar/dist/css/styles.css";
import {
  Box,
  IconButton,
  Typography,
  useTheme,
  Dialog,
  DialogTitle,
  DialogActions,
  Button,
} from "@mui/material";
import { Link, useNavigate, useLocation } from "react-router-dom";
import { tokens } from "../../theme";
import HowToRegOutlinedIcon from "@mui/icons-material/HowToRegOutlined";
import DescriptionOutlinedIcon from "@mui/icons-material/DescriptionOutlined";
import GridViewOutlinedIcon from "@mui/icons-material/GridViewOutlined";
import SupervisorAccountOutlinedIcon from "@mui/icons-material/SupervisorAccountOutlined";
import Diversity2OutlinedIcon from "@mui/icons-material/Diversity2Outlined";
import SettingsAccessibilityOutlinedIcon from "@mui/icons-material/SettingsAccessibilityOutlined";
import AnalyticsOutlinedIcon from "@mui/icons-material/AnalyticsOutlined";
import GradingOutlinedIcon from "@mui/icons-material/GradingOutlined";
import CalendarMonthOutlinedIcon from "@mui/icons-material/CalendarMonthOutlined";
import MenuOutlinedIcon from "@mui/icons-material/MenuOutlined";
import ExitToAppOutlinedIcon from "@mui/icons-material/ExitToAppOutlined";
import AssignmentTurnedInOutlinedIcon from "@mui/icons-material/AssignmentTurnedInOutlined";
import AdminPanelSettingsOutlinedIcon from "@mui/icons-material/AdminPanelSettingsOutlined";
import FactCheckOutlinedIcon from "@mui/icons-material/FactCheckOutlined";
import HandshakeOutlinedIcon from "@mui/icons-material/HandshakeOutlined";
import { useAuth } from "../../context/authContext";
import { createCalendarEvents } from "../../components/googleCalendar";
import AccountBalanceOutlinedIcon from "@mui/icons-material/AccountBalanceOutlined";
import { useGoogleLogin } from "@react-oauth/google";

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
const Sidebar = ({}) => {
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
            <Box textAlign="center" p="10px">
              <Box display="flex" justifyContent="center" alignItems="center">
                <img
                  alt="profile-user"
                  width="90px"
                  height="90px"
                  src="../../dist/assets/Picture.png"
                  style={{
                    borderRadius: "50%",
                    border: `2px solid ${colors.grey[100]}`,
                    boxShadow: "0px 4px 10px rgba(0,0,0,0.2)",
                  }}
                />
              </Box>
              <Typography
                variant="h5"
                color={colors.grey[100]}
                fontWeight="bold"
                mt={1}
              >
                {user.firstName || "User"} {user.lastName || "User"}
              </Typography>
              <Typography variant="body2" color={colors.greenAccent[500]}>
                {user.roles && user.roles.length > 0
                  ? user.roles.join(" / ")
                  : "No Role Assigned"}
              </Typography>
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