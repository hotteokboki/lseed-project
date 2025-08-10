import {
  Box,
  Button,
  IconButton,
  Menu,
  MenuItem,
  Typography,
  useTheme,
  Divider,
  FormControlLabel,
  Switch,
} from "@mui/material";
import Badge from "@mui/material/Badge";
import { useContext, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ColorModeContext, tokens } from "../../theme";
import InputBase from "@mui/material/InputBase";
import LightModeOutlinedIcon from "@mui/icons-material/LightModeOutlined";
import DarkModeOutlinedIcon from "@mui/icons-material/DarkModeOutlined";
import NotificationsOutlinedIcon from "@mui/icons-material/NotificationsOutlined";
import PersonOutlinedIcon from "@mui/icons-material/PersonOutlined";
import SearchIcon from "@mui/icons-material/Search";
import ExitToAppOutlinedIcon from "@mui/icons-material/ExitToAppOutlined";
import { useAuth } from "../../context/authContext";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import axiosClient from "../../api/axiosClient";

const Topbar = ({ notifications, setNotifications }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const colorMode = useContext(ColorModeContext);
  const [anchorEl, setAnchorEl] = useState(null);
  const [notifAnchorEl, setNotifAnchorEl] = useState(null);
  const navigate = useNavigate();
  const unreadCount = (notifications || []).filter((n) => !n.is_read).length;
  const [expandedNotificationId, setExpandedNotificationId] = useState(null);
  const { logout, user, isMentorView, toggleView } = useAuth();

  const handleMenuOpen = (event) => setAnchorEl(event.currentTarget);
  const handleMenuClose = () => setAnchorEl(null);
  const handleNotifOpen = (event) => setNotifAnchorEl(event.currentTarget);
  const handleNotifClose = () => setNotifAnchorEl(null);

  const handleToggleExpand = async (notifId) => {
    setExpandedNotificationId((prev) => (prev === notifId ? null : notifId));
    await markNotificationAsRead(notifId);
  };

  const handleNotificationClick = (notif) => {
    navigate(notif.target_route || "/");
  };

  const markNotificationAsRead = async (notificationId) => {
    try {
      await axiosClient.put(`/api/notifications/${notificationId}/read`);
      setNotifications((prev) =>
        prev.map((n) =>
          n.notification_id === notificationId ? { ...n, is_read: true } : n
        )
      );
    } catch (e) {
      console.error("Failed to mark notification as read:", e);
    }
  };

  const hasBothRoles =
    user?.roles?.includes("LSEED-Coordinator") &&
    user?.roles?.includes("Mentor");

  return (
    <Box display="flex" justifyContent="space-between" p={2}>
      {/* Search Bar */}
      <Box display="flex" backgroundColor={colors.primary[400]} borderRadius="3px">
        <InputBase sx={{ ml: 2, flex: 1 }} placeholder="Search" />
        <IconButton type="button" sx={{ p: 1 }}>
          <SearchIcon />
        </IconButton>
      </Box>

      {/* Icons */}
      <Box display="flex">
        {hasBothRoles && (
          <Box display="flex" alignItems="center" mr={2}>
            <FormControlLabel
              control={
                <Switch
                  checked={isMentorView}
                  onChange={toggleView}
                  color="secondary"
                />
              }
              label={
                <Typography variant="body1" color={colors.grey[100]}>
                  {isMentorView ? "Mentor View" : "Coordinator View"}
                </Typography>
              }
            />
          </Box>
        )}

        <IconButton onClick={colorMode.toggleColorMode}>
          {theme.palette.mode === "dark" ? <DarkModeOutlinedIcon /> : <LightModeOutlinedIcon />}
        </IconButton>

        {/* Notifications Button */}
        <IconButton onClick={handleNotifOpen}>
          <Badge badgeContent={unreadCount} color="error">
            <NotificationsOutlinedIcon />
          </Badge>
        </IconButton>

        {/* Notifications Menu */}
        <Menu
          anchorEl={notifAnchorEl}
          open={Boolean(notifAnchorEl)}
          onClose={handleNotifClose}
          sx={{
            "& .MuiPaper-root": {
              width: 400,
              backgroundColor: "#fff",
              color: "#000",
              border: "1px solid #000",
              boxShadow: "0px 4px 10px rgba(0,0,0,0.2)",
              maxHeight: 400,
              overflowY: "auto",
            },
          }}
        >
          <Typography
            sx={{
              backgroundColor: "#1E4D2B",
              color: "#fff",
              textAlign: "center",
              fontSize: "1.2rem",
              fontWeight: "bold",
              padding: "10px",
              position: "sticky",
              top: 0,
              zIndex: 1,
            }}
          >
            Notifications
          </Typography>

          <Box sx={{ maxHeight: 340, overflowY: "auto" }}>
            {(notifications || []).length > 0 ? (
              notifications.map((notif, index) => (
                <Box key={notif.notification_id}>
                  <MenuItem
                    onClick={() => handleToggleExpand(notif.notification_id)}
                    sx={{
                      display: "flex",
                      flexDirection: "column",
                      alignItems: "flex-start",
                      p: 1.5,
                      backgroundColor: notif.is_read ? "inherit" : colors.greenAccent[100],
                      "&:hover": { backgroundColor: "#f0f0f0" },
                    }}
                  >
                    <Box sx={{ display: "flex", justifyContent: "space-between", width: "100%", alignItems: "center" }}>
                      <Typography sx={{ fontWeight: "bold", flexGrow: 1 }}>
                        {notif.title}
                      </Typography>
                      <IconButton size="small" sx={{ color: "black" }}>
                        {expandedNotificationId === notif.notification_id ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                      </IconButton>
                    </Box>

                    {expandedNotificationId === notif.notification_id && (
                      <Box
                        sx={{
                          mt: 1,
                          width: "100%",
                          p: 1,
                          borderRadius: 1,
                          backgroundColor: "#f9f9f9",
                          border: "1px solid #ddd",
                          display: "flex",
                          flexDirection: "column",
                          gap: 1,
                          wordWrap: "break-word",
                          overflowWrap: "break-word",
                        }}
                      >
                        <Typography variant="body2" sx={{ whiteSpace: "pre-wrap" }}>
                          {notif.message}
                        </Typography>

                        <Typography variant="caption" color="gray">
                          {new Date(notif.created_at).toLocaleString()}
                        </Typography>

                        <Button
                          variant="text"
                          color="primary"
                          sx={{ fontSize: "0.85rem", alignSelf: "flex-start" }}
                          onClick={() => handleNotificationClick(notif)}
                        >
                          Go to page
                        </Button>
                      </Box>
                    )}
                  </MenuItem>

                  {index < notifications.length - 1 && <Divider />}
                </Box>
              ))
            ) : (
              <MenuItem sx={{ textAlign: "center", p: 2 }}>No new notifications</MenuItem>
            )}
          </Box>
        </Menu>

        <IconButton onClick={handleMenuOpen}>
          <PersonOutlinedIcon />
        </IconButton>

        <Menu anchorEl={anchorEl} open={Boolean(anchorEl)} onClose={handleMenuClose}>
          <MenuItem
            onClick={() => {
              handleMenuClose();
              navigate("/profile");
            }}
          >
            Profile
          </MenuItem>
          <MenuItem
            onClick={() => {
              logout();
              navigate("/");
            }}
            style={{ color: colors.redAccent[400], marginTop: 20 }}
          >
            <Typography>Logout</Typography>
          </MenuItem>
        </Menu>
      </Box>
    </Box>
  );
};

export default Topbar;