import React from "react";
import { Box } from "@mui/material";
import { Outlet } from "react-router-dom";
import Sidebar from "../scenes/global/Sidebar";
import Topbar from "../scenes/global/Topbar";
import ScrollToTop from "../components/ScrollToTop";
import { useAuth } from "../context/authContext";
import { useNotifications } from "../hooks/useNotifications";

const AppLayout = () => {
  const { user, isMentorView } = useAuth();

  // centralize polling here so it doesn’t restart on view toggle
  const { notifications, setNotifications } = useNotifications(user?.id);

  return (
    <Box sx={{ display: "flex", width: "100%", minHeight: "100vh" }}>
      {/* IMPORTANT: do NOT key Sidebar/Topbar by isMentorView; let them persist */}
      <Sidebar isMentorView={isMentorView} />
      <Box
        sx={{
          flexGrow: 1,
          display: "flex",
          flexDirection: "column",
          height: "100vh",
          overflowY: "auto",
          padding: "20px",
        }}
      >
        {/* pass notifications down, Topbar becomes presentational */}
        <Topbar notifications={notifications} setNotifications={setNotifications} />
        <ScrollToTop />
        <Outlet />
      </Box>
    </Box>
  );
};

export default AppLayout;