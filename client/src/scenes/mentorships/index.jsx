import React, { useState, useEffect } from "react";
import axios from "axios";
import {
  Box,
  Typography,
  useTheme,
  IconButton,
  FormControl,
  InputLabel,
  Select,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  MenuItem,
  Link,
} from "@mui/material";
import Switch from "@mui/material/Switch";
import { DataGrid, GridActionsCellItem } from "@mui/x-data-grid";
import { tokens } from "../../theme";
import Header from "../../components/Header";
import SEPerformanceTrendChart from "../../components/SEPerformanceTrendChart";
import DownloadOutlinedIcon from "@mui/icons-material/DownloadOutlined";
import { useNavigate } from "react-router-dom"; // For navigation
import { Snackbar, Alert } from "@mui/material";
import { JsonRequestError } from "@fullcalendar/core/index.js";
import axiosClient from "../../api/axiosClient";

const Mentorships = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const navigate = useNavigate(); // Initialize navigation

  const userSession = JSON.parse(localStorage.getItem("user"));
  const [copySuccessOpen, setCopySuccessOpen] = useState(false);
  const [copiedRegisterLinkSnackBar, setCopiedRegisterLinkSnackBar] = useState(false);
  const handleCopyOTP = () => {
    navigator.clipboard.writeText(generatedOTP)
      .then(() => {
        console.log('OTP copied to clipboard!');
        setOtpDialogOpen(false);
        setCopySuccessOpen(true);
      })
      .catch((err) => {
        console.error('Failed to copy OTP:', err);
      });
  };
    const handleCopyRegisterLink = () => {
    navigator.clipboard.writeText("https://t.me/LSEED_Bot")
      .then(() => {
        console.log('Register link copied to clipboard!');
        setOtpDialogOpen(false);
        setCopiedRegisterLinkSnackBar(true);
      })
      .catch((err) => {
        console.error('Failed to copy OTP:', err);
      });
  };
  // State for dialogs
  const [isAvailable, setIsAvailable] = useState(false);
  const [isLoadingToggle, setIsLoadingToggle] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  const [selectedRow, setSelectedRow] = useState(null);
  const [openEditDialog, setOpenEditDialog] = useState(false);
  const [otpDialogOpen, setOtpDialogOpen] = useState(false);
  const [generatedOTP, setGeneratedOTP] = useState("");

  // State for fetched data
  const [socialEnterprises, setSocialEnterprises] = useState([]);
  const [loading, setLoading] = useState(true); // Loading state for API call

  useEffect(() => {
    const fetchAvailability = async () => {
      try {
        const response = await axiosClient.get(`/api/get-mentor-availability`);
        setIsAvailable(response.data.isAvailable);
      } catch (err) {
        console.error("Failed to fetch availability", err);
      }
    };

    fetchAvailability();
  }, []);

  const handleToggle = async () => {
    try {
      const newValue = !isAvailable;
      setIsLoadingToggle(true);
      await axiosClient.post(
        `${process.env.REACT_APP_API_BASE_URL}/api/toggle-mentor-availability`,
        { isAvailable: newValue }
      );
      setIsAvailable(newValue);
    } catch (err) {
      console.error("Failed to update availability", err);
    } finally {
      setIsLoadingToggle(false);
    }
  };

  const handleGenerateOTP = async () => {
    try {
      const response = await fetch(`${process.env.REACT_APP_API_BASE_URL}/show-signup-password`, {
        method: "POST",
        credentials: "include",  // if you need session
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
        throw new Error("Failed to generate OTP");
      }

      const data = await response.json();
      setGeneratedOTP(data.otp);
      setOtpDialogOpen(true);
    } catch (error) {
      console.error("Error generating OTP:", error);
      alert("Failed to generate OTP. Please try again.");
    }
  };

  // Fetch social enterprises from the backend
  useEffect(() => {
    const fetchSocialEnterprise = async () => {
      try {
        const mentor_id = userSession.id; // Replace with actual mentor ID

        const response = await axios.get(
          `${process.env.REACT_APP_API_BASE_URL}/getAllSocialEnterpriseswithMentorID`,
          { params: { mentor_id } } // Pass mentor_id as a query parameter
        );

        const updatedSocialEnterprises = response.data.map((se) => ({
          id: se.se_id,
          name: se.team_name || "Unnamed SE",
          program: se.program_name || "No Program", // ✅ Include program name
          contact: se.contactnum || "No Contact",
          mentors:
            se.mentors.map((m) => m.mentor_name).join(", ") || "No mentor",
        }));

        setSocialEnterprises(updatedSocialEnterprises);
        setLoading(false);
      } catch (error) {
        console.error("Error fetching social enterprises:", error);
        setLoading(false);
      }
    };
    fetchSocialEnterprise();
  }, []);

  // Handle row click
  const handleRowClick = (params) => {
    if (isEditing) {
      setSelectedRow(params.row);
      setOpenEditDialog(true);
    }
  };

  const columns = [
    {
      field: "name",
      headerName: "Social Enterprise",
      flex: 1,
      editable: isEditing,
    },
    { field: "program", headerName: "Program", flex: 1, editable: isEditing },
    {
      field: "contact",
      headerName: "Contact Person",
      flex: 1,
      editable: isEditing,
    },
    { field: "mentors", headerName: "Mentors", flex: 1, editable: isEditing },
    {
      field: "actions",
      headerName: "Actions",
      width: 200,
      renderCell: (params) => {
        return (
          <Box
            display="flex"
            justifyContent="center"
            alignItems="center"
            height="100%"
            gap={1}
            width="100%" // Ensures full width for centering
          >
            <Button
              onClick={() => navigate(`/se-analytics/${params.row.id}`)}
              sx={{
                color: "#fff",
                backgroundColor: colors.greenAccent[500], // Custom color
                "&:hover": { backgroundColor: colors.greenAccent[700] },
              }}
            >
              View SE
            </Button>
          </Box>
        );
      },
    },
  ];

  return (
    <Box m="20px">
      <Header title="SOCIAL ENTERPRISE" subtitle="Manage Social Enterprises" />
      {/* SE Performance Trend*/}
      <Box
        gridColumn="span 12"
        gridRow="span 2"
        backgroundColor={colors.primary[400]}
        paddingTop="10px"
      >
        <SEPerformanceTrendChart />{" "}
      </Box>
      {/* Generate OTP Button */}
      <Box mt="20px">
        <Button
          variant="contained"
          sx={{
            backgroundColor: colors.greenAccent[500],
            color: "black",
            "&:hover": {
              backgroundColor: colors.greenAccent[600],
            },
          }}
          onClick={handleGenerateOTP}
        >
          Generate OTP
        </Button>
      </Box>
      <Box
        width="100%"
        backgroundColor={colors.primary[400]}
        padding="20px"
        display="flex"
        flexDirection="column"
      >
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography
            variant="h3"
            fontWeight="bold"
            color={colors.greenAccent[500]}
            marginBottom="15px"
          >
            My Mentorships
          </Typography>

          {/* Availability Toggle */}
          <Box display="flex" alignItems="center">
            <Typography color={colors.grey[100]} marginRight="8px">
              Available for Assignments
            </Typography>
            <Switch
              checked={isAvailable}
              onChange={handleToggle}
              color="success"
              disabled={isLoadingToggle}
            />
          </Box>
        </Box>

        {/* DataGrid container */}
        <Box
          height="400px"
          minHeight="400px"
          width="100%"
          sx={{
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
          {loading ? (
            <Typography>Loading...</Typography>
          ) : (
            <DataGrid
              rows={socialEnterprises}
              columns={columns}
              getRowId={(row) => row.id}
              onRowClick={handleRowClick}
              editMode="row"
            />
          )}
        </Box>
      </Box>

      {/* OTP Dialog */}
      <Dialog
        open={otpDialogOpen}
        onClose={() => setOtpDialogOpen(false)}
        maxWidth="xs"
        fullWidth
        PaperProps={{
          style: {
            backgroundColor: "#fff",
            color: "#000",
            border: "1px solid #000",
            borderRadius: "8px",
            boxShadow: "0 4px 12px rgba(0, 0, 0, 0.3)",
          },
        }}
      >
        <DialogTitle
          sx={{
            backgroundColor: "#1E4D2B",
            color: "#fff",
            textAlign: "center",
            fontWeight: "bold",
            borderTopLeftRadius: "8px",
            borderTopRightRadius: "8px",
          }}
        >
          Registration Details
        </DialogTitle>

        <DialogContent
          sx={{
            textAlign: "center",
            padding: "24px",
            backgroundColor: "#fff",
            color: "#000",
          }}
        >
          <Typography
            variant="body1"
            color="#000"
            gutterBottom
          >
            Share this OTP with the Social Enterprise to complete their registration.
          </Typography>

          <Typography
            variant="h6"
            fontWeight="bold"
            color="#000"
            gutterBottom
          >
            {generatedOTP}
          </Typography>

          <Typography
            variant="body2"
            color="#000"
            sx={{ marginTop: "16px" }}
          >
            They can register through our Telegram bot:
          </Typography>

          <Link
            href="https://t.me/LSEED_Bot"
            target="_blank"
            rel="noopener"
            underline="hover"
            sx={{
              color: "#1E4D2B",
              fontWeight: "bold",
              fontSize: "16px",
              display: "block",
              marginTop: "4px",
            }}
          >
            @LSEED_Bot
          </Link>
        </DialogContent>

        <DialogActions
          sx={{
            borderTop: "1px solid #000",
            backgroundColor: "#fff",
            padding: "16px",
          }}
        >
          <Button
            onClick={handleCopyRegisterLink}
            variant="outlined"
            sx={{
              borderColor: "#000",
              color: "#000",
              "&:hover": {
                backgroundColor: "#f0f0f0",
              },
            }}
          >
            Copy Register Link
          </Button>

          <Button
            variant="outlined"
            onClick={handleCopyOTP}
            sx={{
              borderColor: "#000",
              color: "#000",
              "&:hover": {
                backgroundColor: "#f0f0f0",
              },
            }}
          >
            Copy OTP
          </Button>
          <Button
            onClick={() => setOtpDialogOpen(false)}
            sx={{
              color: "#000",
              border: "1px solid #000",
              "&:hover": {
                backgroundColor: "#f0f0f0",
              },
            }}
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={copiedRegisterLinkSnackBar}
        autoHideDuration={3000}
        onClose={() => setCopiedRegisterLinkSnackBar(false)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        <Alert
          onClose={() => setCopiedRegisterLinkSnackBar(false)}
          severity="success"
          sx={{ width: "100%" }}
        >
          Register Link copied to clipboard!
        </Alert>
      </Snackbar>

      <Snackbar
        open={copySuccessOpen}
        autoHideDuration={3000}
        onClose={() => setCopySuccessOpen(false)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        <Alert
          onClose={() => setCopySuccessOpen(false)}
          severity="success"
          sx={{ width: "100%" }}
        >
          OTP copied to clipboard!
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default Mentorships;
