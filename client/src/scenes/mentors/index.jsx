import {
  Autocomplete,
  Box,
  Button,
  Dialog,
  DialogTitle,
  DialogActions,
  DialogContent,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  useTheme,
  Typography,
  Menu,
  Grid,
  OutlinedInput,
  Checkbox,
  ListItemText,
} from "@mui/material";
import { tokens } from "../../theme";
import Chip from '@mui/material/Chip';
import React from "react";
import axios from "axios";
import PersonRemoveIcon from "@mui/icons-material/PersonRemove";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import PersonIcon from "@mui/icons-material/Person";
import { DataGrid, GridToolbar } from "@mui/x-data-grid";
import Header from "../../components/Header";
import StatBox from "../../components/StatBox";
import PersonAddIcon from "@mui/icons-material/PersonAdd";
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Snackbar, Alert } from "@mui/material";
import Tooltip from "@mui/material/Tooltip";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { useAuth } from "../../context/authContext";
import axiosClient from "../../api/axiosClient";

const Mentors = ({ }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const { user } = useAuth()
  const [rows, setRows] = useState();
  const navigate = useNavigate();
  const [mentorshipData, setMentorshipData] = useState({
    selectedMentor: "",
    selectedSocialEnterprise: "",
  });
  const [menuOpenBusiness, setMenuOpenBusiness] = useState(false);
  const [menuOpenPreferredTime, setMenuOpenPreferredTime] = useState(false);
  const [menuOpenCommunicationModes, setMenuOpenCommunicationModes] = useState(false);
  const [openApplyDialog, setOpenApplyDialog] = useState(false);
  const [formData, setFormData] = useState({
    affiliation: "",
    motivation: "",
    // expertise: "",
    businessAreas: [],
    preferredTime: [],
    specificTime: "",
    communicationMode: [],
    // ...you can prefill these with user info from the backend if you fetch it
  });
  const [mentorships, setMentorships] = useState([]);
  const [anchorEl, setAnchorEl] = useState(null);
  const [menuRowId, setMenuRowId] = useState(null);
  const [mentorApplications, setMentorApplications] = useState([]);
  const [suggestedMentors, setSuggestedMentors] = useState([]);
  const [otherMentors, setOtherMentors] = useState([]);
  const [selectedApplication, setSelectedApplication] = useState(null);
  const [openApplicationDialog, setOpenApplicationDialog] = useState(false);
  const [loading, setLoading] = useState(true); // Loading state for API call
  const [mentors, setMentors] = useState([]);
  const [socialEnterprises, setSocialEnterprises] = useState([]);
  const [stats, setStats] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isAllowedtoApply, setIsAllowedtoApply] = useState(null);
  const [selectedMentor, setSelectedMentor] = useState(null);
  const [mentorSearch, setMentorSearch] = useState(""); // For autocomplete input
  const [selectedSE, setSelectedSE] = useState("");
  const [snackbar, setSnackbar] = useState({
    open: false,
    message: "",
    severity: "info",
  });
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState("");
  const [snackbarSeverity, setSnackbarSeverity] = useState("success");

  const businessAreasList = [
    "Application Development",
    "Business Registration Process",
    "Community Development",
    "Expansion/Acceleration",
    "Finance",
    "Human Resource",
    "Intellectual Property",
    "Legal Aspects and Compliance",
    "Management",
    "Marketing",
    "Online engagement",
    "Operations",
    "Product Development",
    "Sales",
    "Supply Chain and Logistics",
    "Technology Development",
    "Social Impact",
  ];

  const preferredTimeList = [
    "Weekday (Morning) 8AM - 12NN",
    "Weekday (Afternoon) 1PM - 5PM",
    "Other",
  ];

  const communicationModes = [
    "Face to Face",
    "Facebook Messenger",
    "Google Meet",
    "Zoom",
    "Other",
  ];


  // Fetch mentors from the database
  const fetchMentors = async () => {
    try {
      const response = await axiosClient.get(`/api/mentors`); // ✅ Fixed URL
      console.log("📥 MENTORS Response:", response.data); // ✅ Debugging Log

      const formattedData = response.data.map((mentor) => ({
        id: mentor.mentor_id,
        mentor_firstName: mentor.mentor_firstName,
        mentor_lastName: mentor.mentor_lastName,
        mentorName: `${mentor.mentor_firstName} ${mentor.mentor_lastName}`,
        email: mentor.email,
        contactnum: mentor.contactNum || "N/A",
        numberOfSEsAssigned: mentor.number_SE_assigned || 0,
        assigned_se_names: mentor.assigned_se_names || "", // ✅ Include this line
        status: "Active",
      }));

      setRows(formattedData); // ✅ Correctly setting state
    } catch (error) {
      console.error("❌ Error fetching mentors:", error);
    }
  };

  useEffect(() => {
    fetchMentors();
  }, []);

  // Application View Open
  const handleOpenMenu = (event, rowId) => {
    setAnchorEl(event.currentTarget);
    setMenuRowId(rowId);
  };
  // Application View Close
  const handleCloseMenu = () => {
    setAnchorEl(null);
    setMenuRowId(null);
  };

  const businessAreasSelect = () => (
    <FormControl fullWidth sx={{ mt: 2 }}>
      <InputLabel>Business Areas</InputLabel>
      <Select
        multiple
        value={formData.businessAreas}
        onChange={(e) =>
          setFormData((prev) => ({
            ...prev,
            businessAreas: e.target.value.filter(Boolean),
          }))
        }
        input={<OutlinedInput label="Business Areas" />}
        renderValue={(selected) => (
          <Box
            sx={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 0.5,
              maxHeight: 200,
              overflowY: 'auto',
            }}
          >
            {selected.map((value) => (
              <Chip
                key={value}
                label={value}
                color="default"
                variant="outlined"
              />
            ))}
          </Box>
        )}
        open={menuOpenBusiness}
        onOpen={() => setMenuOpenBusiness(true)}
        onClose={() => setMenuOpenBusiness(false)}
      >
        {businessAreasList.map((area) => (
          <MenuItem key={area} value={area}>
            <Checkbox checked={formData.businessAreas.includes(area)} />
            <ListItemText primary={area} />
          </MenuItem>
        ))}
        <MenuItem disableRipple divider>
          <Button
            fullWidth
            variant="contained"
            color="primary"
            onClick={() => setMenuOpenBusiness(false)}
          >
            Done
          </Button>
        </MenuItem>
      </Select>
    </FormControl>
  );

  const preferredTimeSelect = () => (
    <FormControl fullWidth sx={{ mt: 2 }}>
      <InputLabel>Preferred Time</InputLabel>
      <Select
        multiple
        value={formData.preferredTime}
        onChange={(e) =>
          setFormData((prev) => ({
            ...prev,
            preferredTime: e.target.value.filter(Boolean),
          }))
        }
        input={<OutlinedInput label="Preferred Time" />}
        renderValue={(selected) => (
          <Box
            sx={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 0.5,
              maxHeight: 200,
              overflowY: 'auto',
            }}
          >
            {selected.map((value) => (
              <Chip
                key={value}
                label={value}
                color="default"
                variant="outlined"
              />
            ))}
          </Box>
        )}
        open={menuOpenPreferredTime}
        onOpen={() => setMenuOpenPreferredTime(true)}
        onClose={() => setMenuOpenPreferredTime(false)}
      >
        {preferredTimeList.map((time) => (
          <MenuItem key={time} value={time}>
            <Checkbox checked={formData.preferredTime.includes(time)} />
            <ListItemText primary={time} />
          </MenuItem>
        ))}
        <MenuItem disableRipple divider>
          <Button
            fullWidth
            variant="contained"
            color="primary"
            onClick={() => setMenuOpenPreferredTime(false)}
          >
            Done
          </Button>
        </MenuItem>
      </Select>
    </FormControl>
  );

  const communicationModeSelect = () => (
    <FormControl fullWidth sx={{ mt: 2 }}>
      <InputLabel>Communication Modes</InputLabel>
      <Select
        multiple
        value={formData.communicationMode}
        onChange={(e) =>
          setFormData((prev) => ({
            ...prev,
            communicationMode: e.target.value.filter(Boolean),
          }))
        }
        input={<OutlinedInput label="Communication Modes" />}
        renderValue={(selected) => (
          <Box
            sx={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 0.5,
              maxHeight: 200,
              overflowY: 'auto',
            }}
          >
            {selected.map((value) => (
              <Chip
                key={value}
                label={value}
                color="default"
                variant="outlined"
              />
            ))}
          </Box>
        )}
        open={menuOpenCommunicationModes}
        onOpen={() => setMenuOpenCommunicationModes(true)}
        onClose={() => setMenuOpenCommunicationModes(false)}
      >
        {communicationModes.map((mode) => (
          <MenuItem key={mode} value={mode}>
            <Checkbox checked={formData.communicationMode.includes(mode)} />
            <ListItemText primary={mode} />
          </MenuItem>
        ))}
        <MenuItem disableRipple divider>
          <Button
            fullWidth
            variant="contained"
            color="primary"
            onClick={() => setMenuOpenCommunicationModes(false)}
          >
            Done
          </Button>
        </MenuItem>
      </Select>
    </FormControl>
  );

  const handleApplySubmit = async (e) => {
    e.preventDefault();

    console.log("DATA: ", formData)

    try {
      // You can POST this to your API
      await fetch(`${process.env.REACT_APP_API_BASE_URL}/apply-as-mentor`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...formData, userId: user.id, email: user.email }),
        credentials: "include",  // ✅ this line ensures cookies/session are sent
      });

      setSnackbar({
        open: true,
        message: "Your mentor application was submitted!",
        severity: "success",
      });

      setOpenApplyDialog(false);
      setFormData({
        affiliation: "",
        motivation: "",
        // expertise: "",
        businessAreas: [],
        preferredTime: [],
        specificTime: "",
        communicationMode: [],
      });

      await new Promise((r) => setTimeout(r, 2000));
      window.location.reload();
    } catch (error) {
      console.error("Error submitting application:", error);
      setSnackbar({
        open: true,
        message: "Error submitting application.",
        severity: "error",
      });
    }
  };

  const handleMentorApplicationInputChange = (e, key) => {
    setFormData((prev) => ({
      ...prev,
      [key]: e.target.value,
    }));
  };

  const handleAcceptMentor = async (row) => {
    const res = await axiosClient.post(`/api/accept-mentor-application`, {
      applicationId: row.id
    });

    const data = res.data;
    console.log("✅ Mentor accepted:", data);
  };

  const handleMenuAction = async (action, row) => {
    console.log(`Action: ${action}`, row);

    if (action === "Accept") {
      try {
        await handleAcceptMentor(row);

        setSnackbarMessage("Accepted Application! Mentor Added Successfully");
        setSnackbarSeverity("success");
        setSnackbarOpen(true);

        await new Promise((r) => setTimeout(r, 3500));
        window.location.reload();
      } catch (error) {
        console.error("❌ Error accepting mentor:", error);
        setSnackbarMessage("Error processing acceptance. Please try again.");
        setSnackbarSeverity("error");
        setSnackbarOpen(true);
      }
    }

    if (action === "Decline") {
      const applicationId = row.id;

      try {
        await axiosClient.post("/api/decline-mentor-application", { applicationId });

        setSnackbarMessage("Declined Application!");
        setSnackbarSeverity("success");
        setSnackbarOpen(true);

        // Ideally just refetch the table instead of full reload:
        // await refetchApplications?.();
        await new Promise((r) => setTimeout(r, 800));
        window.location.reload();
      } catch (error) {
        console.error("❌ Error declining mentor:", error);

        if (error?.response?.status === 409) {
          setSnackbarMessage("Already processed by someone else.");
        } else if (error?.response?.status === 404) {
          setSnackbarMessage("Application not found.");
        } else {
          setSnackbarMessage("Error processing decline. Please try again.");
        }

        setSnackbarSeverity("error");
        setSnackbarOpen(true);
      } finally {
        handleCloseMenu();
      }
    }

    if (action === "View") {
      // Open the application view dialog
      setSelectedApplication(row); // set the clicked application details
      setOpenApplicationDialog(true); // show the dialog
    }

    handleCloseMenu();
  };

  useEffect(() => {
    let ignore = false;

    const fetchMentorApplicationStatus = async () => {
      try {
        const res = await axiosClient.get("/api/check-mentor-application-status");
        if (!ignore) setIsAllowedtoApply(Boolean(res.data?.allowed));
      } catch (err) {
        console.error("Failed to check application status:", err);
        if (!ignore) setIsAllowedtoApply(false); // safe default
      }
    };

    fetchMentorApplicationStatus();
    return () => { ignore = true; };
  }, []);

  useEffect(() => {
    const fetchMentorApplications = async () => {
      try {
        const response = await axiosClient.get(`/api/list-mentor-applications`); // adjust endpoint as needed
        const data = response.data;

        console.log("Raw date_applied:", data[0]?.date_applied);

        // Format date_applied in all items
        const formatted = data.map((item) => ({
          ...item,
          date_applied: new Date(item.date_applied).toLocaleDateString("en-US", {
            year: "numeric",
            month: "long",
            day: "numeric",
          }),
        }));
        setMentorApplications(formatted);
      } catch (error) {
        console.error("Error fetching SE applications:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchMentorApplications();
  }, []);

  useEffect(() => {
    if (selectedMentor) {
      fetchSocialEnterprises(selectedMentor.mentor_id);
    }
  }, [selectedMentor]);

  // Function to fetch assigned social enterprises
  const fetchSocialEnterprises = async (mentorId) => {
    try {
      const response = await axiosClient.get(
        `/api/mentors/${mentorId}/social-enterprises`
      );

      const data = response.data; // ✅ Ensure response is parsed as JSON
      setSocialEnterprises(data);
      console.log("📥 Social Enterprises Data:", data);
      return data;
    } catch (error) {
      console.error("Error fetching social enterprises:", error);
    }
  };

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await axiosClient.get(`/api/mentor-stats`);

        const data = response.data;
        setStats(data);
      } catch (error) {
        console.error("Error fetching analytics stats:", error);
      }
    };
    fetchStats();
  }, []);

  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch active mentors
        const mentorsResponse = await axiosClient.get(
          `/api/mentors-with-mentorships`
        );
        const mentorsData = mentorsResponse.data;
        setMentors(mentorsData);

        // Fetch social enterprises without mentors
        const seResponse = await axiosClient.get(
          `/api/social-enterprises-without-mentor`
        );
        const seData = seResponse.data;
        setSocialEnterprises(
          seData.map((se) => ({
            id: se.se_id,
            name: se.team_name,
          }))
        );
      } catch (error) {
        console.error("Error fetching data:", error);
      }
    };
    fetchData();
  }, []);
  // State for dialogs and data
  const [openDialog, setOpenDialog] = useState(false);
  const [openRelatedSEs, setOpenRelatedSEs] = useState(false);
  const [selectedSEs, setSelectedSEs] = useState([]);

  const SEHoverCell = ({ number, seNames, onClick }) => {
    const [hover, setHover] = React.useState(false);

    return (
      <Box
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
        onClick={onClick}
        sx={{
          cursor: "pointer",
          textDecoration: "underline",
        }}
      >
        {hover ? "View" : number}
      </Box>
    );
  };

  const [mentorData, setMentorData] = useState({
    firstName: "",
    lastName: "",
    email: "",
    contactNumber: "",
  });

  // Handle dialog open/close
  const handleDialogOpen = () => {
    setOpenDialog(true);
  };

  const handleDialogClose = () => {
    setOpenDialog(false);
  };

  // Handle input changes for dialog form
  const handleInputChange = (e) => {
    setMentorData({ ...mentorData, [e.target.name]: e.target.value });
  };

  const matchMentors = async (selectedSeId) => {
    try {
      const response = await axiosClient.post(`/api/suggested-mentors`, {
        se_id: selectedSeId
      });

      const data = response.data;

      console.log("✅ Raw mentor match response:", data);
      console.log("👉 Suggested mentors:", data.suggested);
      console.log("👉 Other mentors:", data.others);

      setSuggestedMentors(data.suggested || []);
      setOtherMentors(data.others || []);
    } catch (error) {
      console.error("❌ Error fetching mentor matches:", error);
      setSuggestedMentors([]);
      setOtherMentors([]);
    }
  };

  const handleRemoveMentorship = async () => {
    if (!selectedMentor || !selectedSE) {
      alert("Please select a mentor and a social enterprise.");
      return;
    }
    console.log("mentorId: ", selectedMentor.mentor_id, " seId: ", selectedSE);
    try {
      const response = await axiosClient.post(
        `/api/remove-mentorship`,
        {
          mentorId: selectedMentor.mentor_id,
          seId: selectedSE,
        }
      );

      // Show success message
      setSnackbarMessage("Successfully Removed!");
      setSnackbarSeverity("success");
      setSnackbarOpen(true); // Ensure setSnackbarOpen is defined

      await new Promise((r) => setTimeout(r, 1500));
      window.location.reload();

      setIsModalOpen(false);
      setSelectedMentor(null);
      setSelectedSE("");
      fetchMentors(); // Refresh mentor list
    } catch (error) {
      console.error("Error removing mentorship:", error);
    }
  };

  // Submit new mentor data
  const handleSubmit = async () => {
    const { selectedMentor, selectedSocialEnterprise } = mentorshipData;

    if (!selectedMentor || !selectedSocialEnterprise) {
      alert("Please select both a mentor and a social enterprise.");
      return;
    }

    try {
      const response = await axiosClient.post(`/api/mentorships`, {
        mentor_id: selectedMentor,
        se_id: selectedSocialEnterprise,
      });

      console.log("Mentorship added successfully");

      // Close the dialog box
      setOpenDialog(false);

      // Reset the form data
      setMentorshipData({ selectedMentor: "", selectedSocialEnterprise: "" });

      // Show success popup
      setSnackbarMessage("Mentorship added successfully!");
      setSnackbarSeverity("success");
      setSnackbarOpen(true); // Ensure setSnackbarOpen is defined

      // Fetch latest data
      await fetchLatestMentorships();

      // Refresh the page after a short delay to ensure updates are reflected
      await new Promise((r) => setTimeout(r, 1000));
      window.location.reload();
    } catch (error) {
      console.error("Failed to add mentorship:", error);
    }
  };

  const fetchLatestMentorships = async () => {
    try {
      const response = await fetch(`${process.env.REACT_APP_API_BASE_URL}/api/mentorships`); // Adjust the endpoint as needed
      if (response.ok) {
        const updatedMentorships = await response.json();
        // Update the state with the latest mentorship data
        setMentorships(updatedMentorships); // Assuming you have a state variable `mentorships`
      } else {
        console.error("Failed to fetch latest mentorships");
      }
    } catch (error) {
      console.error("Error fetching mentorships:", error);
    }
  };

  const columns = [
    {
      field: "mentor_fullName",
      headerName: "Mentor Name",
      flex: 1,
      minWidth: 100,
      cellClassName: "name-column--cell",
      renderCell: (params) =>
        `${params.row.mentor_firstName} ${params.row.mentor_lastName}`,
    },
    {
      field: "email",
      headerName: "Email",
      minWidth: 200,
      flex: 1,
      renderCell: (params) => `${params.row.email}`,
    },
    {
      field: "contactnum",
      minWidth: 100,
      headerName: "Contact Number",
      flex: 1,
      renderCell: (params) => `${params.row.contactnum}`,
    },
    {
      field: "numberOfSEsAssigned",
      minWidth: 50,
      headerName: "SEs Assigned",
      headerAlign: "left",
      align: "left",
      flex: 1,
      renderCell: (params) => {
        const seList = params.row.assigned_se_names
          ? params.row.assigned_se_names.split("||").map((name) => name.trim())
          : [];

        return (
          <SEHoverCell
            number={params.row.numberOfSEsAssigned}
            seNames={seList}
            onClick={() => {
              setSelectedSEs(seList);
              setOpenRelatedSEs(true);
            }}
          />
        );
      },
    },
    {
      field: "status",
      headerName: "Status",
      flex: 1,
      minWidth: 100,
      renderCell: (params) => <Box>{params.value}</Box>,
      renderEditCell: (params) => (
        <TextField
          select
          value={params.value}
          fullWidth
        >
          <MenuItem value="Active">Active</MenuItem>
          <MenuItem value="Inactive">Inactive</MenuItem>
        </TextField>
      ),
    },
    {
      field: "actions",
      headerName: "Actions",
      minWidth: 100,
      renderCell: (params) => (
        <Button
          variant="contained"
          size="small"
          onClick={() => navigate(`/mentor-analytics/${params.row.id}`)}
        >
          View Mentor
        </Button>
      ),
    },
  ];

  return (
    <Box m="20px">
      {/* HEADER */}
      <Header title="MENTORS" subtitle="Manage Mentors" />
      {/* ROW 1: STAT BOXES */}
      <Box
        display="grid"
        gridTemplateColumns="repeat(12, 1fr)"
        gridAutoRows="140px"
        gap="20px"
      >
        <Box
          gridColumn="span 3"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            title={stats?.mentorWithoutMentorshipCount[0]?.count} // Render dynamic value
            subtitle="Unassigned Mentors"
            progress={
              parseInt(stats?.mentorWithoutMentorshipCount[0]?.count) /
              parseInt(stats?.mentorCountTotal[0]?.count)
            } // Calculate percentage of unassigned mentors
            increase={
              isNaN(parseInt(stats?.mentorWithoutMentorshipCount[0]?.count) /
                parseInt(stats?.mentorCountTotal[0]?.count))
                ? "0%" :
                `${(
                  (parseInt(stats?.mentorWithoutMentorshipCount[0]?.count) /
                    parseInt(stats?.mentorCountTotal[0]?.count)) *
                  100
                ).toFixed(2)}%`} // Calculate percentage of mentors with mentorship
            icon={
              <PersonIcon
                sx={{ color: colors.greenAccent[600], fontSize: "26px" }}
              />
            }
          />
        </Box>

        <Box
          gridColumn="span 3"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            title={stats?.mentorWithMentorshipCount[0]?.count}
            subtitle="Assigned Mentors"
            progress={
              parseInt(stats?.mentorWithMentorshipCount[0]?.count) /
              parseInt(stats?.mentorCountTotal[0]?.count)
            } // Calculate percentage filled
            increase={
              isNaN(parseInt(stats?.mentorWithMentorshipCount[0]?.count) /
                parseInt(stats?.mentorCountTotal[0]?.count))
                ? "0%" :
                `${(
                  (parseInt(stats?.mentorWithMentorshipCount[0]?.count) /
                    parseInt(stats?.mentorCountTotal[0]?.count)) *
                  100
                ).toFixed(2)}%`} // Calculate percentage of mentors with mentorship
            icon={
              <PersonIcon
                sx={{ fontSize: "26px", color: colors.blueAccent[500] }}
              />
            }
          />
        </Box>
        <Box
          gridColumn="span 3"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            title={
              stats?.mostAssignedMentor?.length
                ? `${stats.mostAssignedMentor[0].mentor_firstname ?? ''} ${stats.mostAssignedMentor[0].mentor_lastname ?? ''}`.trim()
                : "No Available Data"
            }
            subtitle="Most Assigned"
            progress={(
              stats?.mostAssignedMentor[0]?.num_assigned_se /
              stats?.totalSECount[0]?.count
            ).toFixed(2)} // Calculate progress (assigned SE count / total SE count)
            increase={
              isNaN(stats?.mostAssignedMentor[0]?.num_assigned_se /
                stats?.totalSECount[0]?.count)
                ? "0%" :
                `${(
                  (stats?.mostAssignedMentor[0]?.num_assigned_se /
                    stats?.totalSECount[0]?.count -
                    0) *
                  100
                ).toFixed(2)}%`} // Adjust to calculate increase
            icon={
              <PersonAddIcon
                sx={{ color: colors.greenAccent[600], fontSize: "26px" }}
              />
            }
          />
        </Box>
        <Box
          gridColumn="span 3"
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            title={
              stats?.mostAssignedMentor?.length
                ? `${stats?.leastAssignedMentor[0]?.mentor_firstname} ${stats?.leastAssignedMentor[0]?.mentor_lastname}`
                : "No Available Data"
            }
            subtitle="Least Assigned"
            progress={(
              stats?.leastAssignedMentor[0]?.num_assigned_se /
              stats?.totalSECount[0]?.count
            ).toFixed(2)} // Calculate progress (assigned SE count / total SE count)
            increase={
              isNaN(stats?.leastAssignedMentor[0]?.num_assigned_se /
                stats?.totalSECount[0]?.count)
                ? "0%" :
                `${(
                  (stats?.leastAssignedMentor[0]?.num_assigned_se /
                    stats?.totalSECount[0]?.count -
                    0) *
                  100
                ).toFixed(2)}%`} // Adjust to calculate increase
            icon={
              <PersonRemoveIcon
                sx={{ color: colors.greenAccent[600], fontSize: "26px" }}
              />
            }
          />
        </Box>
      </Box>
      {/* ADD MENTOR BUTTON AND EDIT TOGGLE */}
      <Box display="flex" gap="10px" mt="20px">
        <Box display="flex" alignItems="center" gap={2}>
          <Button
            variant="contained"
            sx={{
              backgroundColor: colors.greenAccent[500],
              color: "black",
              "&:hover": {
                backgroundColor: colors.greenAccent[600],
              },
            }}
            onClick={handleDialogOpen}
          >
            Add Mentorship
          </Button>

          {/* Add Mentorship Dialog */}
          <Dialog
            open={openDialog}
            onClose={handleDialogClose}
            maxWidth="md"
            fullWidth
            PaperProps={{
              style: {
                backgroundColor: "#fff", // White background
                color: "#000", // Black text
                border: "1px solid #000", // Black border for contrast
              },
            }}
          >
            {/* Dialog Title */}
            <DialogTitle
              sx={{
                backgroundColor: "#1E4D2B", // DLSU Green header
                color: "#fff", // White text
                textAlign: "center",
                fontSize: "1.5rem",
                fontWeight: "bold",
              }}
            >
              Add New Mentorship
            </DialogTitle>

            {/* Dialog Content */}
            <DialogContent
              sx={{
                padding: "24px",
                maxHeight: "70vh", // Ensure it doesn't overflow the screen
                overflowY: "auto", // Enable scrolling if content is too long
              }}
            >
              {/* Place Dropdowns Side by Side */}
              <Box
                display="flex"
                gap={2} // Add spacing between dropdowns
                alignItems="center" // Align dropdowns vertically
                mb={2} // Add margin at the bottom
                marginTop="20px"
              >
                <Typography
                  variant="subtitle1"
                  sx={{
                    fontWeight: "bold",
                    marginBottom: "8px", // Space between label and dropdown
                  }}
                >
                  Mentor
                </Typography>
                {/* Mentor Dropdown */}
                <FormControl fullWidth margin="normal">
                  <InputLabel
                    id="mentor-label"
                    sx={{
                      backgroundColor: "#fff",
                      padding: "0 4px",
                      "&.Mui-focused": {
                        backgroundColor: "#fff",
                      },
                    }}
                  >
                    Select Mentor
                  </InputLabel>
                  <Select
                    labelId="mentor-label"
                    name="selectedMentor"
                    value={mentorshipData.selectedMentor || ""}
                    onChange={(e) =>
                      setMentorshipData({
                        ...mentorshipData,
                        selectedMentor: e.target.value,
                      })
                    }
                    label="Select Mentor"
                    sx={{
                      "& .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000",
                      },
                      "&:hover .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000",
                      },
                      "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000",
                      },
                      "& .MuiSelect-select": {
                        color: "#000",
                      },
                    }}
                  >
                    {/* Section Header for Suggested */}
                    <Box
                      px={2}
                      py={1}
                      display="flex"
                      alignItems="center"
                      gap={1}
                      sx={{ pointerEvents: "none" }}
                    >
                      <Typography variant="subtitle2" fontWeight="bold">
                        Recommended Mentors (Top & Good Match)
                      </Typography>
                      <Tooltip title="These mentors match most or all areas with the SE’s critical areas.">
                        <InfoOutlinedIcon fontSize="small" sx={{ pointerEvents: "auto" }} />
                      </Tooltip>
                    </Box>

                    {suggestedMentors.length > 0 ? (
                      suggestedMentors.map((mentor) => (
                        <MenuItem
                          key={mentor.mentor_id}
                          value={mentor.mentor_id}
                          disabled={!mentor.is_available_for_assignment}
                          sx={{
                            opacity: mentor.is_available_for_assignment ? 1 : 0.5,
                          }}
                        >
                          <Box display="flex" flexDirection="column" alignItems="flex-start">
                            <Box display="flex" alignItems="center" gap={1}>
                              <Typography>
                                {mentor.mentor_firstname} {mentor.mentor_lastname}
                              </Typography>
                              <Chip
                                label="Recommended"
                                size="small"
                                color="success"
                              />
                              <Chip
                                label={mentor.is_available_for_assignment ? "Available" : "Unavailable"}
                                size="small"
                                color={mentor.is_available_for_assignment ? "success" : "default"}
                              />
                            </Box>
                            <Typography variant="body2" color="text.secondary">
                              Matched on {mentor.match_count} area
                              {mentor.match_count !== 1 ? "s" : ""}:{" "}
                              {mentor.matched_areas?.join(", ") || "N/A"}
                            </Typography>
                            {!mentor.is_available_for_assignment && (
                              <Typography variant="caption" color="error">
                                Not currently accepting new assignments
                              </Typography>
                            )}
                          </Box>
                        </MenuItem>
                      ))
                    ) : (
                      <MenuItem disabled>
                        <Typography variant="body2" color="text.secondary">
                          No highly matching mentors found.
                        </Typography>
                      </MenuItem>
                    )}

                    {/* Section Header for Others */}
                    <Box
                      px={2}
                      py={1}
                      display="flex"
                      alignItems="center"
                      gap={1}
                      sx={{ pointerEvents: "none" }}
                    >
                      <Typography variant="subtitle2" fontWeight="bold">
                        Other Available Mentors (Low Match)
                      </Typography>
                      <Tooltip title="These mentors matched few or none of the SE’s critical areas.">
                        <InfoOutlinedIcon fontSize="small" sx={{ pointerEvents: "auto" }} />
                      </Tooltip>
                    </Box>

                    {otherMentors.map((mentor) => (
                      <MenuItem
                        key={mentor.mentor_id}
                        value={mentor.mentor_id}
                        disabled={!mentor.is_available_for_assignment}
                        sx={{
                          opacity: mentor.is_available_for_assignment ? 1 : 0.5,
                        }}
                      >
                        <Box display="flex" flexDirection="column" alignItems="flex-start">
                          <Box display="flex" alignItems="center" gap={1}>
                            <Typography>
                              {mentor.mentor_firstname} {mentor.mentor_lastname}
                            </Typography>
                            <Chip
                              label={mentor.is_available_for_assignment ? "Available" : "Unavailable"}
                              size="small"
                              color={mentor.is_available_for_assignment ? "success" : "default"}
                            />
                          </Box>
                          <Typography variant="body2" color="text.secondary">
                            Matched on {mentor.match_count} area
                            {mentor.match_count !== 1 ? "s" : ""}:{" "}
                            {mentor.matched_areas?.join(", ") || "N/A"}
                          </Typography>
                          {!mentor.is_available_for_assignment && (
                            <Typography variant="caption" color="error">
                              Not currently accepting new assignments
                            </Typography>
                          )}
                        </Box>
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <Typography
                  variant="subtitle1"
                  sx={{
                    fontWeight: "bold",
                    marginBottom: "8px", // Space between label and dropdown
                  }}
                >
                  Social Enterprise
                </Typography>
                {/* Social Enterprise Dropdown */}
                <FormControl fullWidth margin="normal">
                  <InputLabel
                    id="mentor-label"
                    sx={{
                      backgroundColor: "#fff", // Prevent overlap with the border
                      padding: "0 4px", // Add padding for readability
                      "&.Mui-focused": {
                        backgroundColor: "#fff", // Ensure the background remains white when focused
                      },
                    }}
                  >
                    Select Social Enterprise
                  </InputLabel>
                  <Select
                    labelId="se-label"
                    name="selectedSocialEnterprise"
                    value={mentorshipData.selectedSocialEnterprise}
                    onChange={(e) => {
                      const selectedSeId = e.target.value;
                      console.log("✅ Selected SE ID:", selectedSeId);
                      setMentorshipData((prev) => ({
                        ...prev,
                        selectedSocialEnterprise: selectedSeId,
                      }));
                      matchMentors(selectedSeId); // 🔥
                    }}
                    label="Select Social Enterprise"
                    displayEmpty
                    fullWidth
                    margin="dense"
                    sx={{
                      "& .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000",
                      },
                      "&:hover .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000",
                      },
                      "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000",
                      },
                      "& .MuiSelect-select": {
                        color: "#000",
                      },
                    }}
                  >
                    <MenuItem disabled value="">
                      <em>Select Social Enterprise</em>
                    </MenuItem>
                    {socialEnterprises.map((se) => (
                      <MenuItem key={se.id} value={se.id}>
                        {se.name}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Box>
            </DialogContent>

            {/* Dialog Actions */}
            <DialogActions
              sx={{
                padding: "16px",
                borderTop: "1px solid #000", // Separator line
              }}
            >
              <Button
                onClick={() => {
                  handleDialogClose(); // Close the dialog
                  setTimeout(() => {
                    window.location.reload();
                  }, 500); // Adjust delay if needed
                }}
                sx={{
                  color: "#000",
                  border: "1px solid #000",
                  "&:hover": {
                    backgroundColor: "#f0f0f0", // Hover effect
                  },
                }}
              >
                Cancel
              </Button>

              <Button
                onClick={handleSubmit} // Calls the updated handleSubmit function
                variant="contained"
                disabled={
                  !mentorshipData.selectedMentor ||
                  !mentorshipData.selectedSocialEnterprise
                } // 🔥 Disables if either field is empty
                sx={{
                  backgroundColor:
                    mentorshipData.selectedMentor &&
                      mentorshipData.selectedSocialEnterprise
                      ? "#1E4D2B"
                      : "#A0A0A0", // Gray when disabled
                  color: "#fff",
                  "&:hover": {
                    backgroundColor:
                      mentorshipData.selectedMentor &&
                        mentorshipData.selectedSocialEnterprise
                        ? "#145A32"
                        : "#A0A0A0", // Keep gray on hover if disabled
                  },
                }}
              >
                Submit
              </Button>
            </DialogActions>
          </Dialog>
        </Box>

        <Box display="flex" alignItems="center" gap={2}>
          {/* Remove Mentorship Button */}
          <Button
            variant="contained"
            sx={{
              backgroundColor: colors.redAccent[500],
              color: "black",
              "&:hover": { backgroundColor: colors.redAccent[600] },
            }}
            onClick={() => setIsModalOpen(true)}
          >
            Remove Mentorship
          </Button>

          {/* Remove Mentorship Modal */}
          <Dialog
            open={isModalOpen}
            onClose={() => setIsModalOpen(false)}
            fullWidth
            maxWidth="sm"
          >
            {/* Dialog Title */}
            <DialogTitle
              sx={{
                backgroundColor: "#1E4D2B", // DLSU Green header
                color: "#fff", // White text
                textAlign: "center",
                fontSize: "1.5rem",
                fontWeight: "bold",
              }}
            >
              Remove Mentorship
            </DialogTitle>

            {/* Dialog Content */}
            <DialogContent
              sx={{
                padding: "24px",
                maxHeight: "70vh", // Ensure it doesn't overflow the screen
                overflowY: "auto", // Enable scrolling if content is too long
                backgroundColor: "#fff", // White background
              }}
            >
              {/* Place Fields Side by Side */}
              <Box
                display="flex"
                gap={2} // Add spacing between fields
                alignItems="center" // Align fields vertically
                mb={2} // Add margin at the bottom
                marginTop="20px"
              >
                <Typography
                  variant="subtitle1"
                  sx={{
                    color: "#000", // Black text
                    fontWeight: "bold",
                    marginBottom: "8px", // Space between label and field
                  }}
                >
                  Mentor
                </Typography>
                {/* Mentor Selection (Autocomplete) */}
                <FormControl fullWidth margin="normal">
                  <Autocomplete
                    options={mentors}
                    getOptionLabel={(mentor) =>
                      `${mentor.mentor_firstName} ${mentor.mentor_lastName}`
                    }
                    value={selectedMentor}
                    onChange={(event, newValue) => setSelectedMentor(newValue)}
                    inputValue={mentorSearch}
                    onInputChange={(event, newInputValue) =>
                      setMentorSearch(newInputValue)
                    }
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        label="Select Mentor"
                        fullWidth
                        sx={{
                          "& .MuiOutlinedInput-notchedOutline": {
                            borderColor: "#000 !important", // Consistent border color
                          },
                          "&:hover .MuiOutlinedInput-notchedOutline": {
                            borderColor: "#000 !important",
                          },
                          "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                            borderColor: "#000 !important",
                          },
                          "& .Mui-disabled .MuiOutlinedInput-notchedOutline": {
                            borderColor: "#000 !important", // Ensures border is black when disabled
                          },
                          "& .MuiInputBase-input": {
                            color: "#000 !important", // Keeps text black when enabled
                          },
                          "& .Mui-disabled .MuiInputBase-input": {
                            color: "#000 !important", // Keeps text black when disabled
                            WebkitTextFillColor: "#000 !important", // Fixes opacity issue in some browsers
                          },
                        }}
                      />
                    )}
                  />
                </FormControl>

                <Typography
                  variant="subtitle1"
                  sx={{
                    color: "#000", // Black text
                    fontWeight: "bold",
                    marginBottom: "8px", // Space between label and field
                  }}
                >
                  Social Enterprise
                </Typography>
                {/* Social Enterprise Selection */}
                <FormControl fullWidth margin="normal">
                  <TextField
                    select
                    label="Select Social Enterprise"
                    fullWidth
                    value={selectedSE}
                    onChange={(event) => setSelectedSE(event.target.value)}
                    disabled={!selectedMentor || socialEnterprises.length === 0}
                    sx={{
                      "& .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000 !important", // Keeps border black
                      },
                      "&:hover .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000 !important",
                      },
                      "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000 !important",
                      },
                      "& .Mui-disabled .MuiOutlinedInput-notchedOutline": {
                        borderColor: "#000 !important", // Ensures black border when disabled
                      },
                      "& .MuiInputBase-input": {
                        color: "#000 !important", // Keeps text black when enabled
                      },
                      "& .Mui-disabled .MuiInputBase-input": {
                        color: "#000 !important", // Ensures text stays black when disabled
                        WebkitTextFillColor: "#000 !important", // Fixes opacity issue in some browsers
                      },
                    }}
                  >
                    {socialEnterprises.length > 0 ? (
                      socialEnterprises.map((se) => (
                        <MenuItem key={se.id} value={se.id}>
                          {se.name}
                        </MenuItem>
                      ))
                    ) : (
                      <MenuItem disabled>No SEs assigned</MenuItem>
                    )}
                  </TextField>
                </FormControl>
              </Box>
            </DialogContent>

            {/* Dialog Actions */}
            <DialogActions
              sx={{
                padding: "16px",
                borderTop: "1px solid #000", // Separator line
                backgroundColor: "#fff",
              }}
            >
              <Button
                onClick={() => {
                  setIsModalOpen(false);
                  setTimeout(() => {
                    window.location.reload();
                  }, 500); // Adjust delay if needed
                }}
                sx={{
                  color: "#000",
                  border: "1px solid #000",
                  "&:hover": {
                    backgroundColor: "#e0e0e0", // Hover effect
                  },
                }}
              >
                Cancel
              </Button>

              <Button
                onClick={handleRemoveMentorship}
                variant="contained"
                disabled={!selectedMentor || !selectedSE} // Disables if either field is empty
                sx={{
                  backgroundColor:
                    selectedMentor && selectedSE ? "#1E4D2B" : "#A0A0A0", // Gray when disabled
                  color: "#fff",
                  "&:hover": {
                    backgroundColor:
                      selectedMentor && selectedSE ? "#145A32" : "#A0A0A0", // Keep gray on hover if disabled
                  },
                }}
              >
                Confirm
              </Button>
            </DialogActions>
          </Dialog>
        </Box>
      </Box>

      {/* SE ASSIGNED Names */}
      <Dialog
        open={openRelatedSEs}
        onClose={() => setOpenRelatedSEs(false)}
        maxWidth="md"
        fullWidth
        PaperProps={{
          style: {
            backgroundColor: "#fff",
            color: "#000",
            border: "1px solid #000",
          },
        }}
      >
        {/* Dialog Title */}
        <DialogTitle
          sx={{
            backgroundColor: "#1E4D2B", // DLSU green
            color: "#fff",
            textAlign: "center",
            fontSize: "1.5rem",
            fontWeight: "bold",
          }}
        >
          Assigned Social Enterprises
        </DialogTitle>

        {/* Dialog Content */}
        <DialogContent
          sx={{
            padding: "24px",
            maxHeight: "70vh",
            overflowY: "auto",
          }}
        >
          {selectedSEs.length > 0 ? (
            <Box component="ul" sx={{ pl: 2 }}>
              {selectedSEs.map((seName, index) => (
                <li key={index}>
                  <Typography variant="body1">{seName}</Typography>
                </li>
              ))}
            </Box>
          ) : (
            <Typography variant="body1">No SEs assigned to this mentor.</Typography>
          )}
        </DialogContent>

        {/* Dialog Actions */}
        <DialogActions
          sx={{
            padding: "16px",
            borderTop: "1px solid #000",
          }}
        >
          <Button
            onClick={() => setOpenRelatedSEs(false)}
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

      <Box display="flex" gap="20px" width="100%" mt="20px">
        {/* MENTORS TABLE */}
        <Box
          flex="2"
          backgroundColor={colors.primary[400]}
          padding="20px"
        >
          {/* Top Row with Title and Button */}
          <Box
            display="flex"
            justifyContent="space-between"
            alignItems="center"
            marginBottom="15px"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
            >
              Mentors
            </Typography>

            {user?.roles?.includes("LSEED-Coordinator") && isAllowedtoApply && (
              <Button
                variant="contained"
                color="secondary"
                onClick={() => setOpenApplyDialog(true)}
                sx={{
                  backgroundColor: colors.greenAccent[500],
                  color: "#000",
                  textTransform: "none",
                  "&:hover": {
                    backgroundColor: colors.greenAccent[600],
                  },
                }}
              >
                Apply to Be a Mentor
              </Button>
            )}
          </Box>

          {/* DataGrid Box */}
          <Box
            width="100%"
            height="600px"
            minHeight="400px"
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
            <DataGrid
              rows={rows}
              columns={columns}
              getRowId={(row) => row.id}
              getRowHeight={() => 'auto'}
              editMode="row"
              sx={{
                "& .MuiDataGrid-cell": {
                  display: "flex",
                  alignItems: "center", // vertical centering
                  paddingTop: "12px",
                  paddingBottom: "12px",
                },
                "& .MuiDataGrid-columnHeader": {
                  alignItems: "center", // optional: center header label vertically
                },
                "& .MuiDataGrid-cellContent": {
                  whiteSpace: "normal",
                  wordBreak: "break-word",
                  overflowWrap: "break-word",
                },
                "& .MuiDataGrid-toolbarContainer .MuiButton-text": {
                  color: `${colors.grey[100]} !important`,
                },
              }}
              slots={{ toolbar: GridToolbar }}
            />
          </Box>
        </Box>

        {/* MENTOR APPLICATIONS TABLE */}
        {user?.roles?.includes("LSEED-Director") && (
          <Box
            flex="1"
            backgroundColor={colors.primary[400]}
            sx={{
              height: "700px", // 👈 adjust height as needed
              overflowY: "auto", // 👈 enables vertical scroll
            }}
          >
            <Box
              display="flex"
              justifyContent="space-between"
              alignItems="center"
              borderBottom={`4px solid ${colors.primary[500]}`}
              p="15px"
            >
              <Typography color={colors.greenAccent[500]} variant="h3" fontWeight="600">
                Applications
              </Typography>
            </Box>

            {mentorApplications.map((list, i) => (
              <Box
                key={`mentorApp-${list.id}`}
                display="flex"
                justifyContent="space-between"
                alignItems="center"
                borderBottom={`4px solid ${colors.primary[500]}`}
                p="15px"
                sx={{ minHeight: "72px" }}
              >
                {/* Left: Name & Email */}
                <Box
                  sx={{
                    flex: 1,
                    overflowWrap: "break-word",
                    whiteSpace: "normal",
                  }}
                >
                  <Typography
                    color={colors.greenAccent[500]}
                    variant="h5"
                    fontWeight="600"
                    sx={{
                      whiteSpace: "normal",
                      wordBreak: "break-word",
                    }}
                  >
                    {list.first_name} {list.last_name}
                  </Typography>
                  <Typography
                    color={colors.grey[100]}
                    sx={{
                      whiteSpace: "normal",
                      wordBreak: "break-word",
                    }}
                  >
                    {list.email}
                  </Typography>
                </Box>

                {/* Middle: Date */}
                <Box
                  sx={{
                    flexShrink: 0,
                    color: colors.grey[100],
                    paddingLeft: "20px",
                    paddingRight: "20px",
                  }}
                >
                  {list.date_applied}
                </Box>

                {/* Right: Action Button */}
                <Button
                  onClick={(e) => handleOpenMenu(e, list.id)}
                  endIcon={<KeyboardArrowDownIcon />}
                  sx={{
                    backgroundColor: colors.greenAccent[500],
                    color: "#fff",
                    border: `2px solid ${colors.greenAccent[500]}`,
                    borderRadius: "4px",
                    textTransform: "none",
                    padding: "6px 12px",
                    "&:hover": {
                      backgroundColor: colors.greenAccent[600],
                      borderColor: colors.greenAccent[600],
                    },
                  }}
                >
                  Action
                </Button>

                {menuRowId === list.id && (
                  <Menu
                    anchorEl={anchorEl}
                    open={Boolean(anchorEl)}
                    onClose={handleCloseMenu}
                    anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                    transformOrigin={{ vertical: "top", horizontal: "right" }}
                  >
                    <MenuItem
                      onClick={() => handleMenuAction("View", list)}
                      sx={{
                        color: colors.grey[100],
                        fontWeight: 500,
                        "&:hover": {
                          backgroundColor: colors.blueAccent[700],
                          color: "#fff",
                        },
                      }}
                    >
                      View
                    </MenuItem>

                    <MenuItem
                      onClick={() => handleMenuAction("Accept", list)}
                      sx={{
                        color: colors.greenAccent[500],
                        fontWeight: 500,
                        "&:hover": {
                          backgroundColor: colors.greenAccent[500],
                          color: "#fff",
                        },
                      }}
                    >
                      Accept
                    </MenuItem>

                    <MenuItem
                      onClick={() => handleMenuAction("Decline", list)}
                      sx={{
                        color: "#f44336",
                        fontWeight: 500,
                        "&:hover": {
                          backgroundColor: "#f44336",
                          color: "#fff",
                        },
                      }}
                    >
                      Decline
                    </MenuItem>
                  </Menu>
                )}
              </Box>
            ))}
          </Box>
        )}
      </Box>

      {/* MENTOR APPLICATIONS DIALOG BOX*/}
      <Dialog
        open={openApplyDialog}
        onClose={() => setOpenApplyDialog(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle sx={{ bgcolor: colors.primary[400], color: colors.greenAccent[500] }}>
          Apply to Be a Mentor
        </DialogTitle>
        <DialogContent sx={{ bgcolor: colors.primary[400] }}>
          <form onSubmit={handleApplySubmit}>
            {[
              {
                label: "Affiliation (Position/Organization)",
                key: "affiliation",
              },
              {
                label: "Reason/Motivation to volunteer",
                key: "motivation",
                multiline: true,
              },
              // { 
              //   label: "Areas of Expertise", 
              //   key: "expertise" 
              // },
            ].map((field) => (
              <TextField
                key={field.key}
                name={field.key}
                label={field.label}
                fullWidth
                required
                multiline={field.multiline}
                minRows={field.multiline ? 2 : undefined}
                value={formData[field.key]}
                onChange={(e) => handleMentorApplicationInputChange(e, field.key)}
                sx={{ mt: 2 }}
              />
            ))}

            {/* Business Areas */}
            {businessAreasSelect()}

            {/* Preferred Time */}
            {preferredTimeSelect()}

            {formData.preferredTime.includes("Other") && (
              <TextField
                label="Specify preferred time"
                fullWidth
                required
                value={formData.specificTime}
                onChange={(e) =>
                  setFormData((prev) => ({
                    ...prev,
                    specificTime: e.target.value,
                  }))
                }
                sx={{ mt: 2 }}
              />
            )}

            {/* Communication Modes */}
            {communicationModeSelect()}

            <Box display="flex" justifyContent="flex-end" mt={3}>
              <Button
                onClick={() => setOpenApplyDialog(false)}
                sx={{ mr: 2 }}
              >
                Cancel
              </Button>
              <Button
                variant="contained"
                type="submit"
                sx={{
                  backgroundColor: colors.greenAccent[500],
                  "&:hover": { backgroundColor: colors.greenAccent[600] },
                  color: "#fff",
                }}
              >
                Submit Application
              </Button>
            </Box>
          </form>
        </DialogContent>
      </Dialog>
      {/* MENTOR APPLICATIONS VIEW BOX */}
      <Dialog
        open={openApplicationDialog}
        onClose={() => setOpenApplicationDialog(false)}
        maxWidth="md"
        fullWidth
        PaperProps={{
          style: {
            backgroundColor: "#fff",
            color: "#000",
            border: "1px solid #000",
          },
        }}
      >
        <DialogTitle
          sx={{
            backgroundColor: "#1E4D2B",
            color: "#fff",
            textAlign: "center",
            fontSize: "1.5rem",
            fontWeight: "bold",
          }}
        >
          Mentor Application Details
        </DialogTitle>

        <DialogContent sx={{ padding: 3, maxHeight: "70vh", overflowY: "auto" }}>
          {selectedApplication ? (
            <Grid container spacing={2}>
              {/* Basic Info */}
              <Grid item xs={12}>
                <Typography variant="h6" fontWeight="bold" gutterBottom>
                  Basic Information
                </Typography>
              </Grid>
              <Grid item xs={6}><strong>Name:</strong> {selectedApplication.first_name} {selectedApplication.last_name}</Grid>
              {/* <Grid item xs={6}><strong>Expertise:</strong> {selectedApplication.expertise}</Grid> */}
              <Grid item xs={6}><strong>Email:</strong> {selectedApplication.email}</Grid>
              <Grid item xs={6}><strong>Contact No. :</strong> {selectedApplication.contact_no}</Grid>
              <Grid item xs={6}><strong>Affiliation:</strong> {selectedApplication.affiliation}</Grid>

              {/* Motivation & Areas */}
              <Grid item xs={12}>
                <Typography variant="h6" fontWeight="bold" gutterBottom mt={2}>
                  Mentor Motivation & Focus
                </Typography>
              </Grid>
              <Grid item xs={12}><strong>Motivation:</strong> {selectedApplication.motivation}</Grid>
              <Grid item xs={12}>
                <strong>Interested Critical Areas:</strong>{" "}
                {(selectedApplication.business_areas || []).join(", ")}
              </Grid>

              {/* Schedule & Platforms */}
              <Grid item xs={12}>
                <Typography variant="h6" fontWeight="bold" gutterBottom mt={2}>
                  Mentoring Preferences
                </Typography>
              </Grid>
              <Grid item xs={12}>
                <strong>Available Schedules:</strong>{" "}
                {(selectedApplication.preferred_time || []).join(", ")}
              </Grid>
              <Grid item xs={12}>
                <strong>Preferred Platforms:</strong>{" "}
                {(selectedApplication.communication_mode || []).join(", ")}
              </Grid>

              {/* Application Date */}
              <Grid item xs={6}>
                <strong>Date Applied:</strong>{" "}
                {selectedApplication.date_applied}
              </Grid>
            </Grid>
          ) : (
            <Typography>Loading...</Typography>
          )}
        </DialogContent>

        <DialogActions sx={{ padding: "16px", borderTop: "1px solid #000" }}>
          <Button
            onClick={() => setOpenApplicationDialog(false)}
            sx={{
              color: "#000",
              border: "1px solid #000",
              "&:hover": { backgroundColor: "#f0f0f0" },
            }}
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snackbarOpen}
        autoHideDuration={5000}
        onClose={() => setSnackbarOpen(false)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        <Alert
          onClose={() => setSnackbarOpen(false)}
          severity={snackbarSeverity}
          sx={{ width: "100%" }}
        >
          {snackbarMessage}
        </Alert>
      </Snackbar>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={4000}
        onClose={() => setSnackbar((prev) => ({ ...prev, open: false }))}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        <Alert
          onClose={() => setSnackbar((prev) => ({ ...prev, open: false }))}
          severity={snackbar.severity}
          sx={{ width: "100%" }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>

    </Box>
  );
};

export default Mentors;