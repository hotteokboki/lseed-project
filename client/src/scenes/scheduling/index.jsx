// Sample LSEED Insight Google Calendar
// https://calendar.google.com/calendar/u/0?cid=MWJlZDcwNTZhNzNhOGRhZGU0MjZkZjI2MzMyMTYzNDBjMDE3OWJhZGJmMjUyMGYyMjI0NmVlMTkyMzg2OTBiY0Bncm91cC5jYWxlbmRhci5nb29nbGUuY29t

import { tokens } from "../../theme";
import React, { useEffect, useState } from "react";
import { DataGrid } from "@mui/x-data-grid";
import { TimePicker } from "@mui/x-date-pickers";
import Calendar from "../../components/Calendar";
import {
  Box,
  Button,
  List,
  ListItem,
  ListItemText,
  ListItemButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  CircularProgress,
  Typography,
  Stack,
  Chip,
  Snackbar,
  TextField,
  Alert,
  useTheme,
  MenuItem,
  FormControl,
  InputLabel,
  Select,
} from "@mui/material";
import { LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers";
import { useAuth } from "../../context/authContext";
import Header from "../../components/Header";
import axios from "axios";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import { saveAs } from "file-saver";
import { preventDefault } from "@fullcalendar/core/internal";
import axiosClient from "../../api/axiosClient";

dayjs.extend(utc);
dayjs.extend(timezone);

const Scheduling = ({}) => {
  const [openModal, setOpenModal] = useState(false);
  const [openSEModal, setOpenSEModal] = useState(false);
  const [mentors, setMentors] = useState([]);
  const [mentorPendingSessions, setMentorPendingSessions] = useState([]);
  const [socialEnterprises, setSocialEnterprises] = useState([]);
  const [selectedSE, setSelectedSE] = useState(null);
  const [selectedDate, setSelectedDate] = useState(dayjs());
  const [isLoading, setIsLoading] = useState(false);
  const [mentorshipDates, setMentorshipDates] = useState([]);
  const { user, isMentorView, toggleView } = useAuth();
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [selectedTime, setSelectedTime] = useState(dayjs().startOf("hour"));
  const now = dayjs();
  {/* REFERENCE DELETE THIS LATER ON FOR SNACKBAR */}
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState("");
  const [snackbarSeverity, setSnackbarSeverity] = useState("success");
  const [zoomLink, setZoomLink] = useState("");
  const [startTime, setStartTime] = useState(null);
  const [endTime, setEndTime] = useState(null);
  const [error, setError] = useState("");
  const handleRedirect = () =>
    window.open("https://calendar.google.com", "_blank");
  const handleOpenModal = () => setOpenModal(true);
  const handleCloseModal = () => setOpenModal(false);
  const handleOpenSEModal = () => {
    fetchSocialEnterprises();
    setOpenSEModal(true);
  };
  const handleCloseSEModal = () => setOpenSEModal(false);
  const handleSelectSE = (se) => setSelectedSE(se);
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedRowId, setSelectedRowId] = useState(null);
  const [mentorSchedules, setMentorSchedules] = useState([]);
  const [mentorOwnHistory, setMentorOwnHistory] = useState([]);
  const [lseedHistory, setLseedHistory] = useState([]);
  const allSchedules = [...mentorOwnHistory, ...lseedHistory].filter(
    (item) => item.status === "Accepted"
  );

  const generateTimeSlots = () => {
    const slots = [];
    let time = dayjs().startOf("day"); // Start at 00:00
    for (let i = 0; i < 48; i++) {
      // 48 half-hour slots in a day
      slots.push(time.format("hh:mm A"));
      time = time.add(30, "minute");
    }
    return slots;
  };

  const timeSlots = generateTimeSlots();

  const handleAcceptClick = async (schedule) => {
   try {
      const {
        id,
        mentorship_id,
        realDate,
        realTime,
        zoom,
      } = schedule;

      const response = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/approveMentorship`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            mentoring_session_id: id,
            mentorship_id,
            mentorship_date: realDate,
            mentorship_time: realTime,
            zoom_link: zoom,
          }),
        }
      );

      if (!response.ok) {
        const errorMessage = await response.text();
        throw new Error(`Failed to approve mentorship: ${errorMessage}`);
      }

      setSnackbarMessage(
        "Mentoring session accepted. Awaiting SE confirmation."
      );
      setSnackbarSeverity("success");
      setSnackbarOpen(true);
      setTimeout(() => {
        window.location.reload();
      }, 500);
    } catch (error) {
      console.error("Error approving mentorship:", error);
    }
  };

  const isConfirmDisabled =
    !selectedSE ||
    !selectedDate ||
    !startTime ||
    !endTime ||
    !zoomLink ||
    isLoading;

  const generateICS = () => {
    if (mentorshipDates.length === 0) {
      alert("No scheduled mentorship dates to export.");
      return;
    }

    let icsContent =
      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//SEED Insight//Mentorship Schedule//EN\r\n";

    mentorshipDates.forEach((session) => {
      if (session.status !== "Accepted") {
        return; // Skip non-accepted sessions
      }

      const [year, month, day] = session.mentoring_session_date
        .split("T")[0]
        .split("-")
        .map(Number);
      const [hour, minute] = session.mentoring_session_time
        .split(" - ")[0]
        .split(":")
        .map(Number);

      // Construct date using local time (prevents timezone shift)
      const parsedDate = new Date(year, month - 1, day + 1, hour, minute);

      if (isNaN(parsedDate.getTime())) {
        console.error(
          "Invalid date:",
          session.mentoring_session_date,
          session.mentoring_session_time
        );
        return;
      }

      const startDate =
        parsedDate.toISOString().replace(/[-:]/g, "").split(".")[0] + "Z";
      const endDate =
        new Date(parsedDate.getTime() + 60 * 60 * 1000) // 1-hour session
          .toISOString()
          .replace(/[-:]/g, "")
          .split(".")[0] + "Z";

      icsContent += `BEGIN:VEVENT\r\n`;
      icsContent += `UID:${session.team_name}-${startDate}\r\n`;
      icsContent += `SUMMARY:Mentorship Session with ${session.team_name}\r\n`;
      icsContent += `DESCRIPTION:Mentor: ${session.mentor_name}\\nProgram: ${session.program_name}\\nStatus: ${session.status}\r\n`;
      icsContent += `DTSTART:${startDate}\r\n`;
      icsContent += `DTEND:${endDate}\r\n`;
      icsContent += `LOCATION:${
        session.zoom_link ? session.zoom_link : "TBD"
      }\r\n`;
      icsContent += `STATUS:CONFIRMED\r\n`;
      icsContent += `END:VEVENT\r\n`;
    });

    icsContent += "END:VCALENDAR\r\n";

    if (!icsContent.includes("BEGIN:VEVENT")) {
      alert("No Accepted mentorship schedules to export.");
      return;
    }

    // Create a Blob and trigger download
    const blob = new Blob([icsContent], { type: "text/calendar" });
    saveAs(blob, "mentorship_schedule.ics");
  };

  function mapSchedulesToEvents(schedules) {
    return schedules.map((item) => {
      const [startTime, endTime] = item.mentoring_session_time.split(" - ");

      const start = dayjs(
        `${item.mentoring_session_date} ${startTime}`
      ).format();
      const end = dayjs(`${item.mentoring_session_date} ${endTime}`).format();

      return {
        id: item.mentoring_session_id,
        title: `Mentoring Session for ${item.team_name}`,
        start,
        end,
        allDay: false,
        extendedProps: {
          zoom_link: item.zoom_link,
          status: item.status,
          team_name: item.team_name, // ✅ Add this
          mentor_name: item.mentor_name, // ✅ Add this
          program_name: item.program_name, // ✅ Add this
        },
      };
    });
  }

  const calendarEvents = mapSchedulesToEvents(allSchedules);

  const handleDeclineClick = async (schedule) => {
    try {
      const { id } = schedule;

      const response = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/declineMentorship`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            mentoring_session_id: id,
          }),
        }
      );

      if (!response.ok) {
        const errorMessage = await response.text();
        throw new Error(`Failed to decline mentorship: ${errorMessage}`);
      }

      setSnackbarMessage("Mentoring session declined.");
      setSnackbarSeverity("success");
      setSnackbarOpen(true);
      setTimeout(() => {
        window.location.reload();
      }, 500);
    } catch (error) {
      console.error("Error declining mentorship:", error);
    }
  };

  const handleStartTimeChange = (newStartTimeRaw) => {
    if (!newStartTimeRaw) return;
    const newStartTime = dayjs(newStartTimeRaw);
    setStartTime(newStartTime);
    setEndTime(newStartTime.add(30, "minute"));
  };

  const handleEndTimeChange = (newEndTimeRaw) => {
    if (!newEndTimeRaw) return;
    setEndTime(dayjs(newEndTimeRaw));
  };

  const handleCloseDialog = () => {
    setOpenDialog(false);
    setSelectedRowId(null);
  };

  const handleAccept = () => {
    console.log(`Accepted mentor schedule with ID: ${selectedRowId}`);
    handleCloseDialog();
  };

  const handleDecline = () => {
    console.log(`Declined mentor schedule with ID: ${selectedRowId}`);
    handleCloseDialog();
  };

  // Function to handle Snackbar close
  const handleSnackbarClose = () => {
    setSnackbarOpen(false);
  };

  useEffect(() => {
    const fetchMentorshipDates = async () => {
      try {
        console.log("mentor_id: ", user.id);
        const response = await axios.get(
          `${process.env.REACT_APP_API_BASE_URL}/getMentorshipDates`,
          {
            params: { mentor_id: user.id }, // Fetch mentorships for this mentor
          }
        );
        console.log("📅 Mentorship Dates:", response.data);
        setMentorshipDates(response.data || []);
      } catch (error) {
        console.error("Error fetching mentorship dates:", error);
        setMentorshipDates([]);
      }
    };

    fetchMentorshipDates();
  }, [user.id]); // Refetch when the user changes

  useEffect(() => {
    const fetchMentorPendingSessions = async () => {
      try {
        const res = await axios.get(
          `${process.env.REACT_APP_API_BASE_URL}/api/get-mentor-pending-sessions`,
          {
            withCredentials: true,
          }
        );
        setMentorPendingSessions(res.data || []);
      } catch (error) {
        console.error("❌ Error fetching mentor pending sessions:", error);
        setMentorPendingSessions([]);
      }
    };

    if (user?.roles.includes("Mentor")) {
      fetchMentorPendingSessions();
    }
  }, [user]);

  useEffect(() => {
    const fetchScheduleHistory = async () => {
      try {
        const roles = user?.roles || [];
        const isMentor = roles.includes("Mentor");
        const isLSEEDUser = roles.some((role) => role.startsWith("LSEED"));

        if (isMentor) {
          const mentorRes = await axios.get(
            `${process.env.REACT_APP_API_BASE_URL}/api/mentorSchedulesByID`,
            {
              withCredentials: true,
            }
          );
          setMentorOwnHistory(mentorRes.data || []);
        }

        if (isLSEEDUser) {
          let lseedResponse;
          if (roles.includes("LSEED-Coordinator")) {
            const programRes = await axiosClient.get(
              `/api/get-program-coordinator`);
            const program = programRes.data[0]?.name;
            lseedResponse = await axiosClient.get(
              `/api/get-mentor-schedules`,
              { params: { program } }
            );
          } else {
            lseedResponse = await axiosClient.get(
              `/api/get-mentor-schedules`
            );
          }
          setLseedHistory(lseedResponse.data || []);
        }
      } catch (error) {
        console.error("❌ Error fetching mentor schedules:", error);
        setMentorOwnHistory([]);
        setLseedHistory([]);
      }
    };

    if (user) {
      fetchScheduleHistory();
    }
  }, [user]);

  const formatRows = (data) =>
    data.map((mentorship) => ({
      id: mentorship.mentoring_session_id,
      sessionDetails: `${mentorship.status} Mentoring Session for ${
        mentorship.team_name || "Unknown SE"
      } with Mentor ${mentorship.mentor_name || "Unknown Mentor"}`,
      program_name: mentorship.program_name || "N/A",
      date:
        `${mentorship.mentoring_session_date}, ${mentorship.mentoring_session_time}` ||
        "N/A",
      mentoring_session_time: mentorship.mentoring_session_time || "N/A",
      status: mentorship.status || "N/A",
      zoom_link: mentorship.zoom_link || "N/A",
    }));

  const fetchSocialEnterprises = async () => {
    try {
      setIsLoading(true);

      const response = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/getMentorshipsbyID?mentor_id=${encodeURIComponent(user.id)}`
      );
      const data = await response.json();

      console.log("📥 Received Data in Scheduling:", data);

      if (!Array.isArray(data)) {
        console.error("Invalid data format:", data);
        setSocialEnterprises([]);
        return;
      }

      const updatedSocialEnterprises = [];

      for (const se of data) {
        try {
          const checkResponse = await fetch(
            `${process.env.REACT_APP_API_BASE_URL}/checkTelegramRegistration?mentor_id=${encodeURIComponent(se.mentor_id)}&se_id=${encodeURIComponent(se.se_id)}`
          );
          const checkData = await checkResponse.json();

          updatedSocialEnterprises.push({
            id: se.id,
            mentor_id: se.mentor_id,
            se_id: se.se_id,
            team_name: se.se || "Unknown Team",
            program_name: se.program || "Unknown Program",
            sdg_name: se.sdgs || "No SDG Name",
            preferred_times: se.preferred_mentoring_time || [],
            time_note: se.mentoring_time_note || "No Time Note",
            telegramRegistered: checkData.exists || false
          });
        } catch (error) {
          console.error("Error checking telegram registration:", error);

          // Assume not registered on error
          updatedSocialEnterprises.push({
            id: se.id,
            mentor_id: se.mentor_id,
            se_id: se.se_id,
            team_name: se.se || "Unknown Team",
            program_name: se.program || "Unknown Program",
            sdg_name: se.sdgs || "No SDG Name",
            preferred_times: se.preferred_mentoring_time || [],
            time_note: se.mentoring_time_note || "No Time Note",
            telegramRegistered: false
          });
        }
      }

      setSocialEnterprises(updatedSocialEnterprises);
    } catch (error) {
      console.error("❌ Error fetching social enterprises:", error);
    } finally {
      setIsLoading(false);
    }
  };


  const handleConfirmDate = async () => {
    console.log("SE ID:", selectedSE?.id);
    console.log("Date:", selectedDate?.format?.("YYYY-MM-DD"));
    console.log("Start Time:", startTime?.format?.("HH:mm"));
    console.log("End Time:", endTime?.format?.("HH:mm"));
    console.log("Zoom Link:", zoomLink);

    // Ensure all fields are filled before proceeding
    if (
      !selectedSE?.id ||
      !selectedDate ||
      !startTime ||
      !endTime ||
      !zoomLink
    ) {
      setSnackbarMessage("All fields are required: SE, Date, Start Time, End Time, Zoom Link.");
      setSnackbarSeverity("error");
      setSnackbarOpen(true);
      return;
    }

    try {
      setIsLoading(true);

      const teamName = selectedSE?.team_name || "Selected SE";
      const displayDate = selectedDate?.format("MMMM D, YYYY") || "selected date";
      const displayStartTime = startTime?.format("HH:mm") || "start time";
      const displayEndTime = endTime?.format("HH:mm") || "end time";

      // ✅ Ensure `selectedDate`, `startTime`, and `endTime` are dayjs objects before formatting
      const formattedDate = selectedDate?.isValid?.()
        ? selectedDate.format("YYYY-MM-DD")
        : null;
      const formattedStartTime =
        selectedDate?.isValid?.() && startTime?.isValid?.()
          ? `${selectedDate.format("YYYY-MM-DD")} ${startTime.format("HH:mm:ss")}`
          : null;

      const formattedEndTime =
        selectedDate?.isValid?.() && endTime?.isValid?.()
          ? `${selectedDate.format("YYYY-MM-DD")} ${endTime.format("HH:mm:ss")}`
          : null;

      if (!formattedDate || !formattedStartTime || !formattedEndTime) {
        throw new Error("Invalid date or time format.");
      }

      // ✅ Check that start time is not in the past
      const now = dayjs();
      const selectedStart = dayjs(`${formattedDate} ${startTime.format("HH:mm:ss")}`);
      if (selectedStart.isBefore(now)) {
        setSnackbarMessage("Start time must not be in the past. Please choose a future time.");
        setSnackbarSeverity("error");
        setSnackbarOpen(true);
        setIsLoading(false);
        return;
      }

      const requestBody = {
        mentorship_id: selectedSE.id,
        mentoring_session_date: formattedDate,
        start_time: formattedStartTime,
        end_time: formattedEndTime,
        zoom_link: zoomLink,
      };

      console.log("📤 Sending Data:", requestBody);

      const response = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/updateMentorshipDate`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody),
        }
      );

      if (!response.ok) {
        const errorMessage = await response.json().catch(() => response.text());
        throw new Error(`Failed to update: ${JSON.stringify(errorMessage)}`);
      }

      setSnackbarMessage(
        `Mentoring Session with ${teamName} on ${displayDate} at ${displayStartTime} - ${displayEndTime} scheduled successfully!`
      );
      setSnackbarSeverity("success");
      setSnackbarOpen(true);
      handleCloseSEModal();

      setTimeout(() => {
        window.location.reload();
      }, 5000);
    } catch (error) {
      console.error("❌ Error updating mentorship date:", error.message);
      let message = error.message;
      try {
        const parsed = JSON.parse(message.replace(/^Failed to update: /, ""));
        if (parsed && parsed.error) {
          message = parsed.error;
        }
      } catch (err) {
        // leave message as-is
      }

      setSnackbarMessage(message);
      setSnackbarSeverity("error");
      setSnackbarOpen(true);
    } finally {
      setIsLoading(false);
    }
  };

  // Define DataGrid columns
  const mentorOwnSessionHistory = [
    {
      field: "sessionDetails",
      headerName: "Mentoring Session Information",
      flex: 1,
      minWidth: 250,
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            whiteSpace: "normal",
            wordBreak: "break-word",
            lineHeight: 1.4,
            width: "100%",
          }}
        >
          {params.value}
        </Typography>
      ),
    },
    {
      field: "date",
      headerName: "Scheduled Date",
      width: 200,
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            whiteSpace: "normal",
            wordBreak: "break-word",
            lineHeight: 1.4,
            width: "100%",
          }}
        >
          {params.value}
        </Typography>
      ),
    },
    {
      field: "program_name",
      headerName: "Program",
      width: 100,
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            whiteSpace: "normal",
            wordBreak: "break-word",
            lineHeight: 1.4,
            width: "100%",
          }}
        >
          {params.value}
        </Typography>
      ),
    },
    {
      field: "status",
      headerName: "Status",
      width: 100,
      renderCell: (params) => {
        let color = "default";
        if (params.value === "Pending SE") color = "warning";
        if (params.value === "Accepted") color = "success";
        if (params.value === "Declined") color = "error";
        if (params.value === "Evaluated") color = "info";
        if (params.value === "Completed") color = "primary";

        return <Chip label={params.value} color={color} />;
      },
    },
    {
      field: "zoom_link",
      headerName: "Zoom Link",
      flex: 1,
      minWidth: 150,
      renderCell: (params) => {
        const { row } = params;
        const allowedStatuses = ["Accepted", "In Progress"];

        if (
          allowedStatuses.includes(row.status) &&
          row.zoom_link &&
          row.zoom_link !== "N/A"
        ) {
          return (
            <a
              href={row.zoom_link}
              target="_blank"
              rel="noopener noreferrer"
              style={{ textDecoration: "none" }}
            >
              <Chip label="Join" color="primary" clickable />
            </a>
          );
        }

        return "N/A";
      },
    },
  ];

  useEffect(() => {
    fetch("/auth/session-check", {
      method: "GET",
      credentials: "include", // ✅ Required for sending cookies
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((response) => response.json())
      .then((data) => console.log("[Frontend] Session Check Response:", data))
      .catch((error) =>
        console.error("[Frontend] Session Check Error:", error)
      );
  }, []);

  useEffect(() => {
    const fetchMentorSchedules = async () => {
      try {
        let response;

        if (user?.roles.includes("LSEED-Coordinator")) {
          const res = await axiosClient.get(`/api/get-program-coordinator`);

          const program = res.data[0]?.name;

          response = await axiosClient.get(
            `/api/pending-schedules`,
            { params: { program } }
          );
        } else {
          response = await axiosClient.get(`/api/pending-schedules`);
        }
        const data = response.data; // no need for .json() when using axios

        console.log("📅 Mentor Schedules Data:", data); // ✅ Debugging log

        if (Array.isArray(data)) {
          setMentorSchedules(data);
        } else {
          console.error("❌ Invalid mentor schedule format:", data);
        }
      } catch (error) {
        console.error("❌ Error fetching mentor schedules:", error);
        setMentorSchedules([]);
      }
    };

    fetchMentorSchedules();
  }, []);

  return (
    <Box m="20px">
      {/* Header */}
      <Header
        title="Scheduling Matrix"
        subtitle={
          user?.roles.some((role) => role.startsWith("LSEED"))
            ? "View and Manage the schedule of the mentors"
            : "Find the Appropriate Schedule"
        }
      />

      {/* --- Buttons Section --- */}
      <Box
        display="flex"
        flexDirection="column"
        alignItems="center"
        gap={2}
        mt={2}
        marginBottom="20px"
      >
        <Box
          width="100%"
          p={3}
          bgcolor={colors.primary[400]}
          display="flex"
          gap={2}
          flexDirection={{ xs: "column", md: "row" }} // For responsiveness
        >
          {/* Open LSEED Calendar Button */}
          {/* Visible if user has any LSEED role AND (is not a mentor OR is in coordinator view) */}
            

          {/* Schedule a Mentoring Session Button */}
          {/* Visible if user has Mentor role AND (is not an LSEED user OR is in mentor view) */}
          {user?.roles.includes("Mentor") &&
            (!user?.roles.some((r) => r.startsWith("LSEED")) ||
              (user?.roles.some((r) => r.startsWith("LSEED")) &&
                isMentorView)) && (
              <Button
                variant="contained"
                color="secondary"
                sx={{ fontSize: "16px", py: "10px", flexGrow: 1, minWidth: 0 }}
                onClick={handleOpenSEModal}
              >
                Schedule a Mentoring Session
              </Button>
            )}
        </Box>
      </Box>

      <Box mt={2}>
        <List>
          {mentors.map((mentor) => (
            <ListItem key={mentor.mentor_id}>
              <ListItemText primary={mentor.name} />
              <Button
                variant="contained"
                color={mentor.calendarlink ? "secondary" : "inherit"}
                sx={{
                  fontSize: "16px",
                  padding: "10px",
                  backgroundColor: mentor.calendarlink
                    ? colors.primary[500]
                    : "#B0B0B0",
                  color: "white",
                  "&:hover": {
                    backgroundColor: mentor.calendarlink
                      ? colors.primary[700]
                      : "#A0A0A0",
                  },
                }}
                onClick={() =>
                  mentor.calendarlink &&
                  window.open(mentor.calendarlink, "_blank")
                }
                disabled={!mentor.calendarlink}
              >
                {mentor.calendarlink
                  ? "View Calendar"
                  : "No Calendar Available"}
              </Button>
            </ListItem>
          ))}
        </List>
      </Box>

      <Box width="100%" backgroundColor={colors.primary[400]} padding="20px">
        <Calendar events={calendarEvents} isDashboard={true} />
      </Box>

      {!isMentorView && user?.roles.some((r) => r.startsWith("LSEED")) && (
        <Box mt={4} display="flex" flexDirection="column" gap={2}>
          {/* Pending Schedules for Coordinator View */}
          <Box
            width="100%"
            backgroundColor={colors.primary[400]}
            padding="20px"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
              marginBottom="15px"
            >
              Pending Schedules
            </Typography>

            <Box
              width="100%"
              height="400px"
              minHeight="400px"
              sx={{
                "& .MuiDataGrid-root": { border: "none" },
                "& .MuiDataGrid-cell": { borderBottom: "none" },
                "& .MuiDataGrid-columnHeaders, & .MuiDataGrid-columnHeader": {
                  backgroundColor: colors.blueAccent[700] + " !important",
                },
                "& .MuiDataGrid-virtualScroller": {
                  backgroundColor: colors.primary[400],
                },
                "& .MuiDataGrid-footerContainer": {
                  borderTop: "none",
                  backgroundColor: colors.blueAccent[700],
                  color: colors.grey[100],
                },
              }}
            >
              {mentorSchedules.length > 0 ? (
                <DataGrid
                  rows={mentorSchedules.map((schedule) => ({
                    id: schedule.mentoring_session_id,
                    sessionDetails: `Mentoring Session for ${
                      schedule.team_name || "Unknown SE"
                    } with Mentor ${schedule.mentor_name || "Unknown Mentor"}`,
                    date:
                      `${schedule.mentoring_session_date}, ${schedule.mentoring_session_time}` ||
                      "N/A",
                    time: schedule.mentoring_session_time || "N/A",
                    zoom: schedule.zoom_link || "N/A",
                    mentorship_id: schedule.mentorship_id,
                    status: schedule.status || "Pending",
                    realDate: schedule.mentoring_session_date || "N/A",     // for backend
                    realTime: schedule.mentoring_session_time || "N/A",     // for backend
                  }))}
                  columns={[
                    {
                      field: "sessionDetails",
                      headerName: "Mentoring Session Information",
                      flex: 1,
                      minWidth: 200,
                      renderCell: (params) => (
                        <Typography
                          variant="body2"
                          sx={{
                            whiteSpace: "normal",
                            wordBreak: "break-word",
                            lineHeight: 1.4,
                            width: "100%",
                          }}
                        >
                          {params.value}
                        </Typography>
                      ),
                    },
                    {
                      field: "date",
                      headerName: "Scheduled Date",
                      flex: 1,
                      minWidth: 200,
                      renderCell: (params) => (
                        <Typography
                          variant="body2"
                          sx={{
                            whiteSpace: "normal",
                            wordBreak: "break-word",
                            lineHeight: 1.4,
                            width: "100%",
                          }}
                        >
                          {params.value}
                        </Typography>
                      ),
                    },
                    {
                      field: "status",
                      headerName: "Status",
                      flex: 1,
                      minWidth: 150,
                      renderCell: (params) => (
                        <Box
                          sx={{
                            width: "100%",
                            display: "flex",
                            alignItems: "center",
                          }}
                        >
                          <Chip
                            label={params.value}
                            color={
                              params.value === "Pending"
                                ? "warning"
                                : colors.greenAccent[600]
                            }
                            sx={{ width: "fit-content" }}
                          />
                        </Box>
                      ),
                    },
                    {
                      field: "actions",
                      headerName: "Action",
                      flex: 1,
                      minWidth: 200,
                      renderCell: (params) => (
                        <Box
                          sx={{
                            width: "100%",
                            display: "flex",
                            justifyContent: "center",
                            alignItems: "center",
                            gap: 1,
                          }}
                        >
                          <Button
                            variant="contained"
                            sx={{
                              backgroundColor: colors.greenAccent[500],
                              "&:hover": {
                                backgroundColor: colors.greenAccent[600],
                              },
                            }}
                            onClick={() => handleAcceptClick(params.row)}
                          >
                            Accept
                          </Button>
                          <Button
                            variant="contained"
                            sx={{
                              backgroundColor: colors.redAccent[500],
                              "&:hover": {
                                backgroundColor: colors.redAccent[600],
                              },
                            }}
                            onClick={() => handleDeclineClick(params.row)}
                          >
                            Decline
                          </Button>
                        </Box>
                      ),
                    },
                  ]}
                  pageSize={5}
                  sx={{
                    "& .MuiDataGrid-cell": {
                      display: "flex",
                      alignItems: "center",
                      paddingTop: "12px",
                      paddingBottom: "12px",
                    },
                    "& .MuiDataGrid-cellContent": {
                      whiteSpace: "normal",
                      wordBreak: "break-word",
                    },
                  }}
                  getRowHeight={() => "auto"}
                  rowsPerPageOptions={[5, 10]}
                />
              ) : (
                <Typography color={colors.grey[300]}>
                  No schedules available.
                </Typography>
              )}
            </Box>
          </Box>

          {/* Mentorship History */}
          <Box
            width="100%"
            backgroundColor={colors.primary[400]}
            padding="20px"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
              marginBottom="15px"
            >
              Mentoring Sessions History
            </Typography>

            {lseedHistory.length > 0 ? (
              <Box
                width="100%"
                height="400px"
                minHeight="400px"
                sx={{
                  "& .MuiDataGrid-scrollbarFiller, & .MuiDataGrid-scrollbarFiller--header":
                    {
                      backgroundColor: colors.blueAccent[700] + " !important",
                    },
                  "& .MuiDataGrid-root": { border: "none" },
                  "& .MuiDataGrid-cell": { borderBottom: "none" },
                  "& .MuiDataGrid-columnHeaders, & .MuiDataGrid-columnHeader": {
                    backgroundColor: colors.blueAccent[700] + " !important",
                  },
                  "& .MuiDataGrid-virtualScroller": {
                    backgroundColor: colors.primary[400],
                  },
                  "& .MuiDataGrid-footerContainer": {
                    borderTop: "none",
                    backgroundColor: colors.blueAccent[700],
                    color: colors.grey[100],
                  },
                }}
              >
                <DataGrid
                  rows={formatRows(lseedHistory)}
                  columns={[
                    {
                      field: "sessionDetails",
                      headerName: "Mentoring Session Information",
                      flex: 1,
                      minWidth: 250,
                      renderCell: (params) => (
                        <Typography
                          variant="body2"
                          sx={{
                            whiteSpace: "normal",
                            wordBreak: "break-word",
                            lineHeight: 1.4,
                            width: "100%",
                          }}
                        >
                          {params.value}
                        </Typography>
                      ),
                    },
                    {
                      field: "date",
                      headerName: "Scheduled Date",
                      width: 250,
                      renderCell: (params) => (
                        <Typography
                          variant="body2"
                          sx={{
                            whiteSpace: "normal",
                            wordBreak: "break-word",
                            lineHeight: 1.4,
                            width: "100%",
                          }}
                        >
                          {params.value}
                        </Typography>
                      ),
                    },
                    {
                      field: "program_name",
                      headerName: "Program",
                      width: 250,
                      renderCell: (params) => (
                        <Typography
                          variant="body2"
                          sx={{
                            whiteSpace: "normal",
                            wordBreak: "break-word",
                            lineHeight: 1.4,
                            width: "100%",
                          }}
                        >
                          {params.value}
                        </Typography>
                      ),
                    },
                    {
                      field: "status",
                      headerName: "Status",
                      width: 200,
                      renderCell: (params) => {
                        let color = "default";
                        if (params.value === "Pending SE") color = "warning";
                        if (params.value === "Accepted") color = "success";
                        if (params.value === "Declined") color = "error";
                        if (params.value === "Evaluated") color = "info";
                        if (params.value === "Completed") color = "primary";

                        return <Chip label={params.value} color={color} />;
                      },
                    },
                    {
                      field: "zoom_link",
                      headerName: "Zoom Link",
                      flex: 1,
                      minWidth: 150,
                      renderCell: (params) => {
                        const { row } = params;
                        const allowedStatuses = ["Accepted", "In Progress"];

                        if (
                          allowedStatuses.includes(row.status) &&
                          row.zoom_link &&
                          row.zoom_link !== "N/A"
                        ) {
                          return (
                            <a
                              href={row.zoom_link}
                              target="_blank"
                              rel="noopener noreferrer"
                              style={{ textDecoration: "none" }}
                            >
                              <Chip label="Join" color="primary" clickable />
                            </a>
                          );
                        }

                        return "N/A";
                      },
                    },
                  ]}
                  sx={{
                    "& .MuiDataGrid-cell": {
                      display: "flex",
                      alignItems: "center",
                      paddingTop: "12px",
                      paddingBottom: "12px",
                    },
                    "& .MuiDataGrid-cellContent": {
                      whiteSpace: "normal",
                      wordBreak: "break-word",
                    },
                  }}
                  pageSize={5}
                  getRowHeight={() => "auto"}
                  rowsPerPageOptions={[5, 10]}
                />
              </Box>
            ) : (
              <Typography>No mentorship history available.</Typography>
            )}
          </Box>
        </Box>
      )}

      <Dialog open={openDialog} onClose={handleCloseDialog}>
        <DialogTitle>Manage Mentor Schedule</DialogTitle>
        <DialogContent>
          <Typography>
            Do you want to accept or decline this mentor schedule?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleAccept} color="success">
            Accept
          </Button>
          <Button onClick={handleDecline} color="error">
            Decline
          </Button>
        </DialogActions>
      </Dialog>

      {isMentorView && user?.roles.includes("Mentor") && (
        <Box mt={4}>
          {/* Export Button Positioned Outside DataGrid */}
          <Box mb={2}>
            <Button
              variant="contained"
              sx={{
                backgroundColor: colors.greenAccent[600],
                color: colors.grey[100],
                "&:hover": { backgroundColor: colors.greenAccent[700] },
              }}
              onClick={generateICS}
            >
              Export Calendar
            </Button>
          </Box>

          <Box display="flex" gap="20px" width="100%" mt="20px">
            {/* Mentoring Sessions History (wider, flex=2) */}
            <Box
              flex="2"
              backgroundColor={colors.primary[400]}
              padding="20px"
              overflow="auto"
            >
              <Typography
                variant="h3"
                fontWeight="bold"
                color={colors.greenAccent[500]}
                marginBottom="15px"
              >
                My Mentoring Sessions History
              </Typography>

              {mentorOwnHistory.length > 0 ? (
                <Box
                  sx={{
                    height: 400,
                    width: "100%",
                    overflowX: "auto",
                    "& .MuiDataGrid-root": { border: "none" },
                    "& .MuiDataGrid-cell": { borderBottom: "none" },
                    "& .MuiDataGrid-columnHeaders, & .MuiDataGrid-columnHeader":
                      {
                        backgroundColor: colors.blueAccent[700] + " !important",
                      },
                    "& .MuiDataGrid-virtualScroller": {
                      backgroundColor: colors.primary[400],
                    },
                    "& .MuiDataGrid-footerContainer": {
                      borderTop: "none",
                      backgroundColor: colors.blueAccent[700],
                      color: colors.grey[100],
                    },
                  }}
                >
                  <DataGrid
                    rows={formatRows(mentorOwnHistory)}
                    columns={mentorOwnSessionHistory}
                    sx={{
                      "& .MuiDataGrid-cell": {
                        display: "flex",
                        alignItems: "center",
                        paddingTop: "12px",
                        paddingBottom: "12px",
                      },
                      "& .MuiDataGrid-cellContent": {
                        whiteSpace: "normal",
                        wordBreak: "break-word",
                      },
                    }}
                    pageSize={5}
                    getRowHeight={() => "auto"}
                    rowsPerPageOptions={[5, 10]}
                  />
                </Box>
              ) : (
                <Typography>No mentorship history available.</Typography>
              )}
            </Box>

            {/* My Pending Sessions (narrower, flex=1) */}
            <Box
              flex="1"
              backgroundColor={colors.primary[400]}
              padding="20px"
              overflow="auto"
            >
              <Typography
                variant="h3"
                fontWeight="bold"
                color={colors.greenAccent[500]}
                marginBottom="15px"
              >
                My Pending Sessions
              </Typography>

              {mentorPendingSessions.length > 0 ? (
                mentorPendingSessions.map((session, i) => (
                  <Box
                    key={session.mentoring_session_id || i}
                    display="flex"
                    justifyContent="space-between"
                    alignItems="center"
                    borderBottom={`4px solid ${colors.primary[500]}`}
                    p="15px"
                    sx={{
                      minHeight: "72px",
                      backgroundColor: colors.primary[400],
                    }}
                  >
                    {/* Left: Mentoring Session Information */}
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
                        {session.team_name}
                      </Typography>
                      <Typography
                        color={colors.grey[100]}
                        sx={{
                          whiteSpace: "normal",
                          wordBreak: "break-word",
                        }}
                      >
                        Pending Mentoring Session on{" "}
                        {session.mentoring_session_date},{" "}
                        {session.mentoring_session_time}
                      </Typography>
                    </Box>

                    {/* Right: Status */}
                    <Box
                      sx={{
                        flexShrink: 0,
                        minWidth: "120px",
                        display: "flex",
                        justifyContent: "center",
                      }}
                    >
                      <Chip
                        label={session.status}
                        color={
                          session.status === "Pending SE"
                            ? "warning"
                            : session.status === "Accepted"
                            ? "success"
                            : session.status === "Declined"
                            ? "error"
                            : session.status === "Evaluated"
                            ? "info"
                            : session.status === "Completed"
                            ? "primary"
                            : "default"
                        }
                      />
                    </Box>
                  </Box>
                ))
              ) : (
                <Typography>
                  No pending mentoring sessions available.
                </Typography>
              )}
            </Box>
          </Box>
        </Box>
      )}

      <Dialog
        open={openSEModal}
        onClose={handleCloseSEModal}
        fullWidth
        maxWidth="sm"
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
          Appoint a Mentoring Session
        </DialogTitle>

        {/* Dialog Content */}
        <DialogContent
          sx={{
            padding: "24px",
            maxHeight: "70vh", // Ensure it doesn't overflow the screen
            overflowY: "auto", // Enable scrolling if content is too long
          }}
        >
          {/* Loading Indicator */}
          {isLoading ? (
            <Box
              display="flex"
              justifyContent="center"
              alignItems="center"
              height="100%"
            >
              <CircularProgress />
            </Box>
          ) : (
            <Box>
              {/* Heading - only show if not yet selected */}
              {!selectedSE && (
                <Typography
                  variant="h6"
                  sx={{
                    mb: 2,
                    fontWeight: "bold",
                    color: "#d32f2f",
                  }}
                >
                  You have not yet selected a Social Enterprise
                </Typography>
              )}

              <List>
                {socialEnterprises.map((se) => (
                  <ListItem key={se.id} disablePadding>
                    <ListItemButton
                      onClick={() => handleSelectSE(se)}
                      disabled={!se.telegramRegistered}
                      sx={{
                        marginBottom: "6px",
                        border:
                          selectedSE?.id === se.id ? "2px solid #000" : "none",
                        borderRadius: "4px",
                        "&:hover": {
                          backgroundColor: "#f0f0f0",
                        },
                        opacity: se.telegramRegistered ? 1 : 0.6, // Make disabled entries look grayed out
                      }}
                    >
                      <ListItemText
                        primary={se.team_name}
                        secondary={
                          <Box component="span" sx={{ display: "block" }}>
                            {se.program_name}
                            <br />
                            <strong>Preferred Times:</strong>{" "}
                            {se.preferred_times.length > 0 ? se.preferred_times.join(", ") : "None"}
                            <br />
                            <strong>Time Notes:</strong> {se.time_note}
                            <br />
                            {!se.telegramRegistered && (
                              <Typography
                                variant="caption"
                                color="error"
                                sx={{
                                  display: "block",
                                  marginTop: "4px",
                                  fontWeight: "bold",
                                }}
                              >
                                Please let the social enterprise register in Telegram.
                              </Typography>
                            )}
                          </Box>
                        }
                        primaryTypographyProps={{
                          fontWeight: selectedSE?.id === se.id ? "bold" : "normal",
                          color: "black",
                        }}
                        secondaryTypographyProps={{
                          color: "black",
                        }}
                      />
                    </ListItemButton>
                  </ListItem>
                ))}
              </List>
            </Box>
          )}

          {/* Date & Time Selection Section */}
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
            {/* Date Selection */}
            <LocalizationProvider dateAdapter={AdapterDayjs}>
              <DatePicker
                label="Select Date"
                value={selectedDate}
                onChange={(newDate) => setSelectedDate(newDate)}
                minDate={dayjs()}
                slotProps={{
                  textField: {
                    sx: {
                      "& .MuiOutlinedInput-root": {
                        "& fieldset": { borderColor: "#000" },
                        "&:hover fieldset": { borderColor: "#000" },
                        "&.Mui-focused fieldset": { borderColor: "#000" },
                      },
                      "& .MuiInputBase-input": { color: "#000" },
                      "& .MuiInputLabel-root": { color: "#000" },
                      "& .MuiInputLabel-root.Mui-focused": { color: "#000" },
                    },
                    InputProps: {
                      sx: {
                        "& .MuiSvgIcon-root": { color: "#000" },
                      },
                    },
                  },
                  popper: {
                    sx: {
                      "& .MuiPaper-root": {
                        backgroundColor: "#1E4D2B", // Green background
                        color: "#fff",
                      },
                      "& .MuiPickersDay-root": { color: "#fff" },
                      "& .MuiPickersDay-root.Mui-selected": {
                        backgroundColor: "#fff !important",
                        color: "#1E4D2B",
                      },
                      "& .MuiIconButton-root": { color: "#fff" },
                      "& .MuiTypography-root": { color: "#fff" },
                      "& .MuiOutlinedInput-root": { borderColor: "#fff" },
                    },
                  },
                }}
              />
            </LocalizationProvider>

            <LocalizationProvider dateAdapter={AdapterDayjs}>
              <Stack spacing={2}>
                <Stack spacing={2} direction="row" sx={{ width: "100%" }}>
                  {/* Start Time Picker */}
                  <TimePicker
                    label="Start Time"
                    value={startTime}
                    onChange={handleStartTimeChange}
                    minTime={now}
                    slotProps={{
                      textField: {
                        fullWidth: true,
                        sx: {
                          "& .MuiOutlinedInput-root": {
                            "& fieldset": { borderColor: "#000" },
                            "&:hover fieldset": { borderColor: "#000" },
                            "&.Mui-focused fieldset": { borderColor: "#000" },
                          },
                          "& .MuiInputBase-input": { color: "#000" },
                          "& .MuiInputLabel-root": { color: "#000" },
                          "& .MuiInputLabel-root.Mui-focused": {
                            color: "#000",
                          },
                          "& .MuiSvgIcon-root": { color: "#000" }, // Icon color black
                        },
                      },
                      popper: {
                        sx: {
                          "& .MuiPaper-root": {
                            backgroundColor: "#1E4D2B", // Green dropdown background
                            color: "#fff", // White text
                          },
                          "& .MuiMenuItem-root": { color: "#fff" }, // Default item color
                          "& .MuiMenuItem-root.Mui-selected": {
                            backgroundColor: "#fff !important",
                            color: "#1E4D2B", // Green text for selected item
                          },
                        },
                      },
                      actionBar: {
                        actions: ["cancel", "accept"],
                        sx: {
                          "& .MuiButton-root": {
                            color: colors.grey[100], // Change OK button text color to white
                          },
                        },
                      },
                    }}
                  />

                  {/* End Time Picker */}
                  <TimePicker
                    label="End Time"
                    value={endTime}
                    minTime={startTime} // Prevents selecting a time before the start
                    onChange={handleEndTimeChange}
                    slotProps={{
                      textField: {
                        fullWidth: true,
                        sx: {
                          "& .MuiOutlinedInput-root": {
                            "& fieldset": { borderColor: "#000" },
                            "&:hover fieldset": { borderColor: "#000" },
                            "&.Mui-focused fieldset": { borderColor: "#000" },
                          },
                          "& .MuiInputBase-input": { color: "#000" },
                          "& .MuiInputLabel-root": { color: "#000" },
                          "& .MuiInputLabel-root.Mui-focused": {
                            color: "#000",
                          },
                          "& .MuiSvgIcon-root": { color: "#000" }, // Icon color black
                        },
                      },
                      popper: {
                        sx: {
                          "& .MuiPaper-root": {
                            backgroundColor: "#1E4D2B", // Green dropdown background
                            color: "#fff", // White text
                          },
                          "& .MuiMenuItem-root": { color: "#fff" }, // Default item color
                          "& .MuiMenuItem-root.Mui-selected": {
                            backgroundColor: "#fff !important",
                            color: "#1E4D2B", // Green text for selected item
                          },
                        },
                      },
                      actionBar: {
                        actions: ["cancel", "accept"],
                        sx: {
                          "& .MuiButton-root": {
                            color: colors.grey[100], // Change OK button text color to white
                          },
                        },
                      },
                    }}
                  />
                </Stack>

                {/* Error Message */}
                {error && (
                  <Typography color="error" sx={{ fontSize: "14px" }}>
                    {error}
                  </Typography>
                )}
              </Stack>
            </LocalizationProvider>
          </Box>

          {/* Zoom Link Section */}
          {selectedSE && (
            <>
              <Typography
                variant="h6"
                sx={{
                  marginTop: "20px",
                  marginBottom: "10px",
                  fontWeight: "bold",
                }}
              >
                Enter Zoom Link for Mentoring
              </Typography>
              <TextField
                label="Zoom Link"
                value={zoomLink}
                onChange={(e) => setZoomLink(e.target.value)}
                fullWidth
                sx={{
                  "& .MuiOutlinedInput-root": {
                    "& fieldset": {
                      borderColor: "#000", // Black border
                    },
                    "&:hover fieldset": {
                      borderColor: "#000", // Black border on hover
                    },
                    "&.Mui-focused fieldset": {
                      borderColor: "#000", // Black border when focused
                    },
                  },
                  "& .MuiInputBase-input": {
                    color: "#000", // Black text
                  },
                  "& .MuiInputLabel-root": {
                    color: "#000", // Black label text
                  },
                  "& .MuiInputLabel-root.Mui-focused": {
                    color: "#000", // Black label text when focused
                  },
                }}
              />
            </>
          )}
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
              handleCloseSEModal(); // Close the modal
              setTimeout(() => {
                window.location.reload(); // Reload after 500ms
              }, 500);
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
            onClick={handleConfirmDate}
            disabled={isConfirmDisabled}
            sx={{
              backgroundColor: isConfirmDisabled ? "#A9A9A9" : "#1E4D2B", // Grey out if disabled
              color: "#fff",
              "&:hover": {
                backgroundColor: isConfirmDisabled ? "#A9A9A9" : "#145A32", // Only hover effect when enabled
              },
            }}
          >
            {isLoading ? "Updating..." : "Confirm"}
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
    </Box>
  );
};

export default Scheduling;
