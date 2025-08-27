import ClearIcon from "@mui/icons-material/Clear";
import GroupOutlinedIcon from "@mui/icons-material/GroupOutlined";
import HandshakeOutlinedIcon from "@mui/icons-material/HandshakeOutlined";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import { Alert, Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, IconButton, Menu, MenuItem, Snackbar, Table, TableBody, TableCell, TableHead, TableRow, TextField, Tooltip, Typography, useTheme } from "@mui/material";
import { DataGrid, GridToolbar } from "@mui/x-data-grid";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import axiosClient from "../../api/axiosClient";
import Header from "../../components/Header";
import { tokens } from "../../theme";

const CollaborationDashboard = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [mentorships, setMentorships] = useState([]);
  const [suggestedCollaborations, setSuggestedCollaborations] = useState([]);
  const [selectedMentorshipId, setSelectedMentorshipId] = useState("");
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState("");
  const [snackbarSeverity, setSnackbarSeverity] = useState("success");
  const [existingCollaborations, setExistingCollaborations] = useState([]);
  const [menuRowId, setMenuRowId] = useState(null);
  const [anchorEl, setAnchorEl] = useState(null);
  const [open, setOpen] = useState(false);
  const [openCollaborateDialog, setCollaborateDialog] = useState(false);
  const [expandedCardId, setExpandedCardId] = useState(null);
  const [requests, setRequests] = useState([]);
  const [selectedRequest, setSelectedRequest] = useState(null);

  const navigate = useNavigate();

  const toggleCardDetails = (id) => {
    setExpandedCardId((prevId) => (prevId === id ? null : id));
  };

  useEffect(() => {
    const fetchMentorships = async () => {
      try {
        const res = await axiosClient.get(`/api/mentorship/get-collaborators`);
        setMentorships(res.data || []);
      } catch (err) {
        console.error("Error fetching mentorships:", err);
      }
    };

    fetchMentorships();
  }, []);

  const handleOpenMenu = (event, id) => {
    setMenuRowId(id);
    setAnchorEl(event.currentTarget);
  };

  const handleCloseMenu = () => {
    setMenuRowId(null);
    setAnchorEl(null);
  };

  const handleMenuAction = async (action, request) => {
    if (action === "View") {
      try {
        const res = await axiosClient.get(
          `/api/mentorship/view-collaboration-request/${request.mentorship_collaboration_request_id}`
        );
        setSelectedRequest(res.data[0]);
        setOpen(true);
      } catch (err) {
        console.error("Error fetching full request details:", err);
      }
    } else if (action === "Accept") {
      try {
        let res = await axiosClient.post(
          `/api/mentorship/insert-collaboration`, {
          collaboration_request_details: request
        }
        );
        if (res.status === 200) {
          setSnackbarMessage("Collaboration Accepted Successfully");
          setSnackbarSeverity("success");
          setSnackbarOpen(true);
          setOpen(false);
        }
      } catch (err) {
        console.error("Error fetching full request details:", err);
      }
    } else if (action === "Decline") {
      // TODO handle decline of collaborations
      try {
        let res = await axiosClient.post(
          `/api/mentorship/insert-collaboration`, {
          collaboration_request_details: request
        }
        );
        if (res.status === 200) {
          setSnackbarMessage("Collaboration Accepted Successfully");
          setSnackbarSeverity("success");
          setSnackbarOpen(true);
          setOpen(false);
        }
      } catch (err) {
        console.error("Error fetching full request details:", err);
      }
    }
    handleCloseMenu();
  };

  const handleClose = () => {
    setOpen(false);
  };

  useEffect(() => {
    const fetchExistingCollaborations = async () => {
      try {
        const res = await axiosClient.get(
          `/api/mentorship/get-collaborations`,
        );

        const sanitizedData = (res.data || []).map((item) => ({
          ...item,
          created_at: item.created_at
            ? new Date(item.created_at).toLocaleDateString("en-US", {
              year: "numeric",
              month: "long",
              day: "numeric",
            })
            : "—",
          roleLabel: item.initiated_by_user ? "Mentorship SE" : "Collaborated SE",
        }));

        setExistingCollaborations(sanitizedData);
      } catch (err) {
        console.error("Error fetching existing collaborations:", err);
      }
    };

    fetchExistingCollaborations();
  }, []);

  useEffect(() => {
    const fetchCollaborationRequests = async () => {
      try {
        const res = await axiosClient.get(
          `/api/mentorship/get-collaboration-requests`,
        );

        setRequests(res.data);
      } catch (err) {
        console.error("Error fetching collaboration requests:", err);
      }
    };

    fetchCollaborationRequests();
  }, []);

  const handleRequestCollaboration = async (details) => {
    try {
      await axiosClient.post(`/api/mentorship/request-collaboration`, {
        collaboration_request_details: details
      });
      setSnackbarMessage("Collaboration request sent successfully.");
      setSnackbarSeverity("success");
      setSnackbarOpen(true);
    } catch (err) {
      console.error(err);
      setSnackbarMessage("Failed to send collaboration request.");
      setSnackbarSeverity("error");
      setSnackbarOpen(true);
    }
  };

  useEffect(() => {
    if (!selectedMentorshipId) return;

    const fetchSuggestedCollaborations = async () => {
      try {
        const res = await axiosClient.get(
          `/api/mentorship/suggested-collaborations/${selectedMentorshipId}`,
          { withCredentials: true }
        );

        setSuggestedCollaborations(
          Array.isArray(res.data)
            ? res.data.map((item) => {
              const suggestedId = item.suggested_collaboration_se_id ?? "";
              const seekingId = item.seeking_collaboration_se_id ?? "";

              const collaborationCardId = `${suggestedId}_${seekingId}`;

              return {
                ...item,
                priority: Number(item.tier) || 4,
                matched_categories: Array.isArray(item.matched_categories) ? item.matched_categories : [],
                seeking_collaboration_se_strengths: item.seeking_collaboration_se_strengths ?? [],
                seeking_collaboration_se_weaknesses: item.seeking_collaboration_se_weaknesses ?? [],
                suggested_collaboration_se_strengths: item.suggested_collaboration_se_strengths ?? [],
                suggested_collaboration_se_weaknesses: item.suggested_collaboration_se_weaknesses ?? [],
                collaborationCardId,
              };
            })
            : []
        );
      } catch (error) {
        console.error("Error fetching suggested collaborations:", error);
      }
    };

    fetchSuggestedCollaborations();
  }, [selectedMentorshipId]);

  const handleViewSE = (id) => {
    navigate(`/se-analytics/${id}`);
  };

  const collaborationColumns = [
    {
      field: "own_se_name",
      headerName: "Your Mentorship SE",
      flex: 1,
    },
    {
      field: "collaborating_se_name",
      headerName: "Collaborated SE Name",
      flex: 1,
    },
    {
      field: "collaborating_mentor_name",
      headerName: "Collaborated Mentor Name",
      flex: 1,
    },
    {
      field: "created_at",
      headerName: "Started On",
      flex: 1,
    },
    {
      field: "is_active",
      headerName: "Status",
      flex: 0.5,
      renderCell: (params) => (
        <Typography color={params.value ? "green" : "red"}>
          {params.value ? "Active" : "Inactive"}
        </Typography>
      ),
    },
    {
      field: "actions",
      headerName: "Actions",
      flex: 0.8,
      renderCell: (params) => (
        <Button
          variant="contained"
          sx={{
            color: "#fff",
            backgroundColor: colors.greenAccent[500],
            "&:hover": { backgroundColor: colors.greenAccent[700] },
          }}
          onClick={() => handleViewSE(params.row.collaborating_se_id)}
        >
          View SE
        </Button>
      ),
    },
  ];

  return (
    <Box m="20px">
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Header
          title="Collaboration Dashboard"
          subtitle="Welcome to Collaborations"
        />
      </Box>
      {/* TODO: Fix the statboxes */}
      {/* Row 2 - Collaboration Insights StatBoxes */}
      <Box
        display="flex"
        flexWrap="wrap"
        gap="20px"
        justifyContent="space-between"
        mt="20px"
      >
        {/* No. of Collaborations */}
        <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <HandshakeOutlinedIcon sx={{ fontSize: 40, color: colors.greenAccent[500], mb: 1 }} />
          <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
            69
          </Typography>
          <Typography variant="subtitle2" color={colors.grey[300]}>
            No. of Collaborations
          </Typography>
        </Box>

        {/* No. of Involved Social Enterprises */}
        <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <GroupOutlinedIcon sx={{ fontSize: 40, color: colors.blueAccent[500], mb: 1 }} />
          <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
            60
          </Typography>
          <Typography variant="subtitle2" color={colors.grey[300]}>
            No. of Evaluations
          </Typography>
        </Box>

        {/* Shared / Peer-Identified Strengths */}
        {/* <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <ThumbUpAltOutlinedIcon sx={{ fontSize: 40, color: colors.blueAccent[300], mb: 1 }} />
          <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
            {sharedStrengthsCount || peerStrengthsCount || 0}
          </Typography>
          <Typography variant="subtitle2" color={colors.grey[300]} align="center">
            {strengthsLabel}
          </Typography>
        </Box> */}

        {/* Shared / Peer-Identified Weaknesses */}
        {/* <Box
          flex="1 1 22%"
          backgroundColor={colors.primary[400]}
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
          p="20px"
        >
          <ThumbDownAltOutlinedIcon sx={{ fontSize: 40, color: colors.redAccent[300], mb: 1 }} />
          <Typography variant="h4" fontWeight="bold" color={colors.grey[100]}>
            {sharedWeaknessesCount || peerWeaknessesCount || 0}
          </Typography>
          <Typography variant="subtitle2" color={colors.grey[300]} align="center">
            {weaknessesLabel}
          </Typography>
        </Box> */}
      </Box>

      <Button
        variant="contained"
        sx={{
          backgroundColor: colors.greenAccent[500],
          color: "black",
          "&:hover": {
            backgroundColor: colors.greenAccent[600],
          },
        }}
        onClick={() => setCollaborateDialog(true)}
      >
        Collaborate
      </Button>

      <Dialog
        open={openCollaborateDialog}
        onClose={() => {
          setCollaborateDialog(false);
          setSelectedMentorshipId("");
        }}
        maxWidth="lg"
        fullWidth
      >
        {/* Dialog Title */}
        <DialogTitle
          sx={{
            backgroundColor: "#1E4D2B",
            color: "#fff",
            textAlign: "center",
            fontSize: "1.5rem",
            fontWeight: "bold",
          }}
        >
          Collaboration Opportunities
        </DialogTitle>

        {/* Dialog Content */}
        <DialogContent
          sx={{
            backgroundColor: "#fff",
            color: "#000",
            padding: "24px",
            maxHeight: "70vh",
            overflowY: "auto",
          }}
        >
          {/* Mentorship Selector */}
          {!selectedMentorshipId && (
            <Box mb={4}>
              <Typography variant="h6" fontWeight="bold" gutterBottom>
                Explore Collaboration Opportunities with your Mentorships
              </Typography>
              <TextField
                select
                fullWidth
                label="Select Mentorship"
                variant="outlined"
                value={selectedMentorshipId}
                onChange={(e) => setSelectedMentorshipId(e.target.value)}
                sx={{
                  mt: 1,
                  "& .MuiOutlinedInput-root": {
                    border: "1px solid #000",
                    borderRadius: "4px",
                    "&:hover": {
                      borderColor: "#000",
                    },
                    "&.Mui-focused": {
                      borderColor: "#000",
                    },
                  },
                  "& .MuiInputBase-input": {
                    color: "#000",
                  },
                  "& .MuiInputLabel-root": {
                    backgroundColor: "#fff",
                    padding: "0 4px",
                  },
                }}
              >
                <MenuItem value="">-- None Selected --</MenuItem>
                {mentorships.map((m) => (
                  <MenuItem key={m.mentorship_id} value={m.mentorship_id}>
                    {m.social_enterprise_name || "Untitled Social Enterprise"}
                  </MenuItem>
                ))}
              </TextField>
            </Box>
          )}

          {/* Suggestion Panel */}
          {selectedMentorshipId && (
            <Box>
              <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
                <Typography variant="h5" fontWeight="bold">
                  Suggested Collaborations for{" "}
                  {mentorships.find((m) => m.mentorship_id === selectedMentorshipId)?.social_enterprise_name || "Selected Mentorship"}
                </Typography>

                <Tooltip title="Clear mentorship selection">
                  <IconButton
                    onClick={() => setSelectedMentorshipId("")}
                    color="error"
                    size="large"
                  >
                    <ClearIcon />
                  </IconButton>
                </Tooltip>
              </Box>

              <Typography variant="body2" mb={2}>
                These recommendations are based on complementary strengths and weaknesses in evaluation categories.
              </Typography>

              {/* Tier 1–3 */}
              {suggestedCollaborations.some((s) => s.priority >= 1 && s.priority <= 3) && (
                <>
                  <Typography variant="subtitle1" fontWeight="bold" color="#1E4D2B" mt={2} mb={1}>
                    Suggested Collaborators
                  </Typography>
                  <Box display="grid" gridTemplateColumns="repeat(auto-fill, minmax(300px, 1fr))" gap={2}>
                    {suggestedCollaborations
                      .filter((s) => s.priority >= 1 && s.priority <= 3)
                      .map((item, index) => {
                        const selectedSEName = mentorships.find((m) => m.mentorship_id === selectedMentorshipId)?.social_enterprise_name || "this social enterprise";
                        const explanation = {
                          1: `Strengths of ${item.se_b_name} address the weaknesses of ${selectedSEName}.`,
                          2: `${item.se_b_name} shares common strengths with ${selectedSEName}.`,
                          3: `${item.se_b_name} shares common weaknesses with ${selectedSEName}.`,
                        }[item.priority] || "";

                        return (
                          <Box
                            key={index}
                            p={2}
                            border="1px solid #000"
                            borderRadius="8px"
                            bgcolor="#F5F5F5"
                            display="flex"
                            flexDirection="column"
                            justifyContent="space-between"
                          >
                            <Box>
                              <Typography fontWeight="bold" mb={1}>
                                👤 Mentor: {item.suggested_collaboration_mentor_name}
                              </Typography>

                              <Typography fontSize="0.9rem" mb={0.5}>
                                SE to Collaborate With: <strong>{item.suggested_collaboration_se_name}</strong>
                              </Typography>

                              <Typography fontSize="0.9rem" mb={0.5}>
                                Matched Categories:{" "}
                                {item.matched_categories.length > 0 ? item.matched_categories.join(", ") : <i>None</i>}
                              </Typography>

                              <Box mt={2} mb={3} key={item.collaborationCardId}>
                                {/* Toggle Button */}
                                <Button
                                  variant="text"
                                  size="small"
                                  onClick={() => toggleCardDetails(item.collaborationCardId)}
                                  sx={{ textTransform: "none", fontWeight: "bold", color: "#1E4D2B", mb: 1 }}
                                >
                                  {expandedCardId === item.collaborationCardId
                                    ? "Hide Collaboration Details"
                                    : "Show Collaboration Details"}
                                </Button>

                                {/* Hidden Content */}
                                {expandedCardId === item.collaborationCardId && (
                                  <>
                                    {(item.priority === 1 || item.priority === 2 || item.priority === 3) && (
                                      <>
                                        <Typography fontSize="0.85rem" fontWeight="bold" color="#1E4D2B" mt={1}>
                                          Why this match?
                                        </Typography>

                                        <Typography fontSize="0.85rem" mb={1}>
                                          {item.priority === 1 && (
                                            <>
                                              <b>{item.suggested_collaboration_se_abbreviation}</b> has strengths in:{" "}
                                              <i>{item.matched_categories.join(", ")}</i> — which are weaknesses for{" "}
                                              <b>{item.seeking_collaboration_se_abbreviation}</b>.
                                            </>
                                          )}
                                          {item.priority === 2 && (
                                            <>
                                              Both enterprises share strong capabilities in:{" "}
                                              <i>{item.matched_categories.join(", ")}</i>.
                                            </>
                                          )}
                                          {item.priority === 3 && (
                                            <>
                                              Both enterprises are working on improving:{" "}
                                              <i>{item.matched_categories.join(", ")}</i>. A collaboration may help share solutions.
                                            </>
                                          )}
                                        </Typography>
                                      </>
                                    )}

                                    {/* Strengths & Weaknesses - Seeking SE */}
                                    <Box mb={1}>
                                      <Typography fontSize="0.8rem" color="gray" mb={0.2}>
                                        <strong>{item.seeking_collaboration_se_abbreviation}</strong>
                                      </Typography>
                                      <Typography fontSize="0.8rem" color="gray" mb={0.2} ml={1}>
                                        <strong>Strengths:</strong>
                                      </Typography>
                                      {item.seeking_collaboration_se_strengths?.length > 0 ? (
                                        item.seeking_collaboration_se_strengths.map((strength, index) => (
                                          <Typography key={index} fontSize="0.8rem" color="gray" ml={3}>
                                            – {strength}
                                          </Typography>
                                        ))
                                      ) : (
                                        <Typography fontSize="0.8rem" color="gray" ml={3}>
                                          – None
                                        </Typography>
                                      )}
                                      <Typography fontSize="0.8rem" color="gray" mt={1} mb={0.2} ml={1}>
                                        <strong>Weaknesses:</strong>
                                      </Typography>
                                      {item.seeking_collaboration_se_weaknesses?.length > 0 ? (
                                        item.seeking_collaboration_se_weaknesses.map((weakness, index) => (
                                          <Typography key={index} fontSize="0.8rem" color="gray" ml={3}>
                                            – {weakness}
                                          </Typography>
                                        ))
                                      ) : (
                                        <Typography fontSize="0.8rem" color="gray" ml={3}>
                                          – None
                                        </Typography>
                                      )}
                                    </Box>

                                    {/* Strengths & Weaknesses - Suggested SE */}
                                    <Box mb={1}>
                                      <Typography fontSize="0.8rem" color="gray" mb={0.2}>
                                        <strong>{item.suggested_collaboration_se_abbreviation}</strong>
                                      </Typography>
                                      <Typography fontSize="0.8rem" color="gray" mb={0.2} ml={1}>
                                        <strong>Strengths:</strong>
                                      </Typography>
                                      {item.suggested_collaboration_se_strengths?.length > 0 ? (
                                        item.suggested_collaboration_se_strengths.map((strength, index) => (
                                          <Typography key={index} fontSize="0.8rem" color="gray" ml={3}>
                                            – {strength}
                                          </Typography>
                                        ))
                                      ) : (
                                        <Typography fontSize="0.8rem" color="gray" ml={3}>
                                          – None
                                        </Typography>
                                      )}
                                      <Typography fontSize="0.8rem" color="gray" mt={1} mb={0.2} ml={1}>
                                        <strong>Weaknesses:</strong>
                                      </Typography>
                                      {item.suggested_collaboration_se_weaknesses?.length > 0 ? (
                                        item.suggested_collaboration_se_weaknesses.map((weakness, index) => (
                                          <Typography key={index} fontSize="0.8rem" color="gray" ml={3}>
                                            – {weakness}
                                          </Typography>
                                        ))
                                      ) : (
                                        <Typography fontSize="0.8rem" color="gray" ml={3}>
                                          – None
                                        </Typography>
                                      )}
                                    </Box>
                                  </>
                                )}
                              </Box>
                            </Box>

                            <Button
                              variant="contained"
                              onClick={() => handleRequestCollaboration(item)}
                              sx={{ mt: 2, backgroundColor: "#1E4D2B", color: "#fff", "&:hover": { backgroundColor: "#155A2E" } }}
                              fullWidth
                            >
                              Request Collaboration
                            </Button>
                          </Box>
                        );
                      })}
                  </Box>
                </>
              )}

              {/* Tier 4 */}
              {suggestedCollaborations.some((s) => s.priority === 4) && (
                <>
                  <Typography variant="subtitle1" fontWeight="bold" color="gray" mt={4} mb={1}>
                    Other Collaborators
                  </Typography>
                  <Box display="grid" gridTemplateColumns="repeat(auto-fill, minmax(300px, 1fr))" gap={2}>
                    {suggestedCollaborations
                      .filter((s) => s.priority === 4)
                      .map((item, index) => (
                        <Box
                          key={index}
                          p={2}
                          border="1px solid #000"
                          borderRadius="8px"
                          bgcolor="#F5F5F5"
                          display="flex"
                          flexDirection="column"
                          justifyContent="space-between"
                        >
                          <Box>
                            <Typography fontWeight="bold" mb={1}>
                              👤 Mentor: {item.suggested_collaboration_mentor_name}
                            </Typography>

                            <Typography fontSize="0.9rem" mb={0.5}>
                              SE to Collaborate With: <strong>{item.suggested_collaboration_se_name}</strong>
                            </Typography>

                            <Box mt={2} mb={3} key={item.collaborationCardId}>
                              {/* Toggle Button */}
                              <Button
                                variant="text"
                                size="small"
                                onClick={() => toggleCardDetails(item.collaborationCardId)}
                                sx={{ textTransform: "none", fontWeight: "bold", color: "#1E4D2B", mb: 1 }}
                              >
                                {expandedCardId === item.collaborationCardId
                                  ? "Hide Collaboration Details"
                                  : "Show Collaboration Details"}
                              </Button>

                              {/* Collapsible Section */}
                              {expandedCardId === item.collaborationCardId && (
                                <>
                                  {item.subtier && (
                                    <>
                                      <Typography fontSize="0.85rem" fontWeight="bold" color="#1E4D2B" mt={1}>
                                        Why this fallback match?
                                      </Typography>

                                      <Typography fontSize="0.85rem" mb={1}>
                                        {item.subtier === 1 && (
                                          <>
                                            <b>{item.suggested_collaboration_se_abbreviation}</b> has strengths in:{" "}
                                            <i>{item.matched_categories.join(", ")}</i> — which are weaknesses for{" "}
                                            <b>{item.seeking_collaboration_se_abbreviation}</b>.
                                          </>
                                        )}
                                        {item.subtier === 2 && (
                                          <>
                                            Both enterprises share strong capabilities in:{" "}
                                            <i>{item.matched_categories.join(", ")}</i>.
                                          </>
                                        )}
                                        {item.subtier === 3 && (
                                          <>
                                            Both enterprises are working on improving:{" "}
                                            <i>{item.matched_categories.join(", ")}</i>. A collaboration may help share solutions.
                                          </>
                                        )}
                                      </Typography>
                                    </>
                                  )}

                                  {/* Seeking SE Traits */}
                                  <Box mb={1}>
                                    <Typography fontSize="0.8rem" color="gray" mb={0.2}>
                                      <strong>{item.seeking_collaboration_se_abbreviation}</strong>
                                    </Typography>
                                    <Typography fontSize="0.8rem" color="gray" mb={0.2} ml={1}>
                                      <strong>Strengths:</strong>
                                    </Typography>
                                    {item.seeking_collaboration_se_strengths?.length > 0 ? (
                                      item.seeking_collaboration_se_strengths.map((strength, i) => (
                                        <Typography key={i} fontSize="0.8rem" color="gray" ml={3}>
                                          – {strength}
                                        </Typography>
                                      ))
                                    ) : (
                                      <Typography fontSize="0.8rem" color="gray" ml={3}>
                                        – None
                                      </Typography>
                                    )}

                                    <Typography fontSize="0.8rem" color="gray" mt={1} mb={0.2} ml={1}>
                                      <strong>Weaknesses:</strong>
                                    </Typography>
                                    {item.seeking_collaboration_se_weaknesses?.length > 0 ? (
                                      item.seeking_collaboration_se_weaknesses.map((weakness, i) => (
                                        <Typography key={i} fontSize="0.8rem" color="gray" ml={3}>
                                          – {weakness}
                                        </Typography>
                                      ))
                                    ) : (
                                      <Typography fontSize="0.8rem" color="gray" ml={3}>
                                        – None
                                      </Typography>
                                    )}
                                  </Box>

                                  {/* Suggested SE Traits */}
                                  <Box mb={1}>
                                    <Typography fontSize="0.8rem" color="gray" mb={0.2}>
                                      <strong>{item.suggested_collaboration_se_abbreviation}</strong>
                                    </Typography>
                                    <Typography fontSize="0.8rem" color="gray" mb={0.2} ml={1}>
                                      <strong>Strengths:</strong>
                                    </Typography>
                                    {item.suggested_collaboration_se_strengths?.length > 0 ? (
                                      item.suggested_collaboration_se_strengths.map((strength, i) => (
                                        <Typography key={i} fontSize="0.8rem" color="gray" ml={3}>
                                          – {strength}
                                        </Typography>
                                      ))
                                    ) : (
                                      <Typography fontSize="0.8rem" color="gray" ml={3}>
                                        – None
                                      </Typography>
                                    )}

                                    <Typography fontSize="0.8rem" color="gray" mt={1} mb={0.2} ml={1}>
                                      <strong>Weaknesses:</strong>
                                    </Typography>
                                    {item.suggested_collaboration_se_weaknesses?.length > 0 ? (
                                      item.suggested_collaboration_se_weaknesses.map((weakness, i) => (
                                        <Typography key={i} fontSize="0.8rem" color="gray" ml={3}>
                                          – {weakness}
                                        </Typography>
                                      ))
                                    ) : (
                                      <Typography fontSize="0.8rem" color="gray" ml={3}>
                                        – None
                                      </Typography>
                                    )}
                                  </Box>
                                </>
                              )}
                            </Box>
                          </Box>

                          <Button
                            variant="contained"
                            onClick={() => handleRequestCollaboration(item)}
                            sx={{
                              mt: 2,
                              backgroundColor: "#1E4D2B",
                              color: "#fff",
                              "&:hover": { backgroundColor: "#155A2E" },
                            }}
                            fullWidth
                          >
                            Request Collaboration
                          </Button>
                        </Box>
                      ))}
                  </Box>
                </>
              )}

              {/* No Suggestions */}
              {suggestedCollaborations.length === 0 && (
                <Typography mt={2} color="gray">
                  No suggestions found for the selected mentorship.
                </Typography>
              )}
            </Box>
          )}
        </DialogContent>

        {/* Dialog Footer */}
        <DialogActions sx={{ backgroundColor: "#f5f5f5", p: 2 }}>
          <Button
            onClick={() => {
              setCollaborateDialog(false);
              setSelectedMentorshipId("");
            }}
            sx={{ color: "#1E4D2B", fontWeight: "bold" }}
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <Box display="flex" gap="20px" width="100%" mt="20px">
        {/* EXISTING COLLABORATIONS TABLE */}
        <Box
          flex="2"
          backgroundColor={colors.primary[400]}
          padding="20px"
        >
          <Typography variant="h3" fontWeight="bold" color={colors.greenAccent[500]} mb={2}>
            Existing Collaborations
          </Typography>

          <Box
            height="600px"
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
            <DataGrid
              rows={existingCollaborations.length > 0 ? existingCollaborations.map((row, i) => ({ id: i, ...row })) : []}
              columns={collaborationColumns}
              pageSize={5}
              rowsPerPageOptions={[5, 10]}
              getRowId={(row) => row.collaboration_id}
              getRowHeight={() => 'auto'}
              slots={{ toolbar: GridToolbar }}
              disableSelectionOnClick
              sx={{
                "& .MuiDataGrid-cell": {
                  display: "flex",
                  alignItems: "center",
                  paddingTop: "12px",
                  paddingBottom: "12px",
                },
                "& .MuiDataGrid-columnHeader": {
                  alignItems: "center",
                },
                "& .MuiDataGrid-cellContent": {
                  whiteSpace: "normal",
                  wordBreak: "break-word",
                },
                "& .MuiDataGrid-toolbarContainer .MuiButton-text": {
                  color: `${colors.grey[100]} !important`,
                },
              }}
              localeText={{ noRowsLabel: "No Existing Collaborations" }}
            />
          </Box>
        </Box>

        {/* COLLABORATION REQUESTS LIST */}
        <Box
          flex="1"
          backgroundColor={colors.primary[400]}
          sx={{
            height: "700px",
            overflowY: "auto",
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
              Collaboration Requests
            </Typography>
          </Box>

          {Array.isArray(requests) && requests.map((list, i) => (
            <Box
              key={`${list.mentorship_collaboration_request_id}-${i}`}
              display="flex"
              justifyContent="space-between"
              alignItems="center"
              borderBottom={`4px solid ${colors.primary[500]}`}
              p="15px"
              sx={{ minHeight: "72px" }}
            >
              {/* Left: Collaborating SE Info */}
              <Box sx={{ flex: 1, overflowWrap: "break-word", whiteSpace: "normal" }}>
                <Typography
                  color={colors.greenAccent[500]}
                  variant="h5"
                  fontWeight="600"
                  sx={{ whiteSpace: "normal", wordBreak: "break-word" }}
                >
                  {list.seeking_collaboration_se_name}
                </Typography>
                <Typography
                  color={colors.grey[100]}
                  sx={{ whiteSpace: "normal", wordBreak: "break-word" }}
                >
                  {list.seeking_collaboration_se_abbreviation}
                </Typography>
              </Box>

              {/* Middle: Date */}
              <Box sx={{ flexShrink: 0, color: colors.grey[100], px: 2 }}>
                {new Date(list.created_at).toLocaleDateString("en-PH", {
                  year: "numeric",
                  month: "short",
                  day: "numeric",
                })}
              </Box>

              {/* Right: Action Button */}
              <Button
                onClick={(e) => handleOpenMenu(e, list.mentorship_collaboration_request_id)}
                endIcon={<KeyboardArrowDownIcon />}
                sx={{
                  backgroundColor: colors.greenAccent[500],
                  color: "#fff",
                  border: `2px solid ${colors.greenAccent[500]}`,
                  borderRadius: "4px",
                  textTransform: "none",
                  px: 2,
                  py: 1,
                  "&:hover": {
                    backgroundColor: colors.greenAccent[600],
                    borderColor: colors.greenAccent[600],
                  },
                }}
              >
                Action
              </Button>

              {menuRowId === list.mentorship_collaboration_request_id && (
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
      </Box>

      <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
        <DialogTitle
          sx={{
            backgroundColor: colors.primary[400],
            color: colors.greenAccent[500],
            fontWeight: "bold",
            fontSize: "1.5rem",
          }}
        >
          Collaboration Request Details
        </DialogTitle>

        <DialogContent sx={{ backgroundColor: colors.primary[400], padding: "24px" }}>
          <Table>
            <TableBody>
              {/* Basic Info */}
              <TableRow>
                <TableCell sx={{ color: colors.grey[300], fontWeight: "bold" }}>Mentor</TableCell>
                <TableCell sx={{ color: colors.grey[100] }}>
                  {selectedRequest?.seeking_collaboration_mentor_name || "—"}
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ color: colors.grey[300], fontWeight: "bold" }}>Collaborating SE</TableCell>
                <TableCell sx={{ color: colors.grey[100] }}>
                  {selectedRequest?.seeking_collaboration_se_name} ({selectedRequest?.seeking_collaboration_se_abbreviation})
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ color: colors.grey[300], fontWeight: "bold" }}>Your Mentorship SE</TableCell>
                <TableCell sx={{ color: colors.grey[100] }}>
                  {selectedRequest?.suggested_collaboration_se_name} ({selectedRequest?.suggested_collaboration_se_abbreviation})
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ color: colors.grey[300], fontWeight: "bold" }}>Purpose</TableCell>
                <TableCell sx={{ color: colors.grey[100] }}>
                  {(selectedRequest?.tier === 1 || (selectedRequest?.tier === 4 && selectedRequest?.subtier === 1)) && (
                    <>The collaborating SE is seeking support to <strong>address their weaknesses by leveraging your SE’s strengths</strong>.</>
                  )}

                  {(selectedRequest?.tier === 2 || (selectedRequest?.tier === 4 && selectedRequest?.subtier === 2)) && (
                    <>The collaborating SE sees value in working with your SE due to <strong>shared strengths</strong> in key areas.</>
                  )}

                  {(selectedRequest?.tier === 3 || (selectedRequest?.tier === 4 && selectedRequest?.subtier === 3)) && (
                    <>The collaborating SE wants to <strong>mutually address shared weaknesses</strong> through collaboration.</>
                  )}
                </TableCell>
              </TableRow>

              {/* Matrix */}
              <TableRow>
                <TableCell colSpan={2}>
                  <Typography variant="h6" color={colors.greenAccent[500]} mt={2} mb={1}>
                    Strengths & Weaknesses Matrix
                  </Typography>

                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ fontWeight: "bold", color: colors.grey[300] }}></TableCell>
                        <TableCell sx={{ fontWeight: "bold", color: colors.grey[300] }}>
                          {selectedRequest?.seeking_collaboration_se_abbreviation} (Collaborating SE)
                        </TableCell>
                        <TableCell sx={{ fontWeight: "bold", color: colors.grey[300] }}>
                          {selectedRequest?.suggested_collaboration_se_abbreviation} (Your Mentorship SE)
                        </TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {[
                        "Teamwork",
                        "Logistics",
                        "Marketing Plan/Execution",
                        "Human Resource Management",
                        "Financial Planning/Management",
                        "Product/Service Design/Planning",
                      ].map((cat) => {
                        const scs_strengths = selectedRequest?.seeking_collaboration_se_strengths ?? [];
                        const scs_weaknesses = selectedRequest?.seeking_collaboration_se_weaknesses ?? [];
                        const sgs_strengths = selectedRequest?.suggested_collaboration_se_strengths ?? [];
                        const sgs_weaknesses = selectedRequest?.suggested_collaboration_se_weaknesses ?? [];

                        const isTier1Highlight =
                          (selectedRequest?.tier === 1 || (selectedRequest?.tier === 4 && selectedRequest?.subtier === 1)) &&
                          scs_weaknesses.includes(cat) &&
                          sgs_strengths.includes(cat);

                        const isTier2Highlight =
                          (selectedRequest?.tier === 2 || (selectedRequest?.tier === 4 && selectedRequest?.subtier === 2)) &&
                          scs_strengths.includes(cat) &&
                          sgs_strengths.includes(cat);

                        const isTier3Highlight =
                          (selectedRequest?.tier === 3 || (selectedRequest?.tier === 4 && selectedRequest?.subtier === 3)) &&
                          scs_weaknesses.includes(cat) &&
                          sgs_weaknesses.includes(cat);

                        const highlight = isTier1Highlight || isTier2Highlight || isTier3Highlight;

                        return (
                          <TableRow key={cat}>
                            <TableCell
                              sx={{
                                fontWeight: "bold",
                                color: colors.grey[300],
                                whiteSpace: "normal",
                                wordBreak: "break-word",
                                maxWidth: "200px",
                              }}
                            >
                              {cat}
                            </TableCell>

                            {/* Collaborating SE Cell */}
                            <TableCell
                              sx={{
                                color: colors.grey[100],
                                backgroundColor:
                                  highlight && (scs_strengths.includes(cat) || scs_weaknesses.includes(cat))
                                    ? colors.greenAccent[700]
                                    : "transparent",
                              }}
                            >
                              {scs_strengths.includes(cat)
                                ? "Strength"
                                : scs_weaknesses.includes(cat)
                                  ? "Weakness"
                                  : "—"}
                            </TableCell>

                            {/* Your Mentorship SE Cell */}
                            <TableCell
                              sx={{
                                color: colors.grey[100],
                                backgroundColor:
                                  highlight && (sgs_strengths.includes(cat) || sgs_weaknesses.includes(cat))
                                    ? colors.greenAccent[700]
                                    : "transparent",
                              }}
                            >
                              {sgs_strengths.includes(cat)
                                ? "Strength"
                                : sgs_weaknesses.includes(cat)
                                  ? "Weakness"
                                  : "—"}
                            </TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </TableCell>
              </TableRow>

              {/* Date */}
              <TableRow>
                <TableCell sx={{ color: colors.grey[300], fontWeight: "bold" }}>Requested On</TableCell>
                <TableCell sx={{ color: colors.grey[100] }}>
                  {selectedRequest?.created_at
                    ? new Date(selectedRequest.created_at).toLocaleDateString("en-PH", {
                      year: "numeric",
                      month: "long",
                      day: "numeric",
                    })
                    : "—"}
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </DialogContent>

        <DialogActions sx={{ backgroundColor: colors.primary[400], padding: "16px" }}>
          <Button variant="contained" color="primary" onClick={handleClose}>
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snackbarOpen}
        autoHideDuration={3000}
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

export default CollaborationDashboard;