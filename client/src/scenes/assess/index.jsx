import React, { useState, useEffect, useRef } from "react";
import {
  Box,
  Button,
  Typography,
  useTheme,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  TextField,
  Checkbox,
  FormControlLabel,
  Alert,
  Skeleton,
  Snackbar,
} from "@mui/material";
import { tokens } from "../../theme";
import { DataGrid, GridToolbar } from "@mui/x-data-grid";
import Header from "../../components/Header";
import { useAuth } from "../../context/authContext";
import axiosClient from "../../api/axiosClient";

const EvaluatePage = ({ }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [openSelectDialog, setOpenSelectDialog] = useState(false); // For SE selection dialog
  const [openEvaluateDialog, setOpenEvaluateDialog] = useState(false); // For evaluation dialog
  const dialogContentRef = useRef(null); // Ref for the dialog content
  const { user, isMentorView, toggleView } = useAuth();
  const [selectedSEs, setSelectedSEs] = useState([]); // Selected SEs for evaluation
  const [currentSEIndex, setCurrentSEIndex] = useState(0); // Index of the current SE being evaluated
  const [evaluations, setEvaluations] = useState({}); // Store evaluations for all SEs
  const [error, setError] = useState("");
  const [isLoadingSocialEnterprises, setIsLoadingSocialEnterprises] =
    useState(false);
  const [isLoadingEvaluations, setIsLoadingEvaluations] = useState(false);
  const [evaluationsData, setEvaluationsData] = useState([]);
  const [mentorevaluationsData, setmentorEvaluationsData] = useState([]);
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedEvaluation, setSelectedEvaluation] = useState(null);
  const [socialEnterprises, setSocialEnterprises] = useState([]);
  const [evaluationCriteria, setEvaluationCriteria] = useState({});
  const userSession = JSON.parse(localStorage.getItem("user"));
  const [openMentorshipDialog, setOpenMentorshipDialog] = useState(false); // For Mentorship Assessment dialog
  const [programs, setPrograms] = useState([]); // List of programs
  const [selectedPrograms, setSelectedPrograms] = useState([]); // Selected programs
  const [isLoadingPrograms, setIsLoadingPrograms] = useState(false);
  const [openMentorEvalDialog, setMentorEvalDialog] = useState(false);
  const [mentorEvaluations, setMentorEvaluations] = useState([]);
  const [lseedEvaluations, setLseedEvaluations] = useState([]);

  const handleProgramSelectionChange = (programId) => {
    setSelectedPrograms(
      (prev) =>
        prev.includes(programId)
          ? prev.filter((id) => id !== programId) // Deselect
          : [...prev, programId] // Select
    );
  };

  const [snackbarOpen, setSnackbarOpen] = useState(false);

  const handleSubmitEvaluations = async () => {
    if (!selectedPrograms.length) {
      console.error("❌ ERROR: No programs selected!");
      return;
    }

    try {
      console.log("📤 Submitting evaluations for programs:", selectedPrograms);

      await axiosClient.post(`/api/evaluate-mentor`, {
        programs: selectedPrograms, // Send selected program IDs
      });

      console.log("✅ Evaluation Submitted Successfully!");
    } catch (error) {
      console.error(
        "❌ Error submitting mentor evaluation:",
        error.response?.data || error.message
      );
    } finally {
      handleCloseMentorshipDialog();
      setSnackbarOpen(true);
    }
  };

  const handleOpenMentorshipDialog = () => {
    setOpenMentorshipDialog(true);
  };

  const handleCloseMentorshipDialog = () => {
    setOpenMentorshipDialog(false);
    setTimeout(() => {
      window.location.reload();
    }, 500); // Adjust delay if needed
    setSelectedPrograms([]); // Reset selected programs when closing
  };

  useEffect(() => {
    const fetchPrograms = async () => {
      try {
        setIsLoadingPrograms(true);
        const response = await axiosClient.get(`${process.env.REACT_APP_API_BASE_URL}/api/get-programs`);
        setPrograms(response.data); // Store fetched programs in state
      } catch (error) {
        console.error("❌ Error fetching programs:", error);
      } finally {
        setIsLoadingPrograms(false);
      }
    };

    if (openMentorshipDialog) {
      fetchPrograms();
    }
  }, [openMentorshipDialog]);

  useEffect(() => {
    const fetchPredefinedComments = async () => {
      try {
        const response = await axiosClient.get(
          `/api/get-predefined-comments`
        );
        setEvaluationCriteria(response.data); // Store fetched data in state
        console.log("📥 Predefined Comments Fetched:", response.data);
      } catch (error) {
        console.error("❌ Error fetching predefined comments:", error);
      }
    };

    fetchPredefinedComments();
  }, []);

  useEffect(() => {
    const fetchEvaluations = async () => {
      try {
        setIsLoadingEvaluations(true);

        const roles = user?.roles || [];
        const isMentor = roles.includes("Mentor");
        const isLSEEDUser = roles.some(role => role.startsWith("LSEED"));

        if (isMentor) {
          const mentorResponse = await axiosClient.get(`/api/get-mentor-evaluations`);

          console.log("Mentor Eval Data: ", mentorResponse)

          const formattedMentorData = (mentorResponse.data || []).map(evaluation => ({
            id: evaluation.evaluation_id,
            evaluation_id: evaluation.evaluation_id,
            evaluator_name: evaluation.evaluator_name,
            social_enterprise: evaluation.social_enterprise,
            evaluation_date: evaluation.evaluation_date,
            acknowledged: evaluation.acknowledged ? "Yes" : "No",
          }));

          setMentorEvaluations(formattedMentorData);
        }

        if (isLSEEDUser) {
      
        let lseedResponse;

        if(user?.roles.includes("LSEED-Coordinator")) {
          const res = await fetch(`${process.env.REACT_APP_API_BASE_URL}/api/get-program-coordinator`, {
            method: "GET",
            credentials: "include", // Required to send session cookie
          });

          if (!res.ok) {
            throw new Error("Failed to fetch program coordinator");
          }

          const data = await res.json();
          const program = data[0]?.name;

          if (!program) {
            throw new Error("No program found for this coordinator");
          }
          lseedResponse = await axiosClient.get(
            `${process.env.REACT_APP_API_BASE_URL}/api/get-all-evaluations?program=${program}`,
            { withCredentials: true }
          );
        } else {
            lseedResponse = await axiosClient.get(
              `${process.env.REACT_APP_API_BASE_URL}/api/get-all-evaluations`,
              { withCredentials: true }
            );
        }

          const formattedLseedData = (lseedResponse.data || []).map(evaluation => ({
            id: evaluation.evaluation_id,
            evaluation_id: evaluation.evaluation_id,
            evaluator_name: evaluation.evaluator_name,
            social_enterprise: evaluation.social_enterprise,
            evaluation_date: evaluation.evaluation_date,
            acknowledged: evaluation.acknowledged ? "Yes" : "No",
          }));

          setLseedEvaluations(formattedLseedData);
        }
      } catch (error) {
        console.error("❌ Error fetching evaluations:", error);
      } finally {
        setIsLoadingEvaluations(false);
      }
    };

    if (user?.roles) {
      fetchEvaluations();
    }
  }, [user]);

  const columns = [
    { 
      field: "social_enterprise", 
      headerName: "Social Enterprise", 
      flex: 1,
      minWidth: 150,
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            whiteSpace: "normal",
            wordBreak: "break-word",
          }}
        >
          {params.row.social_enterprise}
        </Typography>
      ),
    },
    { 
      field: "evaluator_name", 
      headerName: "Evaluator", 
      flex: 1,
      minWidth: 150,
      renderCell: (params) => (
        <Box
          sx={{
            width: "100%",
            alignItems: "center",
          }}
        >
          <Typography variant="body2">{params.row.evaluator_name}</Typography>
        </Box>
      ),
    },
    { 
      field: "acknowledged", 
      headerName: "Acknowledged", 
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
          <Typography variant="body2">{params.row.acknowledged}</Typography>
        </Box>
      ),
    },
    { 
      field: "evaluation_date", 
      headerName: "Evaluation Date", 
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
          <Typography variant="body2">{params.row.evaluation_date}</Typography>
        </Box>
      ),
    },
    {
      field: "action",
      headerName: "Action",
      flex: 1,
      minWidth: 150,
      renderCell: (params) => (
        <Button
          variant="contained"
          style={{ backgroundColor: colors.primary[600], color: "white" }}
          onClick={() => handleViewExistingEvaluation(params.row.evaluation_id)} // Pass only evaluation_id
        >
          View
        </Button>
      ),
    },
  ];

  useEffect(() => {
    const fetchmentorEvaluations = async () => {
      try {
        setIsLoadingEvaluations(true);

        const response = await axiosClient.get(
          `/api/get-all-mentor-evaluation-type`
        );

        const data = response.data; 

        if (!Array.isArray(data)) {
          console.error("❌ Unexpected API Response (Not an Array):", data);
          return;
        }

        // Ensure evaluation_id is included and set as `id`
        const formattedData = data.map((evaluation) => ({
          id: evaluation.evaluation_id, // ✅ Use evaluation_id as the unique ID
          mentor_name: evaluation.mentor_name,
          evaluator_name: evaluation.evaluator_name, // ✅ SE evaluating the mentor
          evaluation_date: evaluation.evaluation_date,
        }));

        setmentorEvaluationsData(formattedData);
      } catch (error) {
        console.error("❌ Error fetching evaluations:", error);
      } finally {
        setIsLoadingEvaluations(false);
      }
    };

    fetchmentorEvaluations();
  }, []);

  const mentorEvaluationColumns = [
    { field: "mentor_name", headerName: "Mentor", flex: 1, minWidth: 150, },
    { field: "evaluator_name", headerName: "Evaluator (SE)", flex: 1, minWidth: 150, },
    { field: "evaluation_date", headerName: "Evaluation Date", flex: 1, minWidth: 150, },
    {
      field: "action",
      headerName: "Action",
      flex: 1,
      minWidth: 150,
      renderCell: (params) => (
        <Button
          variant="contained"
          style={{ backgroundColor: colors.primary[600], color: "white" }}
          onClick={() => handleMentorViewExistingEvaluation(params.row.id)} // ✅ Pass evaluation_id
        >
          View
        </Button>
      ),
    },
  ];

  const handleMentorViewExistingEvaluation = async (evaluation_id) => {
    console.log("📌 Evaluation ID Passed:", evaluation_id); // Debugging log

    try {
      const response = await axiosClient.get(
        `/api/get-evaluation-details-for-mentor-evaluation`,
        { params: { evaluation_id } }
      );

      // 🚨 Ensure response.data is an array
      if (
        !response.data ||
        !Array.isArray(response.data) ||
        response.data.length === 0
      ) {
        console.warn("⚠️ No evaluation details found.");
        return;
      }

      // ✅ Process evaluation details safely
      const groupedEvaluation = response.data.reduce((acc, evalItem) => {
        const {
          evaluation_date,
          evaluator_name,
          mentor_name,
          category_name,
          star_rating,
          selected_comments,
          additional_comment,
        } = evalItem;

        if (!acc.id) {
          acc.id = evaluation_id;
          acc.evaluator_name = evaluator_name; // ✅ Social Enterprise (Evaluator)
          acc.mentor_name = mentor_name; // ✅ Mentor being evaluated
          acc.evaluation_date = evaluation_date;
          acc.categories = [];
        }

        acc.categories.push({
          category_name,
          star_rating,
          selected_comments: Array.isArray(selected_comments)
            ? selected_comments
            : [], // ✅ Ensure it's always an array
          additional_comment: additional_comment || "", // ✅ Ensure empty comments don't cause issues
        });

        return acc;
      }, {});

      console.log("✅ Processed Evaluation Data:", groupedEvaluation);
      setSelectedEvaluation(groupedEvaluation);
      setMentorEvalDialog(true);
    } catch (error) {
      console.error("❌ Error fetching evaluation details:", error);
    }
  };

  const handleViewExistingEvaluation = async (evaluation_id) => {
    try {
      const response = await axiosClient.get(
        `/api/get-evaluation-details`,
        {
          params: { evaluation_id },
        }
      );

      if (!response.data || response.data.length === 0) {
        console.warn("⚠️ No evaluation details found.");
        return;
      }

      // Process evaluation details
      const groupedEvaluation = response.data.reduce((acc, evalItem) => {
        const {
          evaluation_date,
          evaluator_name, // ✅ Added evaluator name
          social_enterprise,
          category_name,
          star_rating,
          selected_comments,
          additional_comment,
        } = evalItem;

        if (!acc.id) {
          acc.id = evaluation_id;
          acc.evaluator_name = evaluator_name; // ✅ Store evaluator (SE) name
          acc.social_enterprise = social_enterprise; // ✅ Store evaluated SE
          acc.evaluation_date = evaluation_date;
          acc.categories = [];
        }

        acc.categories.push({
          category_name,
          star_rating,
          selected_comments: Array.isArray(selected_comments)
            ? selected_comments
            : [], // Ensure selected_comments is always an array
          additional_comment,
        });

        return acc;
      }, {});

      console.log("✅ Processed Evaluation Data:", groupedEvaluation);
      setSelectedEvaluation(groupedEvaluation);
      setOpenDialog(true);
    } catch (error) {
      console.error("❌ Error fetching evaluation details:", error);
    }
  };

  const handleSESelectionChange = (seId) => {
    setSelectedSEs((prev) =>
      prev.includes(seId) ? prev.filter((id) => id !== seId) : [...prev, seId]
    );
  };

  // Open the SE selection dialog
  const handleOpenSelectDialog = () => {
    setOpenSelectDialog(true);
  };

  // Close the SE selection dialog
  const handleCloseSelectDialog = () => {
    setOpenSelectDialog(false);
    setTimeout(() => {
      window.location.reload();
    }, 500); // Adjust delay if needed
  };

  // Start evaluation after SE selection
  const handleStartEvaluation = () => {
    if (selectedSEs.length === 0) {
      setError("Please select at least one Social Enterprise to evaluate.");
      return;
    }
    setError("");
    setCurrentSEIndex(0);
    setOpenSelectDialog(false);
    setOpenEvaluateDialog(true);
  };

  const handleRatingChange = (category, value) => {
    const currentSEId = selectedSEs[currentSEIndex];

    const predefinedComments = evaluationCriteria[category]?.[value] || [
      "No predefined comment available.",
    ];

    console.log(`Rating: ${value}, Predefined Comments:`, predefinedComments);

    setEvaluations((prev) => ({
      ...prev,
      [currentSEId]: {
        ...prev[currentSEId],
        [category]: {
          rating: value,
          selectedCriteria:
            predefinedComments.length > 0 ? [predefinedComments[0]] : [], // ✅ Auto-select first comment
          predefinedComments,
        },
      },
    }));
  };

  // Handle additional comments change for the current SE
  const handleCommentsChange = (category, value) => {
    const currentSEId = selectedSEs[currentSEIndex];
    setEvaluations((prev) => ({
      ...prev,
      [currentSEId]: {
        ...prev[currentSEId],
        [category]: {
          ...prev[currentSEId]?.[category],
          comments: value,
        },
      },
    }));
  };
  
  useEffect(() => {
    if (user?.roles?.includes("Mentor")) {
      const fetchSocialEnterprises = async () => {
        try {
          setIsLoadingSocialEnterprises(true); // Start loading

          const mentorshipsResponse = await axiosClient.get(`/api/get-available-evaluations`);

          const updatedSocialEnterprises = mentorshipsResponse.data.map(
            (se) => ({
              id: se.mentoring_session_id, // Updated ID reference
              mentor_name: se.mentor_name || "No Mentor Assigned",
              team_name: se.social_enterprise_name || "Unknown Team",
              se_id: se.se_id || "Unknown Team",
              mentor_id: se.mentor_id || "Unknown Team",
              program_name: se.program_name || "Unknown Program",
              sdg_name: se.sdgs || "No SDG Assigned",
              start_time: se.start_time || "No Start Time",
              end_time: se.end_time || "No End Time",
              date: se.mentoring_session_date,
            })
          );

          setSocialEnterprises(updatedSocialEnterprises);
        } catch (error) {
          console.error("❌ Error fetching data:", error);
        } finally {
          setIsLoadingSocialEnterprises(false); // Stop loading
        }
      };
      fetchSocialEnterprises();
    }
  }, []);

  // Scroll to the top of the dialog when it opens
  useEffect(() => {
    if (openEvaluateDialog && dialogContentRef.current) {
      dialogContentRef.current.scrollTop = 0; // Scroll to the top
    }
  }, [openEvaluateDialog]);

  const handleSubmit = async () => {
    const currentSEId = selectedSEs[currentSEIndex];
    const currentEvaluations = evaluations[currentSEId];
    const getValidRating = (rating) =>
      rating && rating >= 1 && rating <= 5 ? rating : 1;

    const selectedSE = socialEnterprises.find((se) => se.id === currentSEId);
    if (!selectedSE) {
      console.error("❌ Selected SE not found.");
      alert("Error: Selected Social Enterprise not found.");
      return;
    }

    const mentoring_session_id = selectedSE.id;
    if (!mentoring_session_id) {
      console.error("❌ ERROR: mentorId is missing!");
      return;
    }

    const mentorId = selectedSE.mentor_id;
    if (!mentorId) {
      console.error("❌ ERROR: mentorId is missing!");
      return;
    }

    const se_id = selectedSE.se_id;
    if (!mentorId) {
      console.error("❌ ERROR: mentorId is missing!");
      return;
    }

    const isValid = Object.keys(evaluationCriteria).every((category) => {
      const currentSEId = selectedSEs[currentSEIndex];
      const categoryEval = evaluations[currentSEId]?.[category];

      return (
        categoryEval &&
        categoryEval.rating > 0 &&
        categoryEval.selectedCriteria &&
        categoryEval.selectedCriteria.length > 0
      );
    });

    if (!isValid) {
      setError(
        "Please provide a rating and select at least one predefined comment for each category."
      );
      return;
    }

    const formData = {
      se_id: se_id,
      mentorId: mentorId,
      evaluations: currentEvaluations,
      mentoring_session_id: mentoring_session_id,
    };

    Object.keys(currentEvaluations).forEach((category) => {
      formData[`${category}_rating`] = getValidRating(
        currentEvaluations[category]?.rating
      );
      formData[`${category}_selectedcriteria`] =
        currentEvaluations[category]?.selectedCriteria || [];
      formData[`${category}_addtlcmt`] =
        currentEvaluations[category]?.comments || "";
    });

    console.log("📤 Sending Evaluation to Backend:", formData);

    try {
      await axiosClient.post(`/api/evaluate`, formData);

      if (currentSEIndex < selectedSEs.length - 1) {
        setCurrentSEIndex((prevIndex) => prevIndex + 1); // Move to the next SE
        setTimeout(() => {
          if (dialogContentRef.current) {
            dialogContentRef.current.scrollTop = 0; // Scroll to top after transition
          }
        }, 100);
      } else {
        console.log("✅ All SEs have been evaluated.");
        handleCloseEvaluateDialog();
        setTimeout(() => {
          window.scrollTo({ top: 0, behavior: "smooth" }); // Ensure full reset to the top
        }, 200);
      }
    } catch (error) {
      console.error("❌ Error submitting evaluations:", error);
    }
  };

  const isSubmitDisabled = () => {
    const currentSEId = selectedSEs[currentSEIndex];
    const currentEvaluations = evaluations[currentSEId] || {};

    // Ensure every category meets the conditions
    const allCategoriesValid = Object.keys(evaluationCriteria).every(
      (category) => {
        const categoryEval = currentEvaluations[category] || {};
        return categoryEval.rating > 0;
      }
    );

    return !allCategoriesValid; // Disable if any category is invalid
  };

  // Close the evaluation dialog
  const handleCloseEvaluateDialog = () => {
    setOpenEvaluateDialog(false);
    setSelectedSEs([]);
    setCurrentSEIndex(0);
    setEvaluations({});
    setTimeout(() => {
      window.location.reload();
    }, 500); // Adjust delay if needed
  };

  {
    isLoadingSocialEnterprises ? (
      <Box sx={{ padding: "16px" }}>
        {/* Placeholder for Buttons */}
        <Box sx={{ display: "flex", gap: "16px", marginBottom: "16px" }}>
          <Skeleton variant="rectangular" width={120} height={40} />
          <Skeleton variant="rectangular" width={120} height={40} />
        </Box>

        {/* Placeholder for DataGrid Rows */}
        {[1, 2, 3, 4, 5].map((rowIndex) => (
          <Box
            key={rowIndex}
            sx={{
              display: "flex",
              gap: "16px",
              marginBottom: "8px",
            }}
          >
            <Skeleton variant="rectangular" width={200} height={40} />{" "}
            {/* Social Enterprise */}
            <Skeleton variant="rectangular" width={150} height={40} />{" "}
            {/* Assigned Mentor */}
            <Skeleton variant="rectangular" width={150} height={40} />{" "}
            {/* Program Name */}
            <Skeleton variant="rectangular" width={100} height={40} />{" "}
            {/* SDG(s) */}
          </Box>
        ))}
      </Box>
    ) : socialEnterprises.length === 0 ? (
      <Box sx={{ padding: "16px", textAlign: "center" }}>
        <Typography variant="body1" color="white">
          No records found.
        </Typography>
      </Box>
    ) : (
      <DataGrid
        rows={evaluationsData}
        columns={columns}
        getRowId={(row) => row.evaluation_id} // Ensure evaluation_id is used as ID
        getRowHeight={() => 'auto'}
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
    );
  }

  return (
    <Box m="20px">
      <Header
        title={user?.roles.some(role => role.startsWith("LSEED"))  ? "Evaluate Mentors" : "Evaluate SE"}
        subtitle={
          user?.roles.some(role => role.startsWith("LSEED"))
            ? "View and Manage mentor evaluations"
            : "Evaluate Social Enterprises based on key criteria"
        }
      />

      <Box display="flex" flexDirection="column" alignItems="center" gap={4}>
        {/* Buttons */}
        <Box
          width="100%"
          bgcolor={colors.primary[400]}
          display="flex"
          padding={2}
          gap={2} // Adds spacing between buttons
        >
          {/* Show this button only if userRole is Mentor */}
          {(isMentorView && user?.roles.includes("Mentor")) && (
            <Button
              variant="contained"
              color="secondary"
              disabled={socialEnterprises.length === 0}
              sx={{
                fontSize: "16px", // Adjust font size for better fit
                py: "10px", // Reduces height
                flexGrow: 1, // Makes it take full width
                minWidth: 0, // Ensures responsiveness
              }}
              onClick={handleOpenSelectDialog}
            >
              Evaluate SE
            </Button>
          )}
          {/* Show this button only if userRole is LSEED */}
          {(!isMentorView && user?.roles.some(r => r.startsWith("LSEED"))) && (
            <Button
              onClick={handleOpenMentorshipDialog}
              variant="contained"
              color="secondary"
              sx={{
                fontSize: "16px",
                py: "10px",
                flexGrow: 1,
                minWidth: 0,
              }}
            >
              Mentorship Assessment
            </Button>
          )}
        </Box>

        {/* Mentorship Assessment Dialog */}
        <Dialog
          open={openMentorshipDialog}
          onClose={handleCloseMentorshipDialog}
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
          <DialogTitle
            sx={{
              backgroundColor: "#1E4D2B", // DLSU Green header
              color: "#fff", // White text
              textAlign: "center",
              fontSize: "1.5rem",
              fontWeight: "bold",
            }}
          >
            Select Programs for Evaluation
          </DialogTitle>
          <DialogContent
            sx={{
              display: "flex",
              flexDirection: "column", // Ensure vertical stacking
              gap: "8px", // Add spacing between items
            }}
          >
            {isLoadingPrograms ? (
              <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
                {[1, 2, 3].map((index) => (
                  <Skeleton key={index} variant="rectangular" height={40} />
                ))}
              </Box>
            ) : programs.length === 0 ? (
              <Typography>No programs found.</Typography>
            ) : (
              programs.map((program) => (
                <FormControlLabel
                  key={program.id}
                  control={
                    <Checkbox
                      checked={selectedPrograms.includes(program.id)}
                      onChange={() =>
                        handleProgramSelectionChange(
                          program.id,
                          program.mentors
                        )
                      }
                      sx={{
                        color: "#000",
                        "&.Mui-checked": { color: "#000" },
                      }}
                    />
                  }
                  label={
                    <span>
                      <strong>{program.name}</strong>
                      <br />
                      <span style={{ fontSize: "0.9em", color: "#666" }}>
                        Evaluation for:{" "}
                        {program.mentors
                          .map((mentor) => mentor.name)
                          .join(", ")}
                      </span>
                    </span>
                  }
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    marginBottom: "8px",
                  }}
                />
              ))
            )}
            {error && <Alert severity="error">{error}</Alert>}
          </DialogContent>
          <DialogActions
            sx={{
              padding: "16px",
              borderTop: "1px solid #000", // Separator line
            }}
          >
            <Button
              onClick={handleCloseMentorshipDialog}
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
              onClick={handleSubmitEvaluations}
              variant="contained"
              disabled={selectedPrograms.length === 0}
              sx={{
                backgroundColor:
                  selectedPrograms.length > 0 ? "#1E4D2B" : "#A0A0A0", // Change color if disabled
                color: "#fff",
                "&:hover": {
                  backgroundColor:
                    selectedPrograms.length > 0 ? "#145A32" : "#A0A0A0",
                },
              }}
            >
              Send Evaluations
            </Button>
          </DialogActions>
        </Dialog>

        <Snackbar
          open={snackbarOpen}
          autoHideDuration={3000} // ✅ Disappears after 3 seconds
          onClose={() => setSnackbarOpen(false)}
          anchorOrigin={{ vertical: "top", horizontal: "center" }}
        >
          <Alert
            onClose={() => setSnackbarOpen(false)}
            severity="success"
            sx={{ width: "100%" }}
          >
            Successfully Submitted!
          </Alert>
        </Snackbar>

        {/* Show DataGrid only if userRole is Mentor */}
        {(isMentorView && user?.roles.includes("Mentor")) && (
          <Box
            width="100%"
            backgroundColor={colors.primary[400]}
            padding="20px"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
              marginBottom="15px" // Ensures a small gap between header & DataGrid
            >
              My Evaluations
            </Typography>
            <Box
              width="100%"
              height="400px"
              minHeight="400px" // Ensures it does not shrink with missing data
              sx={{
                "& .MuiDataGrid-cell": {
                  display: "flex",
                  alignItems: "center",            // vertical centering
                  paddingTop: "12px",
                  paddingBottom: "12px",
                  borderBottom: "none",            // remove bottom border
                },
                "& .MuiDataGrid-columnHeader": {
                  alignItems: "center",            // center header label vertically
                  backgroundColor: colors.blueAccent[700] + " !important",
                },
                "& .MuiDataGrid-cellContent": {
                  whiteSpace: "normal",            // allow line wrap
                  wordBreak: "break-word",
                },
                "& .MuiDataGrid-scrollbarFiller, & .MuiDataGrid-scrollbarFiller--header": {
                  backgroundColor: colors.blueAccent[700] + " !important",
                },
                "& .MuiDataGrid-root": {
                  border: "none",
                },
                "& .name-column--cell": {
                  color: colors.greenAccent[300],
                },
                "& .MuiDataGrid-columnHeaders": {
                  backgroundColor: colors.blueAccent[700] + " !important",
                },
                "& .MuiDataGrid-virtualScroller": {
                  backgroundColor: colors.primary[400],
                },
                "& .MuiDataGrid-footerContainer": {
                  borderTop: "none",
                  backgroundColor: colors.blueAccent[700],
                },
                "& .MuiDataGrid-toolbarContainer .MuiButton-text": {
                  color: `${colors.grey[100]} !important`,
                },
              }}
            >
              <DataGrid
                rows={mentorEvaluations}
                columns={columns}
                getRowId={(row) => row.id}
                slots={{ toolbar: GridToolbar }}
              />
            </Box>
          </Box>
        )}
        {/* Show DataGrid only if userRole is LSEED */}
        {(!isMentorView && user?.roles.some(r => r.startsWith("LSEED"))) && (
          <Box
            width="100%"
            backgroundColor={colors.primary[400]}
            padding="20px"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
              marginBottom="15px" // Ensures a small gap between header & DataGrid
            >
              Evaluations by Mentors
            </Typography>
            <Box
              width="100%"
              height="400px"
              minHeight="400px" // Ensures it does not shrink with missing data
              sx={{
                "& .MuiDataGrid-scrollbarFiller, & .MuiDataGrid-scrollbarFiller--header":
                  {
                    backgroundColor: colors.blueAccent[700] + " !important",
                  },
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
                rows={lseedEvaluations}
                columns={columns}
                getRowId={(row) => row.id}
                getRowHeight={() => 'auto'}
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
                    whiteSpace: "normal", // allow line wrap
                    wordBreak: "break-word",
                  },
                  "& .MuiDataGrid-toolbarContainer .MuiButton-text": {
                  color: `${colors.grey[100]} !important`,
                  },
                }}
                slots={{ toolbar: GridToolbar }}
              />
            </Box>
          </Box>
        )}

        {(!isMentorView && user?.roles.some(r => r.startsWith("LSEED"))) && (
          <Box
            width="100%"
            backgroundColor={colors.primary[400]}
            padding="20px"
          >
            <Typography
              variant="h3"
              fontWeight="bold"
              color={colors.greenAccent[500]}
              marginBottom="15px" // Ensures a small gap between header & DataGrid
            >
              Evaluations by Social Enterprises
            </Typography>
            <Box
              width="100%"
              height="400px"
              minHeight="400px" // Ensures it does not shrink with missing data
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
                },
              }}
            >
              <DataGrid
                rows={mentorevaluationsData}
                columns={mentorEvaluationColumns}
                getRowId={(row) => row.id}
                getRowHeight={() => 'auto'}
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
        )}

        {/* SE Selection Dialog */}
        <Dialog
          open={openSelectDialog}
          onClose={handleCloseSelectDialog}
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
          <DialogTitle
            sx={{
              backgroundColor: "#1E4D2B", // DLSU Green header
              color: "#fff", // White text
              textAlign: "center",
              fontSize: "1.5rem",
              fontWeight: "bold",
            }}
          >
            Select Social Enterprises for Evaluation
          </DialogTitle>
          <DialogContent>
            {socialEnterprises.map((se) => (
              <FormControlLabel
                key={se.id} // Ensure this matches the property in state
                control={
                  <Checkbox
                    checked={selectedSEs.includes(se.id)} // Use consistent ID reference
                    onChange={() => handleSESelectionChange(se.id)} // Pass correct ID
                    sx={{
                      color: "#000",
                      "&.Mui-checked": { color: "#000" },
                    }}
                  />
                }
                label={
                  <span>
                    <strong>
                      {se.team_name} [SDG: {se.sdg_name}]
                    </strong>
                    <br />
                    <span style={{ fontSize: "0.9em", color: "#666" }}>
                      Mentoring Session on {se.date}, {se.start_time} -{" "}
                      {se.end_time}
                    </span>
                  </span>
                }
                sx={{ marginBottom: "8px" }}
              />
            ))}
            {error && <Alert severity="error">{error}</Alert>}
          </DialogContent>

          <DialogActions
            sx={{
              padding: "16px",
              borderTop: "1px solid #000", // Separator line
            }}
          >
            <Button
              onClick={handleCloseSelectDialog}
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
              onClick={handleStartEvaluation}
              variant="contained"
              disabled={selectedSEs.length === 0} // 🔥 Disable if no SE is selected
              sx={{
                backgroundColor: selectedSEs.length > 0 ? "#1E4D2B" : "#A0A0A0", // Change color if disabled
                color: "#fff",
                "&:hover": {
                  backgroundColor:
                    selectedSEs.length > 0 ? "#145A32" : "#A0A0A0",
                },
              }}
            >
              Start Evaluation
            </Button>
          </DialogActions>
        </Dialog>
        {/* Evaluation Dialog */}
        <Dialog
          open={openEvaluateDialog}
          onClose={handleCloseEvaluateDialog}
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
          {/* Top Portion with DLSU Green Background */}
          <DialogTitle
            sx={{
              backgroundColor: "#1E4D2B", // DLSU Green header
              color: "#fff", // White text
              textAlign: "center",
              fontSize: "1.5rem",
              fontWeight: "bold",
            }}
          >
            Evaluate Social Enterprise
          </DialogTitle>

          {/* Content Section */}
          <DialogContent
            ref={dialogContentRef} // Ref for scrolling to top
            sx={{
              padding: "24px",
              maxHeight: "70vh", // Ensure it doesn't overflow the screen
              overflowY: "auto", // Enable scrolling if content is too long
            }}
          >
            {/* Current SE Name with Mentoring Session Details */}
            <Typography
              variant="h6"
              sx={{
                marginBottom: "16px",
                fontWeight: "bold",
                borderBottom: "1px solid #000", // Separator line
                paddingBottom: "8px",
              }}
            >
              Evaluating
              {socialEnterprises.find(
                (se) => se.id === selectedSEs[currentSEIndex] // Match session ID
              ) && (
                <>
                  <strong>
                    {" "}
                    {
                      socialEnterprises.find(
                        (se) => se.id === selectedSEs[currentSEIndex]
                      )?.team_name
                    }
                  </strong>{" "}
                  [SDG:{" "}
                  {
                    socialEnterprises.find(
                      (se) => se.id === selectedSEs[currentSEIndex]
                    )?.sdg_name
                  }
                  ]
                  <br />
                  <span style={{ fontSize: "0.9em", color: "#666" }}>
                    Mentoring Session on{" "}
                    {
                      socialEnterprises.find(
                        (se) => se.id === selectedSEs[currentSEIndex]
                      )?.date
                    }
                    ,{" "}
                    {
                      socialEnterprises.find(
                        (se) => se.id === selectedSEs[currentSEIndex]
                      )?.start_time
                    }{" "}
                    -{" "}
                    {
                      socialEnterprises.find(
                        (se) => se.id === selectedSEs[currentSEIndex]
                      )?.end_time
                    }
                  </span>
                </>
              )}
            </Typography>
            <Typography
              variant="h6"
              sx={{ textAlign: "center", marginBottom: "16px" }}
            >
              Evaluating {currentSEIndex + 1} / {selectedSEs.length}
            </Typography>

            {/* Evaluation Categories */}
            {Object.keys(evaluationCriteria).map((category) => {
              const currentSEId = selectedSEs[currentSEIndex];
              const categoryEval = evaluations[currentSEId]?.[category] || {
                rating: 0,
                selectedCriteria: [],
                comments: "",
              };

              return (
                <Box
                  key={category}
                  sx={{
                    marginBottom: "24px",
                    padding: "16px",
                    border: "1px solid #000", // Border for each category
                  }}
                >
                  {/* Category Title */}
                  <Typography
                    variant="subtitle1"
                    sx={{
                      fontWeight: "bold",
                      marginBottom: "8px",
                    }}
                  >
                    {category.charAt(0).toUpperCase() + category.slice(1)}
                  </Typography>

                  {/* Star Rating Selection */}
                  <Box display="flex" gap={1} justifyContent="center" mt={1}>
                    {[1, 2, 3, 4, 5].map((value) => (
                      <Box
                        key={value}
                        width="40px"
                        height="40px"
                        border="1px solid black"
                        display="flex"
                        justifyContent="center"
                        alignItems="center"
                        bgcolor={
                          value <= categoryEval.rating
                            ? "#FFEE8C"
                            : "transparent"
                        }
                        sx={{ cursor: "pointer" }}
                        onClick={() => handleRatingChange(category, value)}
                      >
                        <Typography fontSize="24px">★</Typography>
                      </Box>
                    ))}
                  </Box>

                  {/* Predefined Evaluation Criteria (Only If Rating > 0) */}
                  {categoryEval.rating > 0 && (
                    <Box
                      sx={{
                        maxHeight: "150px",
                        overflowY: "auto",
                        mt: 2,
                        p: 1,
                        border: "1px solid #ccc",
                        borderRadius: "5px",
                        display: "flex",
                        flexDirection: "column",
                        gap: 1,
                        backgroundColor: "#f9f9f9", // Light background for contrast
                      }}
                    >
                      {categoryEval.predefinedComments.length > 0 ? (
                        <Typography
                          variant="body1"
                          sx={{
                            fontStyle: "italic",
                            color: "#333",
                            fontWeight: "bold",
                          }}
                        >
                          {categoryEval.predefinedComments[0]}{" "}
                          {/* Always show the first comment */}
                        </Typography>
                      ) : (
                        <Typography variant="body2" color="textSecondary">
                          No predefined comment available.
                        </Typography>
                      )}
                    </Box>
                  )}

                  {/* Additional Comment Field */}
                  <TextField
                    label="Additional Comments"
                    value={categoryEval.comments}
                    onChange={(e) =>
                      handleCommentsChange(category, e.target.value)
                    }
                    variant="outlined"
                    fullWidth
                    multiline
                    rows={3}
                    sx={{
                      marginTop: "8px",
                      "& .MuiOutlinedInput-root": {
                        border: "1px solid #000", // Apply border only to input field
                        borderRadius: "4px", // Rounded corners
                        "&:hover": {
                          borderColor: "#000",
                        },
                        "&.Mui-focused": {
                          borderColor: "#000",
                        },
                      },
                      "& .MuiInputBase-root": {
                        padding: "8px",
                      },
                      "& .MuiInputBase-input": {
                        color: "#000",
                        lineHeight: "1.5",
                        textDecoration: "none",
                      },
                      "& .MuiInputLabel-root": {
                        color: "#000",
                        backgroundColor: "#fff", // Add background to prevent line through label
                        padding: "0 4px", // Small padding to keep it readable
                      },
                      "& .MuiInputLabel-root.Mui-focused": {
                        color: "#000",
                      },
                    }}
                  />
                </Box>
              );
            })}
          </DialogContent>

          {/* Error Message */}
          {error && (
            <Alert severity="error" sx={{ margin: "16px" }}>
              {error}
            </Alert>
          )}

          {/* Action Buttons */}
          <DialogActions
            sx={{
              padding: "16px",
              borderTop: "1px solid #000", // Separator line
            }}
          >
            <Button
              onClick={handleCloseEvaluateDialog}
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
              onClick={() => {
                handleSubmit(); // ✅ Call the submit function
                handleCloseEvaluateDialog(); // ✅ Close the dialog after submission
                setSnackbarOpen(true);
              }}
              variant="contained"
              disabled={isSubmitDisabled()}
              sx={{
                backgroundColor: isSubmitDisabled() ? "#ccc" : "#1E4D2B",
                color: "#fff",
                "&:hover": {
                  backgroundColor: isSubmitDisabled() ? "#ccc" : "#145A32",
                },
              }}
            >
              Submit
            </Button>
          </DialogActions>
        </Dialog>

        {/* Evaluation Details Dialog - Read-Only */}
        <Dialog
          open={openDialog}
          onClose={() => setOpenDialog(false)}
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
          {/* Title with DLSU Green Background */}
          <DialogTitle
            sx={{
              backgroundColor: "#1E4D2B", // DLSU Green header
              color: "#fff", // White text
              textAlign: "center",
              fontSize: "1.5rem",
              fontWeight: "bold",
            }}
          >
            View Evaluation
          </DialogTitle>

          {/* Content Section */}
          <DialogContent
            sx={{
              padding: "24px",
              maxHeight: "70vh", // Ensure it doesn't overflow the screen
              overflowY: "auto", // Enable scrolling if content is too long
            }}
          >
            {selectedEvaluation ? (
              <>
                {/* Evaluator, Social Enterprise, and Evaluation Date */}
                <Box
                  sx={{
                    marginBottom: "16px",
                    display: "flex",
                    flexDirection: "column",
                    gap: "8px",
                  }}
                >
                  <Typography
                    variant="h6"
                    sx={{
                      fontWeight: "bold",
                      borderBottom: "1px solid #000", // Separator line
                      paddingBottom: "8px",
                    }}
                  >
                    Evaluator: {selectedEvaluation.evaluator_name}{" "}
                    {/* ✅ Added Evaluator Name */}
                  </Typography>
                  <Typography
                    variant="h6"
                    sx={{
                      fontWeight: "bold",
                      borderBottom: "1px solid #000", // Separator line
                      paddingBottom: "8px",
                    }}
                  >
                    Social Enterprise Evaluated:{" "}
                    {selectedEvaluation.social_enterprise}
                  </Typography>
                  <Typography variant="subtitle1" sx={{ color: "#000" }}>
                    Evaluation Date: {selectedEvaluation.evaluation_date}
                  </Typography>
                </Box>

                {/* Categories Section */}
                {selectedEvaluation.categories &&
                selectedEvaluation.categories.length > 0 ? (
                  selectedEvaluation.categories.map((category, index) => (
                    <Box
                      key={index}
                      sx={{
                        marginBottom: "24px",
                        padding: "16px",
                        border: "1px solid #000", // Border for each category
                        borderRadius: "8px",
                      }}
                    >
                      {/* Category Name and Rating */}
                      <Typography
                        variant="subtitle1"
                        sx={{
                          fontWeight: "bold",
                          marginBottom: "8px",
                        }}
                      >
                        {category.category_name} - Rating:{" "}
                        {category.star_rating} ★
                      </Typography>

                      {/* Selected Comments */}
                      <Typography variant="body1" sx={{ marginBottom: "8px" }}>
                        Comments:{" "}
                        {category.selected_comments.length > 0 ? (
                          category.selected_comments.join(", ")
                        ) : (
                          <i>No comments</i>
                        )}
                      </Typography>

                      {/* Additional Comment */}
                      <Typography variant="body1">
                        Additional Comment:{" "}
                        {category.additional_comment || (
                          <i>No additional comments</i>
                        )}
                      </Typography>
                    </Box>
                  ))
                ) : (
                  <Typography variant="body1" sx={{ fontStyle: "italic" }}>
                    No categories found for this evaluation.
                  </Typography>
                )}
              </>
            ) : (
              <Typography variant="body1" sx={{ fontStyle: "italic" }}>
                Loading evaluation details...
              </Typography>
            )}
          </DialogContent>

          {/* Action Buttons */}
          <DialogActions sx={{ padding: "16px", borderTop: "1px solid #000" }}>
            <Button
              onClick={() => setOpenDialog(false)}
              sx={{
                color: "#000",
                border: "1px solid #000",
                "&:hover": { backgroundColor: "#f0f0f0" }, // Hover effect
              }}
            >
              Close
            </Button>
          </DialogActions>
        </Dialog>

        {/* Mentor Evaluation Details Dialog - Read-Only */}
        <Dialog
          open={openMentorEvalDialog}
          onClose={() => setMentorEvalDialog(false)}
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
          {/* Title with DLSU Green Background */}
          <DialogTitle
            sx={{
              backgroundColor: "#1E4D2B", // DLSU Green header
              color: "#fff", // White text
              textAlign: "center",
              fontSize: "1.5rem",
              fontWeight: "bold",
            }}
          >
            View Mentor Evaluation
          </DialogTitle>

          {/* Content Section */}
          <DialogContent
            sx={{
              padding: "24px",
              maxHeight: "70vh", // Ensure it doesn't overflow the screen
              overflowY: "auto", // Enable scrolling if content is too long
            }}
          >
            {selectedEvaluation ? (
              <>
                {/* Evaluator (Social Enterprise) and Evaluation Date */}
                <Box
                  sx={{
                    marginBottom: "16px",
                    display: "flex",
                    flexDirection: "column",
                    gap: "8px",
                  }}
                >
                  <Typography
                    variant="h6"
                    sx={{
                      fontWeight: "bold",
                      borderBottom: "1px solid #000", // Separator line
                      paddingBottom: "8px",
                    }}
                  >
                    Evaluator (Social Enterprise):{" "}
                    {selectedEvaluation.evaluator_name}
                  </Typography>
                  <Typography
                    variant="h6"
                    sx={{
                      fontWeight: "bold",
                      borderBottom: "1px solid #000", // Separator line
                      paddingBottom: "8px",
                    }}
                  >
                    Mentor Evaluated: {selectedEvaluation.mentor_name}
                  </Typography>
                  <Typography variant="subtitle1" sx={{ color: "#000" }}>
                    Evaluation Date: {selectedEvaluation.evaluation_date}
                  </Typography>
                </Box>

                {/* Ratings Section */}
                {selectedEvaluation.categories &&
                selectedEvaluation.categories.length > 0 ? (
                  selectedEvaluation.categories.map((category, index) => (
                    <Box
                      key={index}
                      sx={{
                        marginBottom: "16px",
                        padding: "12px",
                        border: "1px solid #000", // Border for each category
                        borderRadius: "8px",
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                      }}
                    >
                      {/* Category Name */}
                      <Typography
                        variant="subtitle1"
                        sx={{ fontWeight: "bold" }}
                      >
                        {category.category_name}
                      </Typography>

                      {/* Star Rating */}
                      <Typography
                        variant="h6"
                        sx={{ fontWeight: "bold", color: "#1E4D2B" }} // DLSU Green color for rating
                      >
                        {category.star_rating} ★
                      </Typography>
                    </Box>
                  ))
                ) : (
                  <Typography variant="body1" sx={{ fontStyle: "italic" }}>
                    No categories found for this evaluation.
                  </Typography>
                )}
              </>
            ) : (
              <Typography variant="body1" sx={{ fontStyle: "italic" }}>
                Loading evaluation details...
              </Typography>
            )}
          </DialogContent>

          {/* Action Buttons */}
          <DialogActions sx={{ padding: "16px", borderTop: "1px solid #000" }}>
            <Button
              onClick={() => setMentorEvalDialog(false)}
              sx={{
                color: "#000",
                border: "1px solid #000",
                "&:hover": { backgroundColor: "#f0f0f0" }, // Hover effect
              }}
            >
              Close
            </Button>
          </DialogActions>
        </Dialog>
      </Box>
    </Box>
  );
};

export default EvaluatePage;