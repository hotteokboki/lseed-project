import React, { useState } from "react";
import {
  Box,
  Button,
  Typography,
  TextField,
  Alert,
  Snackbar,
  useTheme,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  DialogActions
} from "@mui/material";
import { tokens } from "../../theme";
import Header from "../../components/Header";
import { Link } from "react-router-dom";
import axiosClient from "../../api/axiosClient";

const ForgotPassword = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  const [email, setEmail] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [sqOpen, setSqOpen] = useState(false);
  const [questions, setQuestions] = useState([]);   // [{ position, question }]
  const [answers, setAnswers] = useState({});       // { [position]: "user answer" }
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setMessage("");
    setError("");
    setSubmitting(true);

    try {
      const { data } = await axiosClient.post("/auth/forgot-password", { email });

      if (data.requiresSecurityQuestions) {
        setQuestions(data.questions || []); // [{position, question}]
        setAnswers({});
        setSqOpen(true);
      } else {
        setMessage(data.message || "If that email exists, a reset link was sent.");
        setSnackbarOpen(true);
      }
    } catch (err) {
      setError(err?.response?.data?.message || "Something went wrong.");
      setSnackbarOpen(true);
    } finally {
      setSubmitting(false);
    }
  };

  const handleSubmitSq = async () => {
    setSubmitting(true);
    setMessage("");
    setError("");

    try {
      const payload = {
        email,
        answers: questions.map(q => ({
          position: q.position,
          answer: (answers[q.position] || "").trim(),
        })),
      };

      const { data } = await axiosClient.post("/auth/forgot-password", payload);

      setSqOpen(false);
      setMessage(data.message || "Reset link sent. Please check your email.");
      setSnackbarOpen(true);
    } catch (err) {
      setError(err?.response?.data?.message || "Answers did not match. Please try again.");
      setSnackbarOpen(true);
    } finally {
      setSubmitting(false);
    }
  };

  const handleCloseSnackbar = () => {
    setSnackbarOpen(false);
  };

  return (
    <Box m="20px">
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Header title="Forgot Password" subtitle="Reset your account access" />
      </Box>

      <Box
        display="flex"
        flexDirection="column"
        alignItems="center"
        gap={4}
        mt={4}
      >
        <Box
          width="50%"
          bgcolor="white"
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
          padding={4}
          gap={2}
        >
          <Box bgcolor="#1E4D2B" p={2} width="100%" textAlign="center">
            <Typography
              variant="h6" // Bigger than body1
              color="#fff"
              fontWeight="bold"
            >
              Enter your registered email address. We'll send you a link to
              reset your password.
            </Typography>
          </Box>

          <form onSubmit={handleSubmit} style={{ width: "100%" }}>
            <TextField
              type="email"
              label="Email Address"
              fullWidth
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              InputProps={{ style: { color: "#000" } }}
              InputLabelProps={{ style: { color: "#000" } }}
              sx={{
                mb: 2,
                "& .MuiOutlinedInput-root": {
                  "& fieldset": { borderColor: "#000" },
                  "&:hover fieldset": { borderColor: "#000" },
                  "&.Mui-focused fieldset": { borderColor: "#000" },
                },
              }}
            />

            <Button
              type="submit"
              variant="contained"
              fullWidth
              disabled={submitting}
              startIcon={submitting ? <CircularProgress size={18} /> : null}
              sx={{
                backgroundColor: "#1E4D2B",
                color: "#fff",
                fontWeight: "bold",
                "&:hover": { backgroundColor: "#145A32" },
              }}
            >
              {submitting ? "Please wait..." : "Send Reset Link"}
            </Button>
          </form>

          <Typography mt={2}>
            <Link
              to="/"
              style={{ color: "#1E4D2B", textDecoration: "underline" }}
            >
              Go back to Home
            </Link>
          </Typography>
        </Box>
      </Box>

      <Snackbar
        open={snackbarOpen}
        autoHideDuration={4000}
        onClose={handleCloseSnackbar}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        {message ? (
          <Alert
            severity="success"
            onClose={handleCloseSnackbar}
            sx={{ width: "100%" }}
          >
            {message}
          </Alert>
        ) : error ? (
          <Alert
            severity="error"
            onClose={handleCloseSnackbar}
            sx={{ width: "100%" }}
          >
            {error}
          </Alert>
        ) : null}
      </Snackbar>

      <Dialog open={sqOpen} onClose={() => setSqOpen(false)} className="custom-dialog">
        <DialogTitle className="custom-dialog-title">Security Verification</DialogTitle>
        <DialogContent className="custom-dialog-content">
          <Typography variant="body1" sx={{ color: "#000", mb: 2 }}>
            Please answer your security questions to continue.
          </Typography>

          {questions.map((q) => (
            <TextField
              key={q.position}
              fullWidth
              label={q.question}
              value={answers[q.position] || ""}
              onChange={(e) =>
                setAnswers(prev => ({ ...prev, [q.position]: e.target.value }))
              }
              margin="dense"
              InputProps={{ style: { color: "#000" } }}
              InputLabelProps={{ style: { color: "#000" } }}
              sx={{
                "& .MuiOutlinedInput-root": {
                  "& fieldset": { borderColor: "#000" },
                  "&:hover fieldset": { borderColor: "#000" },
                  "&.Mui-focused fieldset": { borderColor: "#000" },
                },
              }}
            />
          ))}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setSqOpen(false)} disabled={submitting}>Cancel</Button>
          <Button onClick={handleSubmitSq} variant="contained" disabled={submitting}>
            {submitting ? "Submitting..." : "Submit"}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default ForgotPassword;
