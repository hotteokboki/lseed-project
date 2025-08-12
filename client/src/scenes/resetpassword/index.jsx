import React, { useState, useEffect } from "react";
import { useSearchParams, Link, useNavigate } from "react-router-dom";
import axios from "axios";
import {
  Box,
  Button,
  Typography,
  TextField,
  Alert,
  Snackbar,
  useTheme,
} from "@mui/material";
import { tokens } from "../../theme";
import Header from "../../components/Header";

// ---- same helpers you used on the profile page ----
const getPasswordChecklist = (password) => {
  const hasUpper = /[A-Z]/.test(password);
  const hasLower = /[a-z]/.test(password);
  const hasNum = /[0-9]/.test(password);
  const hasSpec = /[\W_]/.test(password);
  return {
    length: password.length >= 8,
    uppercase: hasUpper,
    lowercase: hasLower,
    number: hasNum,
    specialChar: hasSpec,
  };
};

const getPasswordStrength = (password) => {
  const c = getPasswordChecklist(password);
  const met = [c.uppercase, c.lowercase, c.number, c.specialChar].filter(Boolean).length;
  if (c.length && met === 4) return "Strong";
  if (password.length >= 8 && met >= 3) return "Moderate";
  return "Weak";
};

const PasswordReset = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const navigate = useNavigate();

  const [searchParams] = useSearchParams();
  const token = searchParams.get("token");

  // token gate state
  const [checkingToken, setCheckingToken] = useState(true);
  const [isValidToken, setIsValidToken] = useState(false);

  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");

  // strength + checklist + confirm error
  const [pwdStrength, setPwdStrength] = useState("Weak");
  const [pwdChecklist, setPwdChecklist] = useState(getPasswordChecklist(""));
  const [confirmError, setConfirmError] = useState("");

  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  // validate the token on mount/refresh
  useEffect(() => {
    const validate = async () => {
      if (!token) {
        setIsValidToken(false);
        setCheckingToken(false);
        return;
      }
      try {
        await axios.get(`${process.env.REACT_APP_API_BASE_URL}/auth/reset-password/validate`, {
          params: { token },
        });
        setIsValidToken(true);
      } catch (e) {
        setIsValidToken(false);
      } finally {
        setCheckingToken(false);
      }
    };
    validate();
  }, [token]);

  const checklistOK =
    pwdChecklist.length &&
    pwdChecklist.uppercase &&
    pwdChecklist.lowercase &&
    pwdChecklist.number &&
    pwdChecklist.specialChar;

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!checklistOK) {
      setError("Password must be at least 8 characters and include uppercase, lowercase, a number, and a special character.");
      setMessage("");
      setSnackbarOpen(true);
      return;
    }

    const strengthNow = getPasswordStrength(password);
    if (strengthNow !== "Strong") {
      setError(`Password strength is ${strengthNow}. Please make it stronger.`);
      setMessage("");
      setSnackbarOpen(true);
      return;
    }

    if (password !== confirmPassword) {
      setError("Passwords do not match");
      setMessage("");
      setSnackbarOpen(true);
      return;
    }

    try {
      setSubmitting(true);
      const res = await axios.post(
        `${process.env.REACT_APP_API_BASE_URL}/auth/reset-password`,
        { token, newPassword: password }
      );

      // backend should invalidate (delete) the token here so refresh will fail validation
      setMessage(res.data.message || "Password reset successful.");
      setError("");
      setSnackbarOpen(true);

      // Clear fields and immediately remove token from URL by navigating away
      setPassword("");
      setConfirmPassword("");
      setPwdStrength("Weak");
      setPwdChecklist(getPasswordChecklist(""));
      setConfirmError("");

      // Option A: go to login (recommended)
      setTimeout(() => navigate("/", { replace: true }), 1200);

      // Option B (alternative): show an Access Denied view on this page after success
      // setIsValidToken(false);
    } catch (err) {
      setError(err.response?.data?.message || "Something went wrong");
      setMessage("");
      setSnackbarOpen(true);
    } finally {
      setSubmitting(false);
    }
  };

  const handleCloseSnackbar = () => setSnackbarOpen(false);

  // Loading / invalid states (block the page on refresh if token is used/expired)
  if (checkingToken) {
    return <Box m="20px"><Typography>Validating reset link...</Typography></Box>;
  }

  if (!isValidToken) {
    return (
      <Box m="20px" textAlign="center">
        <Header title="Reset Password" subtitle="Link invalid or expired" />
        <Box mt={4}>
          <Typography variant="h6" sx={{ mb: 2 }}>
            This reset link is invalid, expired, or already used.
          </Typography>
          <Button variant="contained" onClick={() => navigate("/", { replace: true })}>
            Go to Home
          </Button>
        </Box>
      </Box>
    );
  }

  return (
    <Box m="20px">
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Header title="Reset Password" subtitle="Choose a new password" />
      </Box>

      <Box display="flex" flexDirection="column" alignItems="center" gap={4} mt={4}>
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
            <Typography variant="h6" color="#fff" fontWeight="bold">
              Please enter your new password below
            </Typography>
          </Box>

          <form onSubmit={handleSubmit} style={{ width: "100%" }}>
            <TextField
              type="password"
              label="New Password"
              fullWidth
              required
              value={password}
              onChange={(e) => {
                const val = e.target.value;
                setPassword(val);
                setPwdChecklist(getPasswordChecklist(val));
                setPwdStrength(getPasswordStrength(val));
                if (confirmPassword) {
                  setConfirmError(val !== confirmPassword ? "Passwords do not match" : "");
                }
              }}
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

            <TextField
              type="password"
              label="Confirm New Password"
              fullWidth
              required
              value={confirmPassword}
              onChange={(e) => {
                const val = e.target.value;
                setConfirmPassword(val);
                setConfirmError(val !== password ? "Passwords do not match" : "");
              }}
              error={Boolean(confirmError)}
              helperText={confirmError || " "}
              InputProps={{ style: { color: "#000" } }}
              InputLabelProps={{ style: { color: "#000" } }}
              sx={{
                mb: 1,
                "& .MuiOutlinedInput-root": {
                  "& fieldset": { borderColor: "#000" },
                  "&:hover fieldset": { borderColor: "#000" },
                  "&.Mui-focused fieldset": { borderColor: "#000" },
                },
              }}
            />

            {password && (
              <Box mb={2}>
                <Typography
                  variant="body2"
                  sx={{
                    fontWeight: 600,
                    color:
                      pwdStrength === "Strong"
                        ? "#2e7d32"
                        : pwdStrength === "Moderate"
                        ? "#1976d2"
                        : "#d32f2f",
                  }}
                >
                  Strength: {pwdStrength}
                </Typography>

                <Box
                  mt={0.5}
                  sx={{
                    display: "grid",
                    gridTemplateColumns: "repeat(2, minmax(180px, 1fr))",
                    gap: 1,
                  }}
                >
                  {[
                    { key: "length", label: "At least 8 characters" },
                    { key: "uppercase", label: "Contains uppercase letter" },
                    { key: "lowercase", label: "Contains lowercase letter" },
                    { key: "number", label: "Contains a number" },
                    { key: "specialChar", label: "Contains special character" },
                  ].map((item) => (
                    <Typography
                      key={item.key}
                      variant="caption"
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 1,
                        color: pwdChecklist[item.key] ? "#2e7d32" : "#555",
                      }}
                    >
                      <span style={{ fontWeight: 700 }}>
                        {pwdChecklist[item.key] ? "✓" : "•"}
                      </span>
                      {item.label}
                    </Typography>
                  ))}
                </Box>
              </Box>
            )}

            <Button
              type="submit"
              variant="contained"
              fullWidth
              disabled={
                !password ||
                !confirmPassword ||
                !!confirmError ||
                !checklistOK ||
                getPasswordStrength(password) !== "Strong" ||
                submitting
              }
              sx={{
                backgroundColor: "#1E4D2B",
                color: "#fff",
                fontWeight: "bold",
                "&:hover": { backgroundColor: "#145A32" },
              }}
            >
              {submitting ? "Please wait..." : "Reset Password"}
            </Button>
          </form>

          <Typography mt={2}>
            <Link to="/" style={{ color: "#1E4D2B", textDecoration: "underline" }}>
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
          <Alert severity="success" onClose={handleCloseSnackbar} sx={{ width: "100%" }}>
            {message}
          </Alert>
        ) : error ? (
          <Alert severity="error" onClose={handleCloseSnackbar} sx={{ width: "100%" }}>
            {error}
          </Alert>
        ) : null}
      </Snackbar>
    </Box>
  );
};

export default PasswordReset;