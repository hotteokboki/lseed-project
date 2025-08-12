import React, { useState, useRef, useEffect } from "react";
import { Link } from "react-router-dom";
import "../../styles/Login.css";
import { useAuth } from "../../context/authContext";
import {
  Button,
  Typography,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Checkbox,
  MenuItem,
  Select,
  InputLabel,
  FormControl,
  Alert,
  Snackbar,
  OutlinedInput,
  ListItemText,
  Box,
  Chip,
} from "@mui/material";
import axiosClient from "../../api/axiosClient";

const Login = () => {
  const { login } = useAuth();
  const [isFlipped, setIsFlipped] = useState(false);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [passwordStrength, setPasswordStrength] = useState("");
  const [otpOpen, setOtpOpen] = useState(false);
  const [otp, setOtp] = useState("");
  const [otpError, setOtpError] = useState("");
  const [verifyingOtp, setVerifyingOtp] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [passwordChecklist, setPasswordChecklist] = useState({
    length: false,
    uppercase: false,
    number: false,
    specialChar: false,
  });
  const emptyFormData = {
    // General Sign-Up Info
    firstName: "",
    lastName: "",
    email: "",
    password: "",
    confirmPassword: "",
    contactno: "",

    // Mentor-Specific Info
    affiliation: "",
    motivation: "",
    expertise: "",
    businessAreas: [],
    preferredTime: [],
    specificTime: "",
    communicationMode: [],
  };
  const [formData, setFormData] = useState(emptyFormData);
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState("");
  const [snackbarSeverity, setSnackbarSeverity] = useState("success"); // or "error", "info", etc.
  const [hasSubmitted, setHasSubmitted] = useState(false);
  const [openDialog, setOpenDialog] = useState(false);
  const [retrySeconds, setRetrySeconds] = useState(null);
  const [lockoutTime, setLockoutTime] = useState(0);
  const retryTimerRef = useRef(null);
  const [menuOpenBusiness, setMenuOpenBusiness] = useState(false);
  const [menuOpenPreferredTime, setMenuOpenPreferredTime] = useState(false);
  const [menuOpenCommunicationModes, setMenuOpenCommunicationModes] = useState(false);
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

  const handleDonePreferredTime = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setMenuOpenPreferredTime(false);
  };

  const startRetryCountdown = (seconds) => {
    setRetrySeconds(seconds);

    if (retryTimerRef.current) {
      clearInterval(retryTimerRef.current);
    }

    retryTimerRef.current = setInterval(() => {
      setRetrySeconds((prev) => {
        if (prev <= 1) {
          clearInterval(retryTimerRef.current);
          retryTimerRef.current = null;
          setErrorMessage("");
          return null;
        }
        return prev - 1;
      });
    }, 1000);
  };

  useEffect(() => {
    return () => {
      if (retryTimerRef.current) {
        clearInterval(retryTimerRef.current);
      }
    };
  }, []);

  const handleDoneCommunicationModes = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setMenuOpenCommunicationModes(false);
  };

  const handleFlip = () => {
    setIsFlipped(!isFlipped);
  };

  const handleLogin = async (e) => {
    e.preventDefault();
    setErrorMessage("");

    try {
      const response = await fetch(`${process.env.REACT_APP_API_BASE_URL}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ email, password }),
      });

      let data = {};
      try { data = await response.json(); } catch { }

      if (response.ok) {
        if (data.require2fa) {
          // show OTP modal
          setOtp("");
          setOtpError("");
          setOtpOpen(true);
          return;
        }
        // normal (no 2FA)
        login(data.user);
        window.location.href = data.redirect || "/dashboard";
      } else {
        if (response.status === 429 && data.retryAfter) {
          setErrorMessage(data.message || "Too many login attempts.");
          startRetryCountdown(data.retryAfter);
        } else {
          setErrorMessage(data.message || `Login failed. Status: ${response.status}`);
        }
      }
    } catch (error) {
      setErrorMessage("A network error occurred. Please try again.");
    }
  };

  const handleVerifyOtp = async () => {
    setOtpError("");
    if (otp.length !== 6) {
      setOtpError("Enter the 6-digit code.");
      return;
    }

    try {
      setVerifyingOtp(true);

      // If your axiosClient doesn't already set withCredentials, add it in the third arg.
      const { data } = await axiosClient.post(
        "/login/2fa-verify",
        { code: otp } // payload
        // , { withCredentials: true }
      );

      if (data?.ok) {
        if (data.user) login(data.user);
        setOtpOpen(false);
        window.location.href = data.redirect || "/dashboard";
      } else {
        setOtpError(data?.message || "Invalid code. Please try again.");
      }
    } catch (e) {
      setOtpError(e?.response?.data?.message || "Could not verify code. Please try again.");
    } finally {
      setVerifyingOtp(false);
    }
  };

  const handleCancelOtp = () => {
    setOtp("");
    setOtpError("");
    setOtpOpen(false);
  };

  const handleInputChange = (e) => {
    const { name, value, type, checked } = e.target;

    const updatedForm = {
      ...formData,
      [name]: type === "checkbox" ? checked : value,
    };

    setFormData(updatedForm);

    // If user starts fixing the confirm password, and it now matches, clear the red border
    if (name === "confirmPassword" && hasSubmitted) {
      setHasSubmitted(false); // Remove red border and helper text
    }

    if (name === "password") {
      setPasswordStrength(getPasswordStrength(value));
      setPasswordChecklist(getPasswordChecklist(value));
    }
  };

  const getPasswordStrength = (password) => {
    if (password.length <= 7) return "Weak";
    if (
      /[A-Z]/.test(password) &&
      /[0-9]/.test(password) &&
      /[\W_]/.test(password)
    )
      return "Strong";
    return "Moderate";
  };

  const getPasswordChecklist = (password) => ({
    length: password.length >= 8,
    uppercase: /[A-Z]/.test(password),
    number: /[0-9]/.test(password),
    specialChar: /[\W_]/.test(password),
  });

  const handleSubmit = async (e) => {
    e.preventDefault();
    setHasSubmitted(true); // Mark that the user tried submitting

    // ✅ Check for weak password
    if (
      !passwordChecklist.length ||
      !passwordChecklist.uppercase ||
      !passwordChecklist.number ||
      !passwordChecklist.specialChar
    ) {
      setSnackbarMessage(
        "Password is too weak. Please follow the required rules."
      );
      setSnackbarSeverity("error");
      setSnackbarOpen(true);
      return;
    }

    // Validate password match
    if (formData.password !== formData.confirmPassword) {
      setSnackbarMessage(
        "Passwords do not match. Please correct them before submitting."
      );
      setSnackbarSeverity("error");
      setSnackbarOpen(true);
      return;
    }

    console.log("Success");

    try {
      const response = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/signup`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(formData),
        }
      );

      const data = await response.json();
      if (response.ok) {
        setSnackbarMessage(
          "Signup successful! Check email on application status"
        );
        setSnackbarSeverity("success");
        setSnackbarOpen(true);
        setIsFlipped(false);

        setFormData(emptyFormData);
      } else {
        setErrorMessage(data.message || "Signup failed");
      }
    } catch (error) {
      setErrorMessage("An error occurred. Please try again.");
    }
  };

  return (
    <div className="container">
      <input
        type="checkbox"
        id="flip"
        checked={isFlipped}
        onChange={handleFlip}
        style={{ display: "none" }}
      />
      <div className="cover">
        <div className="front">
          <img src="frontphot.jpg" alt="Welcome" />
          <div className="text">
            <span className="text-1">
              <h4>WELCOME TO</h4>
              <h2>LSEED Insight</h2>
            </span>
            <span className="text-2">Let's get started</span>
          </div>
        </div>
        <div className="back">
          <img src="backphot.png" alt="Join Us" />
          <div className="text">
            <span className="text-1">
              Want to become part of the <br /> Team?
            </span>
          </div>
        </div>
      </div>

      <div className="forms">
        <div className="form-content">
          {!isFlipped ? (
            <div className="login-form">
              <div className="title">
                <h2>LOGIN</h2>
              </div>
              <form onSubmit={handleLogin}>
                <div className="input-boxes">
                  <div className="input-box">
                    <i className="fas fa-envelope"></i>
                    <input
                      type="email"
                      name="email"
                      placeholder="Enter your email"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      required
                    />
                  </div>
                  <div className="input-box">
                    <i className="fas fa-lock"></i>

                    <input
                      type={showPassword ? "text" : "password"}
                      name="password"
                      placeholder="Enter your password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      required
                    />

                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="show-password-button"
                    >
                      <i className={`fas ${showPassword ? 'fa-eye-slash' : 'fa-eye'}`}></i>
                    </button>
                  </div>

                  <div className="text">
                    <Link to="/forgot-password">Forgot password?</Link>
                  </div>
                  <div className="button input-box">
                    <input type="submit" value="Log-In" />
                  </div>
                  {errorMessage && (
                    <div className="error-message">{errorMessage}</div>
                  )}

                  {retrySeconds !== null && (
                    <div className="retry-message">
                      Please wait {retrySeconds} second{retrySeconds !== 1 && 's'} before trying again.
                    </div>
                  )}
                  <div className="separator">OR</div>
                  <div className="text sign-up-text">
                    Don't have an account?{" "}
                    <label htmlFor="flip">Sign up now</label>
                  </div>
                </div>
              </form>
            </div>
          ) : (
            <div className="signup-form">
              <div className="signup-scroll">
                <div className="title">
                  <h2>SIGN UP</h2>
                </div>
                <form onSubmit={handleSubmit}>
                  <div className="input-boxes">
                    {[
                      { label: "First Name", name: "firstName", type: "text" },
                      { label: "Last Name", name: "lastName", type: "text" },
                      { label: "Email", name: "email", type: "email" },
                      { label: "Contact No.", name: "contactno", type: "text" },
                      { label: "Password", name: "password", type: "password" },
                    ].map(({ label, name, type }) => (
                      <TextField
                        key={name}
                        label={label}
                        name={name}
                        type={type}
                        fullWidth
                        required
                        value={formData[name]}
                        onChange={handleInputChange}
                        InputProps={{ style: { color: "#000" } }}
                        InputLabelProps={{ style: { color: "#000" } }}
                        sx={{
                          mt: 2,
                          "& .MuiOutlinedInput-root": {
                            "& fieldset": { borderColor: "#000" },
                            "&:hover fieldset": { borderColor: "#000" },
                            "&.Mui-focused fieldset": { borderColor: "#000" },
                          },
                        }}
                      />
                    ))}
                    {formData.password && (
                      <Box sx={{ mt: 1, ml: 1 }}>
                        <Typography
                          variant="body2"
                          sx={{ color: "#000", mb: 0.5 }}
                        >
                          Your password must contain:
                        </Typography>
                        <ul style={{ paddingLeft: "20px", marginTop: 0 }}>
                          <li
                            style={{
                              color: passwordChecklist.length ? "green" : "red",
                            }}
                          >
                            At least 8 characters
                          </li>
                          <li
                            style={{
                              color: passwordChecklist.uppercase
                                ? "green"
                                : "red",
                            }}
                          >
                            At least one uppercase letter
                          </li>
                          <li
                            style={{
                              color: passwordChecklist.number ? "green" : "red",
                            }}
                          >
                            At least one number
                          </li>
                          <li
                            style={{
                              color: passwordChecklist.specialChar
                                ? "green"
                                : "red",
                            }}
                          >
                            At least one special character (!@#$%^&*)
                          </li>
                        </ul>
                      </Box>
                    )}
                    <TextField
                      label="Confirm Password"
                      name="confirmPassword"
                      type="password"
                      fullWidth
                      required
                      value={formData.confirmPassword}
                      onChange={handleInputChange}
                      error={
                        hasSubmitted &&
                        formData.password !== formData.confirmPassword
                      }
                      helperText={
                        hasSubmitted &&
                          formData.password !== formData.confirmPassword
                          ? "Passwords do not match"
                          : ""
                      }
                      InputProps={{ style: { color: "#000" } }}
                      InputLabelProps={{ style: { color: "#000" } }}
                      sx={{
                        mt: 2,
                        "& .MuiOutlinedInput-root": {
                          "& fieldset": { borderColor: "#000" },
                          "&:hover fieldset": { borderColor: "#000" },
                          "&.Mui-focused fieldset": { borderColor: "#000" },
                        },
                      }}
                    />
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
                      { label: "Areas of Expertise", key: "expertise", multiline: true, },
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
                        onChange={(e) => handleInputChange(e, field.key)}
                        InputProps={{ style: { color: "#000" } }}
                        InputLabelProps={{ style: { color: "#000" } }}
                        sx={{
                          mt: 2,
                          "& .MuiOutlinedInput-root": {
                            "& fieldset": { borderColor: "#000" },
                            "&:hover fieldset": { borderColor: "#000" },
                            "&.Mui-focused fieldset": { borderColor: "#000" },
                          },
                        }}
                      />
                    ))}

                    {/* Business Areas */}
                    <FormControl fullWidth sx={{ mt: 2 }}>
                      <InputLabel>Business Areas</InputLabel>
                      <Select
                        required
                        multiple
                        value={formData.businessAreas}
                        onChange={(e) =>
                          setFormData((prev) => ({
                            ...prev,
                            businessAreas: e.target.value.filter(Boolean),
                          }))
                        }
                        input={
                          <OutlinedInput
                            label="Business Areas"
                            sx={{
                              // these make the input grow to fit chips
                              padding: '8px 12px',
                              minHeight: 80,
                              height: 'auto',
                              alignItems: 'flex-start',
                            }}
                          />
                        }
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
                                sx={{
                                  color: "#000",          // Force text inside chip
                                  backgroundColor: "#e0e0e0", // Optional: softer background for contrast
                                }}
                              />
                            ))}
                          </Box>
                        )}
                        open={menuOpenBusiness}
                        onOpen={() => setMenuOpenBusiness(true)}
                        onClose={() => setMenuOpenBusiness(false)}
                        sx={{
                          "& .MuiOutlinedInput-notchedOutline": {
                            borderColor: "#000",
                          },
                          "& .MuiSvgIcon-root": {
                            color: "#000",                           // Force dropdown arrow black
                          },
                        }}
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

                    {/* Preferred Time Selection */}
                    <FormControl fullWidth sx={{ mt: 2 }}>
                      <InputLabel sx={{ color: "#000" }}>Preferred Time</InputLabel>
                      <Select
                        required
                        multiple
                        value={formData.preferredTime}
                        onChange={(e) =>
                          setFormData((prev) => ({
                            ...prev,
                            preferredTime: e.target.value.filter(Boolean),
                          }))
                        }
                        input={
                          <OutlinedInput
                            label="Preferred Time"
                            sx={{
                              padding: '8px 12px',
                              minHeight: 80,
                              height: 'auto',
                              alignItems: 'flex-start',
                              color: "#000",                          // Force input text black
                              "& .MuiInputBase-input": {
                                color: "#000",                        // Ensure inner input text is black
                              },
                            }}
                          />
                        }
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
                                sx={{
                                  color: "#000",                      // Force chip label black
                                  backgroundColor: "#e0e0e0",         // Soft gray chip background
                                }}
                              />
                            ))}
                          </Box>
                        )}
                        open={menuOpenPreferredTime}
                        onOpen={() => setMenuOpenPreferredTime(true)}
                        onClose={() => setMenuOpenPreferredTime(false)}
                        sx={{
                          "& .MuiOutlinedInput-notchedOutline": {
                            borderColor: "#000",                     // Force border black
                          },
                          "& .MuiInputLabel-root": {
                            color: "#000",                           // Force label black
                          },
                          "& .MuiSvgIcon-root": {
                            color: "#000",                           // Force dropdown arrow black
                          },
                        }}
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
                        InputProps={{ style: { color: "#000" } }}
                        InputLabelProps={{ style: { color: "#000" } }}
                        sx={{
                          mt: 2,
                          "& .MuiOutlinedInput-root": {
                            "& fieldset": { borderColor: "#000" },
                            "&:hover fieldset": { borderColor: "#000" },
                            "&.Mui-focused fieldset": { borderColor: "#000" },
                          },
                        }}
                      />
                    )}

                    {/* Communication Modes */}
                    <FormControl fullWidth sx={{ mt: 2 }}>
                      <InputLabel sx={{ color: "#000" }}>Communication Modes</InputLabel>
                      <Select
                        multiple
                        value={formData.communicationMode}
                        onChange={(e) =>
                          setFormData((prev) => ({
                            ...prev,
                            communicationMode: e.target.value.filter(Boolean),
                          }))
                        }
                        required
                        input={
                          <OutlinedInput
                            label="Communication Modes"
                            sx={{
                              padding: '8px 12px',
                              minHeight: 80,
                              height: 'auto',
                              alignItems: 'flex-start',
                              color: "#000",                          // Force text color black
                              "& .MuiInputBase-input": {
                                color: "#000",                        // Ensure inner input text is black
                              },
                            }}
                          />
                        }
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
                                sx={{
                                  color: "#000",                      // Force chip text black
                                  backgroundColor: "#e0e0e0",         // Soft gray background
                                }}
                              />
                            ))}
                          </Box>
                        )}
                        open={menuOpenCommunicationModes}
                        onOpen={() => setMenuOpenCommunicationModes(true)}
                        onClose={() => setMenuOpenCommunicationModes(false)}
                        sx={{
                          "& .MuiOutlinedInput-notchedOutline": {
                            borderColor: "#000",                     // Force border black
                          },
                          "& .MuiInputLabel-root": {
                            color: "#000",                           // Force label black
                          },
                          "& .MuiSvgIcon-root": {
                            color: "#000",                           // Force dropdown arrow black
                          },
                        }}
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

                    {/* Terms and Submit */}
                    <div
                      className="checkbox-wrapper terms-checkbox"
                      style={{ marginTop: "30px" }}
                    >
                      <input type="checkbox" id="terms" name="terms" required />
                      <label
                        htmlFor="terms"
                        onClick={() => setOpenDialog(true)}
                      >
                        Terms and Conditions
                      </label>
                    </div>

                    <div className="button input-box">
                      <input type="submit" value="Register" />
                    </div>

                    {errorMessage && (
                      <div className="error-message">{errorMessage}</div>
                    )}

                    <div className="separator">OR</div>
                    <div
                      className="text sign-up-text"
                      style={{ marginBottom: "40px" }}
                    >
                      Already have an account?{" "}
                      <label htmlFor="flip">Login now</label>
                    </div>
                  </div>
                </form>
              </div>
            </div>
          )}
        </div>
      </div>

      <Dialog open={otpOpen} onClose={handleCancelOtp} className="custom-dialog">
        <DialogTitle className="custom-dialog-title">
          Two-Factor Authentication
        </DialogTitle>

        <DialogContent className="custom-dialog-content">
          <Typography variant="body1" sx={{ mb: 1, color: "#000" }}>
            Enter the 6-digit code from your authenticator app.
          </Typography>

          <TextField
            autoFocus
            fullWidth
            label="6-digit code"
            value={otp}
            onChange={(e) =>
              setOtp(e.target.value.replace(/\D/g, "").slice(0, 6))
            }
            inputProps={{ inputMode: "numeric", pattern: "[0-9]*", maxLength: 6 }}
            error={!!otpError}
            helperText={otpError || " "}
            InputProps={{ style: { color: "#000" } }}
            InputLabelProps={{ style: { color: "#000" } }}
            sx={{
              mt: 2,
              "& .MuiOutlinedInput-root": {
                "& fieldset": { borderColor: "#000" },
                "&:hover fieldset": { borderColor: "#000" },
                "&.Mui-focused fieldset": { borderColor: "#000" },
              },
            }}
          />
        </DialogContent>

        <DialogActions>
          <Button onClick={handleCancelOtp} disabled={verifyingOtp}>
            Cancel
          </Button>
          <Button
            onClick={handleVerifyOtp}
            disabled={verifyingOtp || otp.length !== 6}
            variant="contained"
            color="primary"
          >
            {verifyingOtp ? "Verifying..." : "Verify"}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={openDialog}
        onClose={() => setOpenDialog(false)}
        className="custom-dialog"
      >
        <DialogTitle className="custom-dialog-title">
          Terms and Conditions
        </DialogTitle>
        <DialogContent className="custom-dialog-content">
          <Box sx={{ color: "#000", textAlign: "justify" }}>
            <Typography variant="body1" paragraph>
              <strong>Good day, Volunteer Mentors!</strong>
            </Typography>

            <Typography variant="body1" paragraph>
              Thank you once again for your interest in joining our panel of
              mentors for <strong>LSEED Mentoring</strong>. We truly appreciate
              it!
            </Typography>

            <Typography variant="body1" paragraph>
              For an overview, LSEED Mentoring is a three-phase online coaching
              & mentoring initiative of the{" "}
              <strong>
                Lasallian Social Enterprise for Economic Development (LSEED)
                Center
              </strong>
              , for Lasallian social entrepreneurs and partners. It also serves
              as a strategy to help Lasallian social enterprises develop new
              mechanisms to adapt to the ever-changing landscape of social
              entrepreneurship in the country.
            </Typography>

            <Typography variant="body1" paragraph>
              In order to properly coordinate mentoring session schedules, we
              would like to inquire about your availability this Academic Year.
              Your response to this survey will serve as available options for
              our students/mentees when selecting mentoring session schedules.
            </Typography>

            <Typography variant="body1" paragraph>
              For questions and/or clarifications, you may get in touch with us
              through email:{" "}
              <a href="mailto:lseed@dlsu.edu.ph">lseed@dlsu.edu.ph</a> or{" "}
              <a href="mailto:norby.salonga@dlsu.edu.ph">
                norby.salonga@dlsu.edu.ph
              </a>
              .
            </Typography>

            <Typography variant="body1" paragraph>
              <strong>Privacy and Confidentiality Note:</strong> All collected
              information through this form will only be used for LSEED Online
              Mentoring.
            </Typography>

            <Typography variant="body1" paragraph>
              By filling out this form, I understand that I have the
              responsibility as a volunteer mentor to keep all information
              (shared and entrusted to me) with utmost
              confidentiality—specifically the SE ideas and information of
              students/social entrepreneurs participating in LSEED Mentoring.
            </Typography>

            <Typography variant="body1">
              <strong>Do you agree?</strong>
            </Typography>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Accept</Button>
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
    </div>
  );
};

export default Login;
