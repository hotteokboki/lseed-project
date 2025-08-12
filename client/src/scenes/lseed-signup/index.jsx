import React, { useState, useEffect } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import "../../styles/Login.css";
import {
    TextField,
    Button,
    Typography,
    Box,
    Snackbar,
    Alert,
} from "@mui/material";
import Header from "../../components/Header";
import axiosClient from "../../api/axiosClient";

const LSEEDSignup = () => {
    const [isFlipped, setIsFlipped] = useState(true);
    const [searchParams] = useSearchParams();
    const navigate = useNavigate();

    const [isValidToken, setIsValidToken] = useState(false);
    const [checkingToken, setCheckingToken] = useState(true);

    const [hasSubmitted, setHasSubmitted] = useState(false);
    const [passwordChecklist, setPasswordChecklist] = useState({
        length: false,
        uppercase: false,
        number: false,
        specialChar: false,
    });
    const [snackbar, setSnackbar] = useState({
        open: false,
        message: "",
        severity: "info",
    });

    const [formData, setFormData] = useState({
        firstName: "",
        lastName: "",
        email: "",
        password: "",
        confirmPassword: "",
        contactno: "",
    });

    const token = searchParams.get("token");

    useEffect(() => {
        if (!token) {
            setSnackbar({
                open: true,
                message: "Invalid or missing invite token.",
                severity: "error",
            });
            setCheckingToken(false);
            return;
        }

        const validateToken = async () => {
            try {
                await axiosClient.get(`/api/validate-invite-token`, {
                    params: { token },
                });
                // If request succeeds, token is valid
                setIsValidToken(true);
            } catch (error) {
                // If request fails, Axios throws here
                setSnackbar({
                    open: true,
                    message:
                        error?.response?.status === 400 || error?.response?.status === 404
                            ? "Invalid or expired invite token."
                            : "Server error. Please try again.",
                    severity: "error",
                });
            } finally {
                setCheckingToken(false);
            }
        };

        validateToken();
    }, [token]);

    // Block signup without a valid token
    if (checkingToken) {
        return <div>Validating invitation...</div>;
    }

    if (!isValidToken) {
        return (
            <Box m="20px" textAlign="center">
                <Header title="Signup Page Expired" subtitle="Link invalid or expired" />
                <Box mt={4}>
                    <Typography variant="h6" sx={{ mb: 2 }}>
                        This signup link is invalid, expired, or already used.
                    </Typography>
                    <Button variant="contained" onClick={() => navigate("/", { replace: true })}>
                        Go to Home
                    </Button>
                </Box>
            </Box>
        );
    }
    const handleFlip = () => {
        setIsFlipped(!isFlipped);
    };

    const getPasswordChecklist = (password) => ({
        length: password.length >= 8,
        uppercase: /[A-Z]/.test(password),
        number: /[0-9]/.test(password),
        specialChar: /[\W_]/.test(password),
    });

    const handleGoToLogin = () => {
        window.location.href = `${process.env.REACT_APP_API_FRONTEND_URL}/`;
    };

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData((prev) => ({
            ...prev,
            [name]: value,
        }));

        // If user starts fixing the confirm password, and it now matches, clear the red border
        if (name === "confirmPassword" && hasSubmitted) {
            setHasSubmitted(false); // Remove red border and helper text
        }

        if (name === "password") {
            setPasswordChecklist(getPasswordChecklist(value));
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setHasSubmitted(true);

        // ✅ Check for weak password
        if (
            !passwordChecklist.length ||
            !passwordChecklist.uppercase ||
            !passwordChecklist.number ||
            !passwordChecklist.specialChar
        ) {
            setSnackbar({
                open: true,
                message: "Password is too weak. Please follow the required rules.",
                severity: "error",
            });
            return;
        }

        // Check for password match
        if (formData.password !== formData.confirmPassword) {
            setSnackbar({
                open: true,
                message: "Passwords do not match.",
                severity: "error",
            });
            return;
        }

        try {
            const res = await fetch(`${process.env.REACT_APP_API_BASE_URL}/signup-lseed-role`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ ...formData, token }),
            });

            const data = await res.json();

            if (res.ok) {
                setSnackbar({
                    open: true,
                    message: "Account created successfully!",
                    severity: "success",
                });

                // Reset form
                setFormData({
                    firstName: "",
                    lastName: "",
                    email: "",
                    password: "",
                    confirmPassword: "",
                });

                setIsFlipped(false);
            } else {
                setSnackbar({
                    open: true,
                    message: data.message || "Signup failed.",
                    severity: "error",
                });
            }
        } catch (error) {
            setSnackbar({
                open: true,
                message: "Server error. Please try again.",
                severity: "error",
            });
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
                            LSEED <br /> Sign-Up
                        </span>
                    </div>
                </div>
            </div>

            <div className="forms">
                <div className="form-content">
                    {isFlipped ? (
                        <div className="signup-form">
                            <div className="signup-scroll">
                                <div className="title">
                                    <h2>LSEED SIGN UP</h2>
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
                                                <Typography variant="body2" sx={{ color: "#000", mb: 0.5 }}>
                                                    Your password must contain:
                                                </Typography>
                                                <ul style={{ paddingLeft: "20px", marginTop: 0 }}>
                                                    <li style={{ color: passwordChecklist.length ? "green" : "red" }}>
                                                        At least 8 characters
                                                    </li>
                                                    <li style={{ color: passwordChecklist.uppercase ? "green" : "red" }}>
                                                        At least one uppercase letter
                                                    </li>
                                                    <li style={{ color: passwordChecklist.number ? "green" : "red" }}>
                                                        At least one number
                                                    </li>
                                                    <li style={{ color: passwordChecklist.specialChar ? "green" : "red" }}>
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
                                            error={hasSubmitted && formData.password !== formData.confirmPassword}
                                            helperText={
                                                hasSubmitted && formData.password !== formData.confirmPassword
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

                                        <div className="button input-box" style={{ marginTop: "30px" }}>
                                            <input type="submit" value="Register" />
                                        </div>
                                    </div>
                                </form>
                            </div>
                        </div>
                    ) : (
                        <div className="login-form">
                            <div className="title">
                                <h2>THANK YOU!</h2>
                            </div>
                            <div className="text sign-up-text" style={{ textAlign: "center" }}>
                                Your account has been created successfully. Please proceed to Login.
                            </div>
                            <div className="button input-box" style={{ marginTop: "30px" }}>
                                <input type="button" value="Go to Login" onClick={handleGoToLogin} />
                            </div>
                        </div>
                    )}
                </div>
            </div>

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
        </div>
    );
};

export default LSEEDSignup;