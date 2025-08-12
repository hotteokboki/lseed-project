import React, { useState, useEffect } from "react";
import {
  Box,
  Button,
  Typography,
  TextField,
  Checkbox,
  MenuItem,
  Select,
  InputLabel,
  FormControl,
  Snackbar,
  Alert,
  Card,
  CardContent,
} from "@mui/material";
import { useTheme } from "@mui/material/styles";
import { tokens } from "../../theme";
import Header from "../../components/Header";
import axiosClient from "../../api/axiosClient";

// Password strength logic that matches your rules
const getPasswordChecklist = (password) => {
  const hasUpper = /[A-Z]/.test(password);
  const hasLower = /[a-z]/.test(password);
  const hasNum = /[0-9]/.test(password);
  const hasSpec = /[\W_]/.test(password); // any non-word char or underscore
  return {
    length: password.length >= 8,   // align with your copy
    uppercase: hasUpper,
    lowercase: hasLower,
    number: hasNum,
    specialChar: hasSpec,
  };
};

const getPasswordStrength = (password) => {
  const c = getPasswordChecklist(password);
  const met = [c.uppercase, c.lowercase, c.number, c.specialChar].filter(Boolean).length;

  if (c.length && met === 4) return "Strong";                // 12+ and all four
  if (password.length >= 8 && met >= 3) return "Moderate";   // decent but not all
  return "Weak";
};


const ProfilePage = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  // --- Security state ---
  const [successMsg, setSuccessMsg] = useState(""); // generic success snackbar text

  // Change password
  const [pwd, setPwd] = useState({ current: "", next: "", confirm: "" });
  const [pwdSaving, setPwdSaving] = useState(false);

  // state
  const [pwdVerified, setPwdVerified] = useState(false);
  const [pwdChecking, setPwdChecking] = useState(false);
  const [verifyError, setVerifyError] = useState("");

  // verify current password first
  const handleVerifyCurrent = async () => {
    if (!pwd.current) {
      setVerifyError("Please enter your current password.");
      return;
    }
    try {
      setPwdChecking(true);
      setVerifyError("");
      setError(""); // optional: clear global error
      const res = await axiosClient.post("/api/security/change-password/verify-current", {
        currentPassword: pwd.current,
      });
      if (res.data?.ok) {
        setPwdVerified(true);
        setSuccessMsg("Current password verified.");
        setIsSuccess(true);
      }
    } catch (e) {
      setPwdVerified(false);
      // show inline error under the field
      setVerifyError(e?.response?.data?.message || "Failed to verify password.");
    } finally {
      setPwdChecking(false);
    }
  };

  const [pwdStrength, setPwdStrength] = useState("Weak");
  const [pwdChecklist, setPwdChecklist] = useState(getPasswordChecklist(""));

  // Two-factor auth (2FA)
  const [twoFA, setTwoFA] = useState({
    enabled: false,
    qr: null,          // data URL for QR
    secret: null,      // (optional) if you want to show secret
    verifying: false,
    code: "",
    loading: false,
  });

  // Security questions
  const [secQs, setSecQs] = useState([
    { question: "", answer: "" },
    { question: "", answer: "" },
    { question: "", answer: "" },
  ]);
  const [sqSaving, setSqSaving] = useState(false);

  // Common-answer blocklist for security answers
  const COMMON_ANSWERS = new Set([
    "the bible", "qwerty", "password", "123456", "n/a", "none", "unknown", "no idea",
    "filipinas", "philippines", "manila", "jose rizal", "maria", "juan", "i don't know"
  ]);

  // entropy-ish check for answers
  const isAnswerWeak = (s) => {
    if (!s) return true;
    const a = s.trim().toLowerCase();
    if (a.length < 14) return true;                         // require longer strings
    if (COMMON_ANSWERS.has(a)) return true;                 // block common answers
    const uniqueChars = new Set(a.replace(/\s+/g, ""));     // basic uniqueness check
    return uniqueChars.size < 8;
  };

  // quick random passphrase generator (nonsense answers)
  const randomAnswer = () => {
    const words = [
      "lilac", "ember", "coastal", "quartz", "falcon", "pixel", "hollow", "nectar",
      "krypton", "delta", "vector", "prism", "hazel", "onyx", "cinder", "marble",
      "aurora", "glacier", "citron", "violet", "plasma", "tundra", "cobalt", "zinnia"
    ];
    const pick = () => words[Math.floor(Math.random() * words.length)];
    // 4–5 words + a number for entropy
    return `${pick()}-${pick()}-${pick()}-${pick()}-${Math.floor(100 + Math.random() * 900)}`;
  };

  const [profileData, setProfileData] = useState({
    firstName: "",
    lastName: "",
    email: "",
    contactnum: "",
    businessAreas: [],
    preferredTime: "",
    specificTime: "",
    communicationMode: [],
    bio: "",
    role: "",
  });

  const [originalData, setOriginalData] = useState(null);
  const [isSuccess, setIsSuccess] = useState(false);
  const [error, setError] = useState("");
  const [isEditing, setIsEditing] = useState(false);
  const [loading, setLoading] = useState(true);
  const [confirmError, setConfirmError] = useState("");

  useEffect(() => {
    const fetchAll = async () => {
      setLoading(true);
      setError("");
      try {
        const [profileRes, twofaRes, sqRes] = await Promise.allSettled([
          axiosClient.get("/api/profile"),
          axiosClient.get("/api/security/2fa/status"),
          axiosClient.get("/api/security/security-questions"),
        ]);

        if (profileRes.status === "fulfilled") {
          const data = profileRes.value.data;
          const profileInfo = {
            firstName: data.firstName || "",
            lastName: data.lastName || "",
            email: data.email || "",
            contactnum: data.contactnum || "",
            businessAreas: data.businessAreas || [],
            preferredTime: "",
            specificTime: "",
            communicationMode: [],
            bio: "",
            role: data.role || "",
          };
          setProfileData(profileInfo);
          setOriginalData(profileInfo);
        } else {
          throw new Error(profileRes.reason?.message || "Profile load failed");
        }

        if (twofaRes.status === "fulfilled") {
          setTwoFA(p => ({ ...p, enabled: !!twofaRes.value.data?.enabled }));
        }

        if (sqRes.status === "fulfilled") {
          const arrIn = Array.isArray(sqRes.value.data) ? sqRes.value.data : [];
          const arr = arrIn.slice(0, 3);
          while (arr.length < 3) arr.push({ question: "", answer: "" });
          setSecQs(arr);
        }
      } catch (err) {
        setError(`Failed to load profile: ${err.message}`);
      } finally {
        setLoading(false);
      }
    };
    fetchAll();
  }, []);

  const handleChange = (field, value) => {
    setProfileData((prev) => ({ ...prev, [field]: value }));
  };

  // ---- Change Password ----
  const handleChangePassword = async () => {
    // must verify current first
    if (!pwdVerified) {
      return setError("Please verify your current password first.");
    }

    if (!pwd.current || !pwd.next || !pwd.confirm) {
      return setError("Please fill in all password fields.");
    }
    if (pwd.next !== pwd.confirm) {
      return setError("New password and confirmation do not match.");
    }

    // FE rules: 8+ with upper, lower, number, special
    const checklist = getPasswordChecklist(pwd.next);
    const allGood =
      checklist.length &&
      checklist.uppercase &&
      checklist.lowercase &&
      checklist.number &&
      checklist.specialChar;

    if (!allGood) {
      return setError(
        "Password must be at least 8 characters and include uppercase, lowercase, a number, and a special character."
      );
    }

    const strengthNow = getPasswordStrength(pwd.next);
    if (strengthNow !== "Strong") {
      return setError(`Password strength is ${strengthNow}. Please make it stronger.`);
    }

    try {
      setPwdSaving(true);
      setError("");
      await axiosClient.post("/api/security/change-password", {
        currentPassword: pwd.current,
        newPassword: pwd.next,
      });

      setSuccessMsg("Password changed successfully.");
      setIsSuccess(true);

      // reset fields & state
      setPwd({ current: "", next: "", confirm: "" });
      setPwdStrength("Weak");
      setPwdChecklist(getPasswordChecklist(""));
      setPwdVerified(false); // require re-verify next time

      // optional: no need to reload; UI already reflects success
      // setTimeout(() => window.location.reload(), 1500);
    } catch (e) {
      setError(e?.response?.data?.message || e.message || "Failed to change password.");
    } finally {
      setPwdSaving(false);
    }
  };

  // ---- 2FA ----
  const start2FASetup = async () => {
    try {
      setTwoFA((p) => ({ ...p, loading: true }));
      const res = await axiosClient.get("/api/security/2fa/setup");
      // Expecting { qrCodeDataURL, secret } from server
      setTwoFA((p) => ({
        ...p,
        qr: res.data?.qrCodeDataURL || null,
        secret: res.data?.secret || null,
        verifying: true,
        loading: false
      }));
    } catch (e) {
      setError(e?.response?.data?.message || e.message || "Failed to start 2FA setup.");
      setTwoFA((p) => ({ ...p, loading: false }));
    }
  };

  const verifyAndEnable2FA = async () => {
    if (!twoFA.code) return setError("Enter the 6-digit code from your authenticator app.");
    try {
      setTwoFA((p) => ({ ...p, loading: true }));
      await axiosClient.post("/api/security/2fa/enable", { code: twoFA.code });
      setSuccessMsg("Two-factor authentication is now enabled.");
      setIsSuccess(true);
      setTwoFA({ enabled: true, qr: null, secret: null, verifying: false, code: "", loading: false });
    } catch (e) {
      setError(e?.response?.data?.message || e.message || "Invalid code. Please try again.");
      setTwoFA((p) => ({ ...p, loading: false }));
    }
  };

  const disable2FA = async () => {
    try {
      setTwoFA((p) => ({ ...p, loading: true }));
      await axiosClient.post("/api/security/2fa/disable");
      setSuccessMsg("Two-factor authentication has been disabled.");
      setIsSuccess(true);
      setTwoFA({ enabled: false, qr: null, secret: null, verifying: false, code: "", loading: false });
    } catch (e) {
      setError(e?.response?.data?.message || e.message || "Failed to disable 2FA.");
      setTwoFA((p) => ({ ...p, loading: false }));
    }
  };

  // ---- Security Questions ----
  const updateSecQ = (idx, field, value) => {
    setSecQs((prev) => {
      const next = [...prev];
      next[idx] = { ...next[idx], [field]: value };
      return next;
    });
  };

  const saveSecurityQuestions = async () => {
    // Validate: discourage common/guessable answers
    for (let i = 0; i < secQs.length; i++) {
      const { question, answer } = secQs[i];
      if (!question?.trim() || !answer?.trim()) {
        return setError(`Security question ${i + 1}: question and answer are required.`);
      }
      if (isAnswerWeak(answer)) {
        return setError(`Security question ${i + 1}: please use a longer, random answer (avoid common words/short strings).`);
      }
    }

    try {
      setSqSaving(true);
      setError("");
      await axiosClient.put("/api/security/security-questions", { questions: secQs });
      setSuccessMsg("Security questions updated.");
      setIsSuccess(true);
    } catch (e) {
      setError(e?.response?.data?.message || e.message || "Failed to save security questions.");
    } finally {
      setSqSaving(false);
    }
  };

  const handleSave = async () => {
    try {
      setError("");
      await axiosClient.put("/api/profile", profileData);

      setIsSuccess(true);
      setIsEditing(false);
      setOriginalData({ ...profileData });

      // Reload after snackbar shows for a moment
      setTimeout(() => {
        window.location.reload();
      }, 1500); // adjust delay if needed

    } catch (err) {
      setError(err.message || "Failed to save changes");
    }
  };

  const handleCancel = () => {
    setIsEditing(false);
    setError("");
    if (originalData) {
      setProfileData({ ...originalData });
    }
  };

  if (loading) {
    return (
      <Box m="20px">
        <Typography>Loading profile...</Typography>
      </Box>
    );
  }

  return (
    <Box m="20px">
      <Header
        title="USER PROFILE"
        subtitle="View and edit your profile details"
      />

      {/* Role Badge */}
      <Box mb={3}>
        <Typography
          variant="h6"
          sx={{
            display: "inline-block",
            px: 3,
            py: 1,
            backgroundColor: colors.primary[400],
            color: colors.grey[100],
            borderRadius: "20px",
            fontWeight: "bold",
          }}
        >
          Role: {profileData.role}
        </Typography>
      </Box>

      {/* Action Buttons */}
      <Box display="flex" gap={2} mb={3} justifyContent="left">
        {!isEditing ? (
          <Button
            variant="contained"
            onClick={() => setIsEditing(true)}
            sx={{
              backgroundColor: colors.blueAccent[600],
              color: "white",
              "&:hover": { backgroundColor: colors.blueAccent[700] },
            }}
          >
            Edit Profile
          </Button>
        ) : (
          <>
            <Button
              variant="outlined"
              onClick={handleCancel}
              sx={{
                color: colors.grey[100],
                borderColor: colors.grey[400],
                "&:hover": { borderColor: colors.grey[300] },
              }}
            >
              Cancel
            </Button>
            <Button
              variant="contained"
              onClick={handleSave}
              sx={{
                backgroundColor: colors.greenAccent[600],
                color: "white",
                "&:hover": { backgroundColor: colors.greenAccent[700] },
              }}
            >
              Save Changes
            </Button>
          </>
        )}
      </Box>

      {/* Profile Card */}
      <Card sx={{ backgroundColor: colors.primary[400], mb: 3 }}>
        <CardContent>
          <Typography variant="h5" color={colors.grey[100]} mb={3}>
            Basic Information
          </Typography>

          <Box display="flex" flexDirection="column" gap={2.5}>
            <Box display="flex" gap={2}>
              <TextField
                label="First Name"
                fullWidth
                value={profileData.firstName}
                onChange={(e) => handleChange("firstName", e.target.value)}
                disabled={!isEditing}
                sx={{
                  "& .MuiInputLabel-root": { color: colors.grey[100] },
                  "& .MuiOutlinedInput-root": {
                    color: colors.grey[100],
                    "& fieldset": { borderColor: colors.grey[400] },
                    "&:hover fieldset": { borderColor: colors.grey[300] },
                    "&.Mui-focused fieldset": {
                      borderColor: colors.blueAccent[500],
                    },
                  },
                }}
              />
              <TextField
                label="Last Name"
                fullWidth
                value={profileData.lastName}
                onChange={(e) => handleChange("lastName", e.target.value)}
                disabled={!isEditing}
                sx={{
                  "& .MuiInputLabel-root": { color: colors.grey[100] },
                  "& .MuiOutlinedInput-root": {
                    color: colors.grey[100],
                    "& fieldset": { borderColor: colors.grey[400] },
                    "&:hover fieldset": { borderColor: colors.grey[300] },
                    "&.Mui-focused fieldset": {
                      borderColor: colors.blueAccent[500],
                    },
                  },
                }}
              />
            </Box>

            <TextField
              label="Email Address"
              fullWidth
              value={profileData.email}
              onChange={(e) => handleChange("email", e.target.value)}
              disabled={!isEditing}
              sx={{
                "& .MuiInputLabel-root": { color: colors.grey[100] },
                "& .MuiOutlinedInput-root": {
                  color: colors.grey[100],
                  "& fieldset": { borderColor: colors.grey[400] },
                  "&:hover fieldset": { borderColor: colors.grey[300] },
                  "&.Mui-focused fieldset": {
                    borderColor: colors.blueAccent[500],
                  },
                },
              }}
            />

            <TextField
              label="Contact Number"
              fullWidth
              value={profileData.contactnum}
              onChange={(e) => handleChange("contactnum", e.target.value)}
              disabled={!isEditing}
              sx={{
                "& .MuiInputLabel-root": { color: colors.grey[100] },
                "& .MuiOutlinedInput-root": {
                  color: colors.grey[100],
                  "& fieldset": { borderColor: colors.grey[400] },
                  "&:hover fieldset": { borderColor: colors.grey[300] },
                  "&.Mui-focused fieldset": {
                    borderColor: colors.blueAccent[500],
                  },
                },
              }}
            />
          </Box>
        </CardContent>
      </Card>

      {/* Mentor-specific fields */}
      {profileData.role === "Mentor" && (
        <Card sx={{ backgroundColor: colors.primary[400], mb: 3 }}>
          <CardContent>
            <Typography variant="h5" color={colors.grey[100]} mb={3}>
              Mentor Specialization
            </Typography>

            <Box display="flex" flexDirection="column" gap={2.5}>
              <FormControl fullWidth disabled={!isEditing}>
                <InputLabel sx={{ color: colors.grey[100] }}>
                  Business Areas
                </InputLabel>
                <Select
                  multiple
                  value={profileData.businessAreas}
                  onChange={(e) =>
                    handleChange("businessAreas", e.target.value)
                  }
                  renderValue={(selected) => selected.join(", ")}
                  sx={{
                    color: colors.grey[100],
                    "& .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.grey[400],
                    },
                    "&:hover .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.grey[300],
                    },
                    "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.blueAccent[500],
                    },
                  }}
                >
                  {businessAreaOptions.map((area) => (
                    <MenuItem key={area} value={area}>
                      <Checkbox
                        checked={profileData.businessAreas.includes(area)}
                        sx={{ color: colors.greenAccent[500] }}
                      />
                      <Typography>{area}</Typography>
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              <FormControl fullWidth disabled={!isEditing}>
                <InputLabel sx={{ color: colors.grey[100] }}>
                  Preferred Mentoring Time
                </InputLabel>
                <Select
                  value={profileData.preferredTime}
                  onChange={(e) =>
                    handleChange("preferredTime", e.target.value)
                  }
                  sx={{
                    color: colors.grey[100],
                    "& .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.grey[400],
                    },
                    "&:hover .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.grey[300],
                    },
                    "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.blueAccent[500],
                    },
                  }}
                >
                  <MenuItem value="Weekday (Morning)">
                    Weekday (Morning) 8AM - 12NN
                  </MenuItem>
                  <MenuItem value="Weekday (Afternoon)">
                    Weekday (Afternoon) 1PM - 5PM
                  </MenuItem>
                  <MenuItem value="Other">Other</MenuItem>
                </Select>
              </FormControl>

              {profileData.preferredTime === "Other" && (
                <TextField
                  label="Specify preferred time"
                  fullWidth
                  value={profileData.specificTime}
                  onChange={(e) => handleChange("specificTime", e.target.value)}
                  disabled={!isEditing}
                  sx={{
                    "& .MuiInputLabel-root": { color: colors.grey[100] },
                    "& .MuiOutlinedInput-root": {
                      color: colors.grey[100],
                      "& fieldset": { borderColor: colors.grey[400] },
                      "&:hover fieldset": { borderColor: colors.grey[300] },
                      "&.Mui-focused fieldset": {
                        borderColor: colors.blueAccent[500],
                      },
                    },
                  }}
                />
              )}

              <FormControl fullWidth disabled={!isEditing}>
                <InputLabel sx={{ color: colors.grey[100] }}>
                  Communication Modes
                </InputLabel>
                <Select
                  multiple
                  value={profileData.communicationMode}
                  onChange={(e) =>
                    handleChange("communicationMode", e.target.value)
                  }
                  renderValue={(selected) => selected.join(", ")}
                  sx={{
                    color: colors.grey[100],
                    "& .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.grey[400],
                    },
                    "&:hover .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.grey[300],
                    },
                    "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
                      borderColor: colors.blueAccent[500],
                    },
                  }}
                >
                  {communicationModeOptions.map((mode) => (
                    <MenuItem key={mode} value={mode}>
                      <Checkbox
                        checked={profileData.communicationMode.includes(mode)}
                        sx={{ color: colors.greenAccent[500] }}
                      />
                      <Typography>{mode}</Typography>
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              <TextField
                label="Short Bio / Experience"
                multiline
                rows={4}
                fullWidth
                value={profileData.bio}
                onChange={(e) => handleChange("bio", e.target.value)}
                disabled={!isEditing}
                placeholder="Tell us about your experience and expertise..."
                sx={{
                  "& .MuiInputLabel-root": { color: colors.grey[100] },
                  "& .MuiOutlinedInput-root": {
                    color: colors.grey[100],
                    "& fieldset": { borderColor: colors.grey[400] },
                    "&:hover fieldset": { borderColor: colors.grey[300] },
                    "&.Mui-focused fieldset": {
                      borderColor: colors.blueAccent[500],
                    },
                  },
                }}
              />
            </Box>
          </CardContent>
        </Card>
      )}

      {/* Security */}
      <Card sx={{ backgroundColor: colors.primary[400], mb: 3 }}>
        <CardContent>
          <Typography variant="h5" color={colors.grey[100]} mb={2}>
            Security
          </Typography>

          {/* Change Password */}
          <Box mb={3}>
            <Typography variant="h6" color={colors.grey[100]} mb={1}>
              Change Password
            </Typography>
            <Typography variant="body2" color={colors.grey[200]} mb={2}>
              Verify your current password before setting a new one.
            </Typography>

            <Box display="grid" gridTemplateColumns="1fr 1fr" gap={2}>
              {/* Current password (always visible) */}
              <TextField
                label="Current Password"
                type="password"
                value={pwd.current}
                onChange={(e) => {
                  setPwd((p) => ({ ...p, current: e.target.value }));
                  if (verifyError) setVerifyError(""); // clear as user types
                }}
                disabled={pwdSaving || pwdChecking || pwdVerified}
                error={Boolean(verifyError)}
                helperText={verifyError || " "}
                sx={{
                  "& .MuiInputLabel-root": { color: colors.grey[100] },
                  "& .MuiOutlinedInput-root": { color: colors.grey[100] }
                }}
              />

              {/* Continue / Verified indicator */}
              <Box display="flex" alignItems="center" gap={1}>
                {!pwdVerified ? (
                  <Button
                    variant="contained"
                    onClick={handleVerifyCurrent}
                    disabled={!pwd.current || pwdChecking}
                    sx={{
                      backgroundColor: colors.blueAccent[600],
                      "&:hover": { backgroundColor: colors.blueAccent[700] }
                    }}
                  >
                    {pwdChecking ? "Checking..." : "Continue"}
                  </Button>
                ) : (
                  <Typography
                    color={colors.greenAccent[400]}
                    fontWeight={600}
                    sx={{ display: "flex", alignItems: "center", gap: 0.5 }}
                  >
                    Verified ✓
                  </Typography>
                )}

                {/* Inline error message */}
                {!pwdVerified && verifyError && (
                  <Typography variant="body2" color="#f44336" sx={{ ml: 1 }}>
                    {verifyError}
                  </Typography>
                )}
              </Box>

              {/* New + Confirm fields only AFTER verification */}
              {pwdVerified && (
                <>
                  <TextField
                    label="New Password"
                    type="password"
                    value={pwd.next}
                    onChange={(e) => {
                      const val = e.target.value;
                      setPwd((p) => ({ ...p, next: val }));
                      setPwdChecklist(getPasswordChecklist(val));
                      setPwdStrength(getPasswordStrength(val));

                      // live mismatch check
                      if (pwd.confirm && pwd.confirm !== val) {
                        setConfirmError("Passwords do not match.");
                      } else {
                        setConfirmError("");
                      }
                    }}
                    disabled={pwdSaving}
                    sx={{ "& .MuiInputLabel-root": { color: colors.grey[100] }, "& .MuiOutlinedInput-root": { color: colors.grey[100] } }}
                  />

                  <TextField
                    label="Confirm New Password"
                    type="password"
                    value={pwd.confirm}
                    onChange={(e) => {
                      const val = e.target.value;
                      setPwd((p) => ({ ...p, confirm: val }));
                      setConfirmError(val !== pwd.next ? "Passwords do not match." : "");
                    }}
                    disabled={pwdSaving}
                    error={Boolean(confirmError)}
                    helperText={confirmError || " "}
                    sx={{ "& .MuiInputLabel-root": { color: colors.grey[100] }, "& .MuiOutlinedInput-root": { color: colors.grey[100] } }}
                  />

                  {/* Submit + Cancel buttons */}
                  <Box display="flex" alignItems="center" gap={2}>
                    <Button
                      variant="contained"
                      onClick={handleChangePassword}
                      disabled={pwdSaving}
                      sx={{
                        backgroundColor: colors.greenAccent[600],
                        "&:hover": { backgroundColor: colors.greenAccent[700] }
                      }}
                    >
                      {pwdSaving ? "Saving..." : "Update Password"}
                    </Button>

                    <Button
                      variant="outlined"
                      color="secondary"
                      onClick={() => {
                        setPwd({ current: "", next: "", confirm: "" });
                        setPwdStrength("Weak");
                        setPwdChecklist(getPasswordChecklist(""));
                        setConfirmError?.("");      // if you have this state
                        setVerifyError("");         // clear inline verify error
                        setError("");               // clear global error/snackbar
                        setPwdVerified(false);      // <-- important: hide new/confirm section
                        setPwdChecking(false);      // reset any loading state
                      }}
                      disabled={pwdSaving}
                    >
                      Cancel
                    </Button>
                  </Box>

                  {/* Strength + checklist only when typing a new password */}
                  {pwd.next && (
                    <Box gridColumn="1 / -1" mt={1}>
                      <Typography
                        variant="body2"
                        sx={{
                          color:
                            pwdStrength === "Strong"
                              ? colors.greenAccent[400]
                              : pwdStrength === "Moderate"
                                ? colors.blueAccent[300]
                                : "#f44336",
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
                              color: pwdChecklist[item.key]
                                ? colors.greenAccent[400]
                                : colors.grey[200],
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
                </>
              )}
            </Box>
          </Box>

          {/* Two-Factor Authentication */}
          <Box mb={3}>
            <Typography variant="h6" color={colors.grey[100]} mb={1}>
              Two-Factor Authentication (2FA)
            </Typography>
            <Typography variant="body2" color={colors.grey[200]} mb={2}>
              Add an extra layer of security using an authenticator app (Google Authenticator, Authy, etc.).
            </Typography>

            {!twoFA.enabled && !twoFA.verifying && (
              <Button
                variant="contained"
                onClick={start2FASetup}
                disabled={twoFA.loading}
                sx={{ backgroundColor: colors.blueAccent[600], "&:hover": { backgroundColor: colors.blueAccent[700] } }}
              >
                {twoFA.loading ? "Preparing..." : "Set up 2FA"}
              </Button>
            )}

            {!twoFA.enabled && twoFA.verifying && (
              <Box display="flex" gap={3} alignItems="center" flexWrap="wrap">
                {twoFA.qr && (
                  <Box>
                    <img src={twoFA.qr} alt="Scan this QR with your authenticator app" style={{ width: 180, height: 180, borderRadius: 8 }} />
                    {twoFA.secret && (
                      <Typography variant="caption" color={colors.grey[200]}>
                        Secret: {twoFA.secret}
                      </Typography>
                    )}
                  </Box>
                )}
                <Box display="flex" gap={2} alignItems="center">
                  <TextField
                    label="6-digit code"
                    value={twoFA.code}
                    onChange={(e) =>
                      setTwoFA(p => ({ ...p, code: e.target.value.replace(/\D/g, "").slice(0, 6) }))
                    }
                    inputMode="numeric"
                    pattern="\d*"
                  />
                  <Button
                    variant="contained"
                    onClick={verifyAndEnable2FA}
                    disabled={twoFA.loading}
                    sx={{ backgroundColor: colors.greenAccent[600], "&:hover": { backgroundColor: colors.greenAccent[700] } }}
                  >
                    {twoFA.loading ? "Verifying..." : "Verify & Enable"}
                  </Button>
                </Box>
              </Box>
            )}

            {twoFA.enabled && (
              <Box display="flex" gap={2} alignItems="center">
                <Typography color={colors.greenAccent[400]}>2FA is enabled.</Typography>
                <Button
                  variant="outlined"
                  onClick={disable2FA}
                  disabled={twoFA.loading}
                  sx={{ color: colors.grey[100], borderColor: colors.grey[400], "&:hover": { borderColor: colors.grey[300] } }}
                >
                  {twoFA.loading ? "Disabling..." : "Disable 2FA"}
                </Button>
              </Box>
            )}
          </Box>

          {/* Security Questions (Randomized Answers) */}
          <Box>
            <Typography variant="h6" color={colors.grey[100]} mb={1}>
              Password Reset Questions (Use Random Answers)
            </Typography>
            <Typography variant="body2" color={colors.grey[200]} mb={2}>
              For security, <strong>do not use truthful answers</strong>. Use random, unique phrases.
              Avoid common answers (e.g., “The Bible”). Treat answers like extra passwords and store them in a password manager.
            </Typography>

            <Box display="flex" flexDirection="column" gap={2}>
              {secQs.map((qa, idx) => (
                <Box key={idx} display="grid" gridTemplateColumns="2fr 2fr auto" gap={2}>
                  <TextField
                    label={`Question ${idx + 1}`}
                    value={qa.question}
                    onChange={(e) => updateSecQ(idx, "question", e.target.value)}
                    placeholder="e.g., Custom prompt you'll remember"
                    sx={{
                      "& .MuiInputLabel-root": { color: colors.grey[100] },
                      "& .MuiOutlinedInput-root": { color: colors.grey[100] }
                    }}
                  />
                  <TextField
                    label="Random Answer"
                    value={qa.answer}
                    onChange={(e) => updateSecQ(idx, "answer", e.target.value)}
                    helperText={qa.answer && isAnswerWeak(qa.answer) ? "Answer looks weak. Use longer random phrase." : " "}
                    sx={{
                      "& .MuiInputLabel-root": { color: colors.grey[100] },
                      "& .MuiOutlinedInput-root": { color: colors.grey[100] }
                    }}
                  />
                  <Button
                    variant="outlined"
                    onClick={() => updateSecQ(idx, "answer", randomAnswer())}
                    sx={{ color: colors.grey[100], borderColor: colors.grey[400], "&:hover": { borderColor: colors.grey[300] } }}
                  >
                    Generate
                  </Button>
                </Box>
              ))}

              <Box>
                <Button
                  variant="contained"
                  onClick={saveSecurityQuestions}
                  disabled={sqSaving}
                  sx={{ backgroundColor: colors.greenAccent[600], "&:hover": { backgroundColor: colors.greenAccent[700] } }}
                >
                  {sqSaving ? "Saving..." : "Save Security Questions"}
                </Button>
              </Box>
            </Box>
          </Box>
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {/* Success Snackbar */}
      <Snackbar
        open={isSuccess}
        autoHideDuration={1500}
        onClose={() => setIsSuccess(false)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        <Alert onClose={() => setIsSuccess(false)} severity="success" sx={{ width: "100%" }}>
          {successMsg || "Profile updated successfully!"}
        </Alert>
      </Snackbar>
    </Box>
  );
};

const businessAreaOptions = [
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

const communicationModeOptions = [
  "Face to Face",
  "Facebook Messenger",
  "Google Meet",
  "Zoom",
  "Other",
];

export default ProfilePage;
