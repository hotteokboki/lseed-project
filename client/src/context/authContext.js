import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';

const AuthContext = createContext();

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthContextProvider");
  }
  return context; // No need for redundant default values here, as the throw handles null context
};

export const AuthContextProvider = ({ children }) => {
  const [user, setUser] = useState(null); // Store user session data
  const [loading, setLoading] = useState(true); // Loading state while checking session

  const [isMentorView, setIsMentorView] = useState(() => {
    // This function runs only once during the initial render
    try {
      const storedIsMentorView = localStorage.getItem('isMentorView');
      // localStorage stores everything as strings, so parse it back to a boolean
      return storedIsMentorView === 'true'; // Default to false if not "true" (including null)
    } catch (error) {
      console.error("Error reading isMentorView from localStorage:", error);
      return false; // Default to false if there's an error
    }
  }); 

  const navigate = useNavigate();

  // Effect to check for session and set initial view preferences on app load
  useEffect(() => {
    const storedUser = localStorage.getItem('user');
    if (storedUser) {
      try {
        const parsedUser = JSON.parse(storedUser);
        setUser(parsedUser);

        const hasLSEED = parsedUser.roles.some(role => role.startsWith("LSEED"));
        const hasMentor = parsedUser.roles.includes("Mentor");

        // Logic for setting isMentorView on initial app load/refresh
        if (hasLSEED && hasMentor) {
          // If user has both roles, the useState initializer already handled
          // restoring from localStorage. No need to re-set it here unless
          // you want to force a default *regardless* of stored preference.
          // Keeping it as is, to respect stored preference.
        } else if (hasMentor) {
            // Mentor ONLY: force mentor view and persist
            setIsMentorView(true);
            localStorage.setItem('isMentorView', 'true');
        } else { // Covers LSEED ONLY, Administrator ONLY, etc.
            // Other roles: force coordinator view and persist
            setIsMentorView(false);
            localStorage.setItem('isMentorView', 'false');
        }

      } catch (error) {
        console.error("Failed to parse user from localStorage:", error);
        localStorage.removeItem('user'); // Clear corrupted data
        localStorage.removeItem('isMentorView'); // Also clear view preference
        setUser(null);
        setIsMentorView(false); // Reset to default
      }
    }
    setLoading(false);
  }, []); // Empty dependency array means this runs only once on mount

  // Effect to update localStorage whenever isMentorView changes
  useEffect(() => {
    try {
      // Only set if user is logged in to avoid saving "false" if not authenticated
      if (user) {
        localStorage.setItem('isMentorView', isMentorView.toString()); // Store as string
      }
    } catch (error) {
      console.error("Error writing isMentorView to localStorage:", error);
    }
  }, [isMentorView, user]); // Depend on user as well, to avoid saving if not logged in

  // Login function to set session and update user state
  const login = async (userData) => {
    try {
      console.log("[authContext] Logging in user:", userData);
      localStorage.setItem("user", JSON.stringify(userData));
      setUser(userData);

      const hasLSEED = userData.roles.some(role => role.startsWith("LSEED"));
      const hasMentor = userData.roles.includes("Mentor");

      // Set initial view state right after login based on roles
      if (hasLSEED && hasMentor) {
        // User has both roles, respect stored preference on login, else default to coordinator
        const storedIsMentorView = localStorage.getItem('isMentorView');
        setIsMentorView(storedIsMentorView === 'true');
      } else if (hasMentor) {
        setIsMentorView(true); // Mentor ONLY: force mentor view
      } else { // Covers LSEED ONLY, Administrator ONLY, etc.
        setIsMentorView(false); // Other roles: force coordinator view
      }
      // The localStorage.setItem for isMentorView will be handled by the useEffect above
      // triggered by setIsMentorView.

      // Navigation logic after successful login
      if (userData.roles.includes("Administrator")) {
        navigate("/admin");
      } else if (userData.roles.some(r => r.startsWith("LSEED"))) { // LSEED-Coordinator or LSEED-Director
        navigate("/dashboard/lseed"); // Specific LSEED dashboard
      } else if (userData.roles.includes("Mentor")) { // Mentor (and not LSEED)
        navigate("/dashboard/mentor"); // Specific Mentor dashboard
      } else {
        navigate("/dashboard"); // Fallback for other roles or general dashboard
      }
    } catch (error) {
      console.error('Login error: ', error);
      // You might want to display an error message to the user here
      // For example, using a toast notification system
    }
  };

  // Logout function to clear session and user state
  const logout = async () => {
    try {
      // Assuming your backend /logout endpoint handles session invalidation on the server
      await axios.post(`${process.env.REACT_APP_API_BASE_URL}/logout`, null, {
        withCredentials: true,
      });

      console.log("[authContext] Logout successful");

      localStorage.removeItem('user');
      localStorage.removeItem('isMentorView');
      setUser(null);
      setIsMentorView(false); // Reset view state on logout
      navigate('/', { replace: true }); // Crucial for preventing back button issues
    } catch (error) {
      console.error("Error logging out:", error);
      // Even if backend logout fails, ensure frontend state is cleared for a better UX
      localStorage.removeItem('user');
      localStorage.removeItem('isMentorView');
      setUser(null);
      setIsMentorView(false);
      navigate('/', { replace: true });
    }
  };

  const toggleView = useCallback(() => {
    if (!user || !user.roles) {
      console.error("User or user roles not available for toggling.");
      return;
    }

    const hasMentorRole = user.roles.includes("Mentor");
    const hasLSEEDRole = user.roles.some(r => r.startsWith("LSEED"));

    // Allow toggling only if the user has BOTH roles
    if (hasMentorRole && hasLSEEDRole) {
      setIsMentorView(prevIsMentorView => {
        const newIsMentorView = !prevIsMentorView;
        const newActiveRole = newIsMentorView ? "Mentor" : "LSEED-Coordinator";

        console.log(`Toggling view: from ${prevIsMentorView ? 'Mentor' : 'Coordinator'} to ${newActiveRole}`);

        // ⭐️ Notify the server about the new active role
        fetch(`${process.env.REACT_APP_API_BASE_URL}/api/session/role`, {
          method: "POST",
          credentials: "include",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({ activeRole: newActiveRole })
        })
          .then(res => res.json())
          .then(data => {
            if (data.success) {
              console.log(`✅ Active role updated on server: ${newActiveRole}`);
            } else {
              console.error(`❌ Server error updating active role:`, data);
            }
          })
          .catch(error => {
            console.error("❌ Failed to send active role to server:", error);
          });

        return newIsMentorView;
      });
    } else {
      console.warn("Toggle attempted by a user without both 'Mentor' and 'LSEED' roles. No action taken.");
    }
  }, [user]);

  // Render a loading spinner or message while authentication state is being determined
  if (loading) {
    return <div>Loading authentication...</div>;
  }

  return (
    <AuthContext.Provider value={{ user, login, logout, isMentorView, toggleView, loading }}>
      {children}
    </AuthContext.Provider>
  );
};