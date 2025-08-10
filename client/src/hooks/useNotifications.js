import { useState, useEffect, useRef } from "react";
import axiosClient from "../api/axiosClient";

export const useNotifications = (userId) => {
  const [notifications, setNotifications] = useState([]);
  const [error, setError] = useState(null);
  const [intervalMs, setIntervalMs] = useState(5000); // start polling every 5s
  const pollRef = useRef(null);

  useEffect(() => {
    if (!userId) return;

    const fetchNotifications = async () => {
      try {
        const { data } = await axiosClient.get("/api/notifications", {
          params: { receiver_id: userId },
        });
        setNotifications(data);
        setError(null);
        setIntervalMs(5000); // reset backoff on success
      } catch (err) {
        console.error("❌ Error fetching notifications:", err);
        setError(err);

        // If we hit 429, back off polling to reduce load
        if (err?.response?.status === 429) {
          setIntervalMs((prev) => Math.min(prev * 2, 60000)); // double up to max 1 min
        }
      }
    };

    // Initial fetch
    fetchNotifications();

    // Clear any previous polling before setting a new one
    if (pollRef.current) {
      clearInterval(pollRef.current);
    }

    pollRef.current = setInterval(fetchNotifications, intervalMs);

    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [userId, intervalMs]);

  return { notifications, setNotifications, error };
};
