import React, { createContext, useContext, useEffect, useState } from 'react';
import axios from 'axios';
import axiosClient from '../api/axiosClient';

const CsrfContext = createContext(null);

// Add a global to store the token for interceptors
let csrfTokenValue = null;

export const setGlobalCsrfToken = (token) => {
  csrfTokenValue = token;
};

export const getGlobalCsrfToken = () => csrfTokenValue;

export const CsrfProvider = ({ children }) => {
  const [csrfToken, setCsrfToken] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchCsrfToken = async () => {
      try {
        const res = await axiosClient.get(`/api/get-csrf-token`);
        setCsrfToken(res.data.csrfToken);
        setGlobalCsrfToken(res.data.csrfToken); // ✅ store it in the global too
      } catch (err) {
        console.error('Failed to fetch CSRF token:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchCsrfToken();
  }, []);

  return (
    <CsrfContext.Provider value={{ csrfToken, loading }}>
      {children}
    </CsrfContext.Provider>
  );
};

export const useCsrf = () => {
  return useContext(CsrfContext);
};