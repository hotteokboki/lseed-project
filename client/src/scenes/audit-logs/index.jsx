import React, { useEffect, useState } from "react";
import { Box, Typography, useTheme, Snackbar, Alert } from "@mui/material";
import { DataGrid } from "@mui/x-data-grid";
import Header from "../../components/Header";
import { tokens } from "../../theme";
import axiosClient from "../../api/axiosClient";

const AuditLogsPage = () => {
    const theme = useTheme();
    const colors = tokens(theme.palette.mode);

    const [logs, setLogs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [snackbarOpen, setSnackbarOpen] = useState(false);

    useEffect(() => {
    const fetchLogs = async () => {
        try {
        setLoading(true);
        const response = await axiosClient.get(`/api/get-audit-logs`);
        
        const data = response.data;
        setLogs(data.logs); 

        console.log(data.logs)
        } catch (err) {
        console.error(err);
        setError(err.message || "An error occurred while fetching logs.");
        setSnackbarOpen(true);
        } finally {
        setLoading(false);
        }
    };

    fetchLogs();
    }, []);

    const columns = [
        {
            field: "actor",
            headerName: "Actor",
            flex: 1,
            renderCell: (params) => {
            const { first_name, last_name } = params.row;
            return (first_name && last_name) ? `${first_name} ${last_name}` : "System";
            }
        },
        {
            field: "action",
            headerName: "Action",
            flex: 1,
        },
        {
            field: "details",
            headerName: "Details",
            flex: 2,
            renderCell: (params) => {
            const details = params.row.details;
            if (!details) return "";
            if (typeof details === "object") {
                return Object.entries(details)
                .map(([k, v]) => `${k}: ${v}`)
                .join(", ");
            }
            return String(details);
            }
        },
        {
            field: "timestamp",
            headerName: "Timestamp",
            flex: 1,
            renderCell: (params) => {
            // Defensive rendering
            const raw = params?.row?.timestamp;
            if (!raw) return "—";
            try {
                return new Date(raw).toLocaleString();
            } catch {
                return raw;
            }
            },
        },
    ];

    return (
    <Box m="20px">
        <Header title="AUDIT LOGS" subtitle="View all system activities" />

        <Box
        height="600px"
        backgroundColor={colors.primary[400]}
        padding="20px"
        sx={{
            "& .MuiDataGrid-root": { border: "none" },
            "& .MuiDataGrid-cell": { borderBottom: "none" },
            "& .MuiDataGrid-columnHeaders": {
            backgroundColor: colors.blueAccent[700],
            borderBottom: "none",
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
        {loading ? (
            <Typography variant="h5">Loading audit logs...</Typography>
        ) : (
            <DataGrid
                getRowId={(row) => row.log_id}
                rows={logs}
                columns={columns}
                pageSize={5}
                rowsPerPageOptions={[5, 10]}
                disableSelectionOnClick
            />

        )}
        </Box>

        <Snackbar
        open={snackbarOpen}
        autoHideDuration={3000}
        onClose={() => setSnackbarOpen(false)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
        >
        <Alert
            onClose={() => setSnackbarOpen(false)}
            severity="error"
            sx={{ width: "100%" }}
        >
            {error}
        </Alert>
        </Snackbar>
    </Box>
    );
};

export default AuditLogsPage;