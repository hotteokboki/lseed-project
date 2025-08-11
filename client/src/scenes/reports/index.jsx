import { useEffect } from "react";
import axios from "axios";
import React, { useRef, useState } from "react";
import {
  Box,
  Button,
  Typography,
  useTheme,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
} from "@mui/material";
import Header from "../../components/Header";
import { tokens } from "../../theme";
import * as XLSX from "xlsx";
import axiosClient from "../../api/axiosClient";

const Reports = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  const fileInputRef = useRef(null);
  const [parsedData, setParsedData] = useState({});
  const [fileName, setFileName] = useState("");
  const [selectedSE, setSelectedSE] = useState("");
  const [socialEnterprises, setSocialEnterprises] = useState([]);
  const [selectedReportType, setSelectedReportType] = useState("");

  const handleButtonClick = () => {
    fileInputRef.current.click();
  };

  const normalizeColumnName = (colName) => {
    return String(colName || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  };

  const columnMapping = {
    financial_statements: {
      "date": "date",
      "total_revenue": "total_revenue",
      "totalrevenue": "total_revenue",
      "total_expenses": "total_expenses",
      "totalexpenses": "total_expenses",
      "net_income": "net_income",
      "netincome": "net_income",
      "total_assets": "total_assets",
      "totalassets": "total_assets",
      "total_liabilities": "total_liabilities",
      "totalliabilities": "total_liabilities",
      "owner_equity": "owner_equity",
      "ownerequity": "owner_equity",
    },
    inventory_report: {
      "item_name": "item_name",
      "itemname": "item_name",
      "qty": "qty",
      "quantity": "qty",
      "price": "price",
      "amount": "amount",
    },
    cash_in: {
      "date": "date",
      "cash": "cash",
      "sales": "sales",
      "otherRevenue": "otherRevenue",
      "rawMaterials": "rawMaterials",
      "cashUnderAssets": "cashUnderAssets",
      "savings": "savings",
      "assets": "assets",
      "liability": "liability",
      "ownerCapital": "ownerCapital",
      "notes": "notes",
      "enteredBy": "enteredBy",
    },
    cash_out: {
      "date": "date",
      "cash": "cash",
      "utilities": "utilities",
      "officesupplies": "officeSupplies",
      "expenses": "expenses",
      "cashunderassets": "cashUnderAssets",
      "investments": "investments",
      "savings": "savings",
      "assets": "assets",
      "inventory": "inventory",
      "liability": "liability",
      "ownerswithdrawals": "ownerWithdrawal",
      "notes": "notes",
      "enteredby": "enteredBy",
    },
  };

  const parseNumericValue = (value) => {
    if (value === null || value === undefined) {
      return null;
    }
    
    if (typeof value === 'string') {
      const cleanedValue = value.replace(/,/g, '').trim();

      const excelErrorStrings = ['#N/A', '#REF!', '#NAME?', '#VALUE!', '#DIV/0!', '#NULL!', '#NUM!'];
      if (excelErrorStrings.includes(cleanedValue.toUpperCase())) {
          return null;
      }

      if (cleanedValue === '') {
        return null;
      }
      const parsed = parseFloat(cleanedValue);
      return isNaN(parsed) ? null : parsed;
    }
    return value;
  };

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    if (!file) return;

    setFileName(file.name);
    setParsedData({}); // Reset parsed data

    const reader = new FileReader();
    reader.onload = (e) => {
      const data = new Uint8Array(e.target.result);
      const workbook = XLSX.read(data, { type: "array" });
      const newParsedData = {};

      workbook.SheetNames.forEach((sheetName) => {
        const worksheet = workbook.Sheets[sheetName];
        const rawData = XLSX.utils.sheet_to_json(worksheet, { header: 1, raw: false });

        if (rawData.length === 0) return;

        let targetTable = null;
        let dataRows = [];
        let globalReportDate = null;
        let globalItemName = null;

        const lowerSheetName = sheetName.toLowerCase();
        const lowerFileName = file.name.toLowerCase();

        if (lowerSheetName.includes("cash in") || lowerFileName.includes("cash in")) {
          targetTable = "cash_in";
          const dateCell = String(rawData[1]?.[0] || '').trim();
          if (dateCell.toLowerCase().startsWith("month:")) {
              globalReportDate = dateCell.substring(dateCell.indexOf(':') + 1).trim();
          } else if (dateCell.toLowerCase().startsWith("date:")) {
              globalReportDate = dateCell.substring(dateCell.indexOf(':') + 1).trim();
          }
        } else if (lowerSheetName.includes("cash out") || lowerFileName.includes("cash out")) {
          targetTable = "cash_out";
          const dateCell = String(rawData[1]?.[0] || '').trim();
          if (dateCell.toLowerCase().startsWith("month:")) {
              globalReportDate = dateCell.substring(dateCell.indexOf(':') + 1).trim();
          } else if (dateCell.toLowerCase().startsWith("date:")) {
              globalReportDate = dateCell.substring(dateCell.indexOf(':') + 1).trim();
          }
        } else if (lowerSheetName.includes("inventory report") || lowerFileName.includes("inventory report") || selectedReportType === "inventory_report") {
          targetTable = "inventory_report";
          const dateCell = String(rawData[1]?.[0] || '').trim();
          if (dateCell.toLowerCase().startsWith("date:")) {
              globalReportDate = dateCell.substring(dateCell.indexOf(':') + 1).trim();
          } else if (dateCell.toLowerCase().startsWith("month:")) {
              globalReportDate = dateCell.substring(dateCell.indexOf(':') + 1).trim();
          }

          const itemNameHeaderCell = String(rawData[4]?.[0] || '').trim();
          if (itemNameHeaderCell.toLowerCase().startsWith("item name:")) {
              globalItemName = itemNameHeaderCell.substring(itemNameHeaderCell.indexOf(':') + 1).trim();
          }
        } else if (lowerSheetName.includes("financial statements") || lowerFileName.includes("financial statements")) {
          targetTable = "financial_statements";
        }

        if (!targetTable && selectedReportType) {
          targetTable = selectedReportType;
        }

        if (!targetTable) {
          console.warn(`Skipping sheet '${sheetName}' because target table could not be determined.`);
          return;
        }

        if (targetTable === "cash_in" || targetTable === "cash_out") {
          if (rawData.length < 6) {
            console.warn(`File '${file.name}' or sheet '${sheetName}' does not have enough rows for expected Cash In/Out format. Skipping special processing.`);
            dataRows = rawData.slice(1);
          } else {
            dataRows = rawData.slice(5);
          }
        } else if (targetTable === "inventory_report") {
            if (rawData.length < 6) {
                console.warn(`File '${file.name}' or sheet '${sheetName}' does not have enough rows for expected Inventory Report format. Skipping special processing.`);
                dataRows = rawData.slice(1);
            } else {
                dataRows = rawData.slice(5);
            }
        } else {
          dataRows = rawData.slice(1);
        }

        const filteredDataRows = dataRows.filter(row => {
          const firstCell = String(row[0] || '').trim().toLowerCase();
          
          if (firstCell.startsWith("totals:") || 
              firstCell.startsWith("(") ||
              firstCell === 'less: final count' ||
              firstCell === 'add: purchases' ||
              firstCell === 'total cost of goods sold' ||
              firstCell === 'weighted average formula' ||
              firstCell === 'price of final count =' ||
              firstCell === 'total amount of purchases' ||
              firstCell === 'total quantity' ||
              firstCell === 'final count' ||
              firstCell === 'final total cost of goods sold'
             ) {
            return false;
          }

          if (firstCell === '') {
            return false;
          }

          return true;
        });
        let currentActiveItemName = globalItemName; 

        const transformedSheetData = filteredDataRows.map((row) => {
          const newRow = {};

          if (targetTable === "cash_in") {
            if (globalReportDate !== null) {
                newRow["month"] = globalReportDate;
            }
            const dateValue = String(row[0] || '').trim();
            if (dateValue !== '') newRow["date"] = dateValue;

            const cash = parseNumericValue(row[1]);
            if (cash !== null) newRow["cash"] = cash;

            const sales = parseNumericValue(row[2]);
            if (sales !== null) newRow["sales"] = sales;

            const otherRevenue = parseNumericValue(row[3]);
            if (otherRevenue !== null) newRow["otherRevenue"] = otherRevenue;

            const rawMaterials = parseNumericValue(row[4]);
            if (rawMaterials !== null) newRow["rawMaterials"] = rawMaterials;
            
            const cashUnderAssets = parseNumericValue(row[5]);
            if (cashUnderAssets !== null) newRow["cashUnderAssets"] = cashUnderAssets;

            const savings = parseNumericValue(row[6]);
            if (savings !== null) newRow["savings"] = savings;

            let totalAssets = 0;
            let hasAnyAssetComponent = false;
            if (rawMaterials !== null) { totalAssets += rawMaterials; hasAnyAssetComponent = true; }
            if (cashUnderAssets !== null) { totalAssets += cashUnderAssets; hasAnyAssetComponent = true; }
            if (savings !== null) { totalAssets += savings; hasAnyAssetComponent = true; }

            if (totalAssets !== 0) {
              newRow["assets"] = totalAssets;
            } else if (hasAnyAssetComponent) {
              newRow["assets"] = 0;
            }

            const liability = parseNumericValue(row[7]);
            if (liability !== null) newRow["liability"] = liability;

            const ownerCapital = parseNumericValue(row[8]);
            if (ownerCapital !== null) newRow["ownerCapital"] = ownerCapital;

            const notesValue = String(row[9] || '').trim();
            if (notesValue !== '') newRow["notes"] = notesValue;

            const enteredByValue = String(row[10] || '').trim();
            if (enteredByValue !== '') newRow["enteredBy"] = enteredByValue;

          } else if (targetTable === "cash_out") {
            if (globalReportDate !== null) {
                newRow["month"] = globalReportDate;
            }

            const dateValue = String(row[0] || '').trim();
            if (dateValue !== '') newRow["date"] = dateValue;

            const cash = parseNumericValue(row[1]);
            if (cash !== null) newRow["cash"] = cash;

            const utilities = parseNumericValue(row[3]);
            if (utilities !== null) newRow["utilities"] = utilities;

            const officeSupplies = parseNumericValue(row[4]);
            if (officeSupplies !== null) newRow["officeSupplies"] = officeSupplies;

            let totalExpenses = 0;
            let hasAnyExpenseComponent = false;
            if (utilities !== null) { totalExpenses += utilities; hasAnyExpenseComponent = true; }
            if (officeSupplies !== null) { totalExpenses += officeSupplies; hasAnyExpenseComponent = true; }

            if (totalExpenses !== 0) {
              newRow["expenses"] = totalExpenses;
            } else if (hasAnyExpenseComponent) {
                newRow["expenses"] = 0;
            }

            const cashUnderAssets = parseNumericValue(row[10]);
            if (cashUnderAssets !== null) newRow["cashUnderAssets"] = cashUnderAssets;

            const investments = parseNumericValue(row[11]);
            if (investments !== null) newRow["investments"] = investments;

            const savings = parseNumericValue(row[12]);
            if (savings !== null) newRow["savings"] = savings;

            let totalAssets = 0;
            let hasAnyAssetComponent = false;
            if (cashUnderAssets !== null) { totalAssets += cashUnderAssets; hasAnyAssetComponent = true; }
            if (investments !== null) { totalAssets += investments; hasAnyAssetComponent = true; }
            if (savings !== null) { totalAssets += savings; hasAnyAssetComponent = true; }

            if (totalAssets !== 0) {
              newRow["assets"] = totalAssets;
            } else if (hasAnyAssetComponent) {
                newRow["assets"] = 0;
            }

            const inventory = parseNumericValue(row[13]);
            if (inventory !== null) newRow["inventory"] = inventory;

            const liability = parseNumericValue(row[14]);
            if (liability !== null) newRow["liability"] = liability;

            const ownerWithdrawal = parseNumericValue(row[15]);
            if (ownerWithdrawal !== null) newRow["ownerWithdrawal"] = ownerWithdrawal;

            const notesValue = String(row[16] || '').trim();
            if (notesValue !== '') newRow["notes"] = notesValue;

            const enteredByValue = String(row[17] || '').trim();
            if (enteredByValue !== '') newRow["enteredBy"] = enteredByValue;

        } else if (targetTable === "inventory_report") {
            if (globalReportDate !== null) {
                newRow["month"] = globalReportDate;
            }

            const rawItemLabel = String(row[0] || '').trim();

            if (rawItemLabel.toLowerCase().startsWith('item name:')) {
                currentActiveItemName = rawItemLabel.substring(rawItemLabel.indexOf(':') + 1).trim();
                return null;
            }

            if (rawItemLabel.toLowerCase() === 'beggining inventory' && currentActiveItemName !== null) {
                newRow["item_name"] = currentActiveItemName;
            } else if (rawItemLabel !== '') {
                newRow["item_name"] = rawItemLabel;
            }

            const qty = parseNumericValue(row[1]);
            if (qty !== null) newRow["qty"] = qty;

            const price = parseNumericValue(row[2]);
            if (price !== null) newRow["price"] = price;

            const amount = parseNumericValue(row[3]);
            if (amount !== null) newRow["amount"] = amount;

        } else {
            const mainHeader = rawData[0];
            mainHeader.forEach((header, index) => {
              const mappedColName = columnMapping[targetTable]?.[normalizeColumnName(header)];
              if (mappedColName) {
                let value = row[index];
                if (typeof value === 'string') {
                  const cleanedValue = value.replace(/,/g, '').trim();
                  if (cleanedValue === '') {
                    value = null;
                  } else if (!isNaN(parseFloat(cleanedValue))) {
                    value = parseFloat(cleanedValue);
                  }
                }
                if (value !== null && value !== undefined && value !== '') {
                    newRow[mappedColName] = value;
                }
              }
            });
          }

          return newRow;
        }).filter(row => row !== null && Object.keys(row).length > 0);

        if (transformedSheetData.length > 0) {
          newParsedData[targetTable] = transformedSheetData;
        } else {
          console.warn(`Sheet '${sheetName}' parsed but yielded no valid mapped data. Skipping.`);
        }
      });
      setParsedData(newParsedData);
    };
    reader.readAsArrayBuffer(file);
  };

  const handleImport = async () => {
    if (!selectedSE) {
      alert("Please select a Social Enterprise before importing.");
      return;
    }
    if (Object.keys(parsedData).length === 0) {
      alert("No data to import. Please upload a file first.");
      return;
    }

    let allImportsSuccessful = true;
    for (const reportType in parsedData) {
      const dataToImport = parsedData[reportType];
      if (dataToImport.length === 0) {
        console.log(`No data for ${formatTableName(reportType)}. Skipping import.`);
        continue;
      }

      const expectedColsForType = Object.values(columnMapping[reportType] || {});
      const actualCols = Object.keys(dataToImport[0] || {});
      const missingCols = expectedColsForType.filter(col => !actualCols.includes(col));

      if (missingCols.length > 0) {
        console.warn(`Potential missing columns based on local mapping for ${formatTableName(reportType)}: ${missingCols.join(", ")}. Backend might have further validation.`);
      }

      console.log(`Sending to backend for ${reportType}:`, {
        se_id: selectedSE,
        data: dataToImport,
      });

      try {
        await axiosClient.post(
          `/api/import/${reportType}`,
          {
            se_id: selectedSE,
            data: dataToImport,
          },
        );
        console.log(`${formatTableName(reportType)} import successful!`);
      } catch (err) {
        console.error(`Import error for ${formatTableName(reportType)}:`, err);
        allImportsSuccessful = false;

        let errorMessage = `Import failed for ${formatTableName(reportType)}: `;
        if (err.response) {
          if (err.response.status === 400 && err.response.data?.message === "Missing required columns") {
            errorMessage += `${err.response.data.message}. Missing: ${err.response.data.missingFields?.join(", ")}`;
          } else if (err.response.data?.message) {
            errorMessage += err.response.data.message;
          } else {
            errorMessage += `Server responded with status ${err.response.status}.`;
          }
        } else {
          errorMessage += `Network error or server unavailable.`;
        }
        alert(errorMessage);
      }
    }

    if (allImportsSuccessful) {
      alert("All selected reports imported successfully!");
    } else {
      alert("Some reports failed to import. Check the console for details.");
    }
    setParsedData({});
    setFileName("");
  };

  const handleCancel = () => {
    setParsedData({});
    setFileName("");
  };

  const handleSEChange = (event) => {
    setSelectedSE(event.target.value);
  };

  useEffect(() => {
    const fetchSEs = async () => {
      try {
        const response = await axiosClient.get(`/api/getAllSocialEnterprisesForComparison`);
        setSocialEnterprises(response.data);
      } catch (error) {
        console.error("Error fetching SE list:", error);
      }
    };

    fetchSEs();
  }, []);

  const reportTables = [
    "financial_statements",
    "cash_in",
    "cash_out",
    "inventory_report",
  ];

  const formatTableName = (name) => {
    return name
      .split("_")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");
  };

  return (
    <Box m="20px">
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Header title="Reports" subtitle="Generate Reports" />
      </Box>

      {/* Dropdown on top */}
      <Box
        display="flex"
        flexDirection="column"
        alignItems="left"
        gap={4}
        mt={4}
      >
        <Box
          width="27%"
          bgcolor={colors.primary[400]}
          display="flex"
          padding={2}
          gap={2}
        >
          <FormControl
            fullWidth
            sx={{
              maxWidth: "500px",
              backgroundColor: colors.blueAccent[500],
            }}
          >
            <InputLabel id="se-select-label" sx={{ color: "white" }}>
              Select Social Enterprise
            </InputLabel>
            <Select
              labelId="se-select-label"
              value={selectedSE}
              label="Select Social Enterprise"
              onChange={handleSEChange}
              sx={{
                color: "white",
                ".MuiOutlinedInput-notchedOutline": { border: 0 },
                "& .MuiSvgIcon-root": { color: "white" },
              }}
            >
              {socialEnterprises.map((se) => (
                <MenuItem
                  key={se.se_id}
                  value={se.se_id}
                  sx={{ color: "white" }}
                >
                  {se.abbr}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>
        <Box
          width="27%"
          bgcolor={colors.primary[400]}
          display="flex"
          padding={2}
          gap={2}
        >
          <FormControl
            fullWidth
            sx={{
              maxWidth: "500px",
              backgroundColor: colors.blueAccent[500],
            }}
          >
            <InputLabel id="report-type-label" sx={{ color: "white" }}>
              Select Report Type
            </InputLabel>
            <Select
              labelId="report-type-label"
              value={selectedReportType}
              label="Select Report Type (for CSV only)"
              onChange={(e) => setSelectedReportType(e.target.value)}
              sx={{
                color: "white",
                ".MuiOutlinedInput-notchedOutline": { border: 0 },
                "& .MuiSvgIcon-root": { color: "white" },
              }}
            >
              <MenuItem value=""><em>None (XLSX sheets will be auto-detected)</em></MenuItem>
              {reportTables.map((table) => (
                <MenuItem key={table} value={table} sx={{ color: "white" }}>
                  {formatTableName(table)}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>
      </Box>

      {/* Upload Section */}
      <Box
        display="flex"
        flexDirection="column"
        alignItems="center"
        gap={4}
        mt={4}
      >
        <Box
          width="100%"
          bgcolor={colors.primary[400]}
          display="flex"
          padding={2}
          gap={2}
        >
          <Button
            variant="contained"
            color="secondary"
            sx={{ fontSize: "16px", py: "10px", flexGrow: 1 }}
            onClick={handleButtonClick}
          >
            Upload from Device
          </Button>

          <input
            type="file"
            accept=".xlsx, .csv"
            ref={fileInputRef}
            style={{ display: "none" }}
            onChange={handleFileChange}
          />
        </Box>

        {/* Preview Table - Show preview for the first detected sheet if multiple, or for the selected CSV type */}
        {Object.keys(parsedData).length > 0 && (
          <Box mt={2} width="100%" bgcolor={colors.primary[400]} p={2}>
            <Typography variant="h4" color={colors.greenAccent[500]} mb={2}>
              Preview: {fileName}
            </Typography>

            {Object.keys(parsedData).map((reportTypeKey) => (
              <Box key={reportTypeKey} mb={4}>
                <Typography variant="h5" color={colors.grey[100]} mb={1}>
                  {formatTableName(reportTypeKey)} Data Preview (First 5 Rows)
                </Typography>
                <Box
                  sx={{
                    overflowX: "auto",
                    maxHeight: "300px",
                    border: "1px solid #ccc",
                    borderRadius: "8px",
                    padding: "10px",
                  }}
                >
                  <table style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead>
                      <tr>
                        {/* Get keys from the first row of the specific reportTypeKey data for headers */}
                        {parsedData[reportTypeKey] && parsedData[reportTypeKey].length > 0 &&
                          Object.keys(parsedData[reportTypeKey][0]).map((key) => (
                            <th
                              key={key}
                              style={{
                                border: "1px solid #ddd",
                                padding: "8px",
                                background: "#222",
                                color: "#fff",
                              }}
                            >
                              {key}
                            </th>
                          ))}
                      </tr>
                    </thead>
                    <tbody>
                      {/* Slice the specific reportTypeKey data for rows */}
                      {parsedData[reportTypeKey] && parsedData[reportTypeKey].slice(0, 5).map((row, index) => (
                        <tr key={index}>
                          {/* Iterate over the keys of the first row to ensure consistent column order */}
                          {parsedData[reportTypeKey] && parsedData[reportTypeKey].length > 0 &&
                            Object.keys(parsedData[reportTypeKey][0]).map((key, i) => (
                              <td
                                key={i}
                                style={{
                                  border: "1px solid #ddd",
                                  padding: "8px",
                                  color: "#eee",
                                }}
                              >
                                {row[key]}
                              </td>
                            ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </Box>
              </Box>
            ))}

            {/* Action Buttons */}
            <Box display="flex" gap={2} mt={2}>
              <Button
                variant="contained"
                color="success"
                onClick={handleImport}
                disabled={Object.keys(parsedData).length === 0 || !selectedSE}
              >
                Import to Database
              </Button>
              <Button variant="outlined" color="error" onClick={handleCancel}>
                Cancel
              </Button>
            </Box>
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default Reports;