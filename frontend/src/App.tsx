import React, { useState, useEffect } from 'react';
import { Container, Typography, Paper, FormControl, InputLabel, Select, MenuItem, TextField, Button, Box, Stepper, Step, StepLabel, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Checkbox, FormGroup, FormControlLabel, CircularProgress, Snackbar, Alert } from '@mui/material';
import axios from 'axios';

const sourceTypes = [
  { value: 'CLICKHOUSE', label: 'ClickHouse' },
  { value: 'FLATFILE', label: 'Flat File' },
];

const targetTypes = [
  { value: 'CLICKHOUSE', label: 'ClickHouse' },
  { value: 'FLATFILE', label: 'Flat File' },
];

function App() {
  const [sourceType, setSourceType] = useState('CLICKHOUSE');
  const [targetType, setTargetType] = useState('FLATFILE');
  const [status, setStatus] = useState('Idle');
  const [result, setResult] = useState('');
  const [activeStep, setActiveStep] = useState(0);
  const steps = ['Select Source/Target', 'Configure Connection', 'Select Table/Columns', 'Preview & Ingest'];

  // ClickHouse connection state
  const [chSource, setChSource] = useState({
    host: '',
    port: '',
    database: '',
    username: '',
    jwtToken: '',
  });
  const [chTarget, setChTarget] = useState({
    host: '',
    port: '',
    database: '',
    username: '',
    jwtToken: '',
  });

  // Flat File connection state
  const [ffSource, setFfSource] = useState({
    filePath: '',
    delimiter: ',',
    hasHeader: true,
  });
  const [ffTarget, setFfTarget] = useState({
    filePath: '',
    delimiter: ',',
    hasHeader: true,
  });

  // Table and column selection state
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [columns, setColumns] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [previewData, setPreviewData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // Validation for connection params
  const validateConnectionParams = () => {
    if (sourceType === 'CLICKHOUSE') {
      const { host, port, database, username, jwtToken } = chSource;
      if (!host || !port || !database || !username || !jwtToken) return false;
    } else {
      const { filePath, delimiter } = ffSource;
      if (!filePath || !delimiter) return false;
    }
    if (targetType === 'CLICKHOUSE') {
      const { host, port, database, username, jwtToken } = chTarget;
      if (!host || !port || !database || !username || !jwtToken) return false;
    } else {
      const { filePath, delimiter } = ffTarget;
      if (!filePath || !delimiter) return false;
    }
    return true;
  };

  // Handlers for input changes
  const handleChSourceChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setChSource({ ...chSource, [e.target.name]: e.target.value });
  };
  const handleChTargetChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setChTarget({ ...chTarget, [e.target.name]: e.target.value });
  };
  const handleFfSourceChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFfSource({ ...ffSource, [e.target.name]: e.target.value });
  };
  const handleFfTargetChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFfTarget({ ...ffTarget, [e.target.name]: e.target.value });
  };

  // Fetch tables if source is ClickHouse
  useEffect(() => {
    if (sourceType === 'CLICKHOUSE' && activeStep === 2) {
      setLoading(true);
      axios.post('/api/v1/clickhouse/tables', chSource)
        .then(response => {
          setTables(response.data);
          setLoading(false);
        })
        .catch(err => {
          setError('Failed to fetch tables');
          setLoading(false);
        });
    }
  }, [sourceType, activeStep, chSource]);

  // Fetch columns when a table is selected
  useEffect(() => {
    if (selectedTable && sourceType === 'CLICKHOUSE') {
      setLoading(true);
      axios.post('/api/v1/clickhouse/columns', chSource, { params: { tableName: selectedTable } })
        .then(response => {
          setColumns(response.data);
          setSelectedColumns([]);
          setLoading(false);
        })
        .catch(err => {
          setError('Failed to fetch columns');
          setLoading(false);
        });
    }
  }, [selectedTable, sourceType, chSource]);

  // Fetch preview data when columns are selected
  useEffect(() => {
    if (selectedColumns.length > 0 && sourceType === 'CLICKHOUSE') {
      setLoading(true);
      axios.post('/api/v1/clickhouse/preview', chSource, { params: { tableName: selectedTable, columns: selectedColumns, limit: 100 } })
        .then(response => {
          setPreviewData(response.data);
          setLoading(false);
        })
        .catch(err => {
          setError('Failed to fetch preview data');
          setLoading(false);
        });
    }
  }, [selectedColumns, selectedTable, sourceType, chSource]);

  // Handle column selection
  const handleColumnChange = (column: string) => {
    setSelectedColumns(prev => 
      prev.includes(column) ? prev.filter(c => c !== column) : [...prev, column]
    );
  };

  // Handle ingestion start
  const handleStartIngestion = () => {
    setStatus('Ingesting...');
    setLoading(true);
    const request = {
      sourceType,
      targetType,
      clickHouseConfig: sourceType === 'CLICKHOUSE' ? chSource : chTarget,
      flatFileConfig: sourceType === 'FLATFILE' ? ffSource : ffTarget,
      selectedColumns,
      tableName: selectedTable,
    };
    axios.post('/api/v1/ingest', request)
      .then(response => {
        setStatus('Completed');
        setResult(`Records processed: ${response.data.recordsProcessed}`);
        setLoading(false);
      })
      .catch(err => {
        setStatus('Failed');
        setResult(err.message);
        setLoading(false);
      });
  };

  return (
    <Container maxWidth="md" sx={{ py: 4 }}>
      <Typography variant="h4" gutterBottom align="center">
        ClickHouse & Flat File Ingestion Tool
      </Typography>
      <Paper elevation={3} sx={{ p: 3 }}>
        <Stepper activeStep={activeStep} alternativeLabel>
          {steps.map((label) => (
            <Step key={label}>
              <StepLabel>{label}</StepLabel>
            </Step>
          ))}
        </Stepper>
        <Box sx={{ mt: 4 }}>
          {/* Step 1: Source/Target Selection */}
          {activeStep === 0 && (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, justifyContent: 'center' }}>
              <Box sx={{ width: { xs: '100%', sm: 'calc(50% - 8px)' } }}>
                <FormControl fullWidth>
                  <InputLabel>Source</InputLabel>
                  <Select
                    value={sourceType}
                    label="Source"
                    onChange={(e) => setSourceType(e.target.value)}
                  >
                    {sourceTypes.map((type) => (
                      <MenuItem key={type.value} value={type.value}>{type.label}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Box>
              <Box sx={{ width: { xs: '100%', sm: 'calc(50% - 8px)' } }}>
                <FormControl fullWidth>
                  <InputLabel>Target</InputLabel>
                  <Select
                    value={targetType}
                    label="Target"
                    onChange={(e) => setTargetType(e.target.value)}
                  >
                    {targetTypes.map((type) => (
                      <MenuItem key={type.value} value={type.value}>{type.label}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Box>
              <Box sx={{ width: '100%' }}>
                <Button variant="contained" color="primary" onClick={() => setActiveStep(1)}>
                  Next
                </Button>
              </Box>
            </Box>
          )}

          {/* Step 2: Connection Parameters */}
          {activeStep === 1 && (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
              <Box sx={{ width: { xs: '100%', md: 'calc(50% - 8px)' } }}>
                <Paper variant="outlined" sx={{ p: 2 }}>
                  <Typography variant="subtitle1" gutterBottom>Source: {sourceType}</Typography>
                  {sourceType === 'CLICKHOUSE' ? (
                    <>
                      <TextField label="Host" name="host" value={chSource.host} onChange={handleChSourceChange} fullWidth margin="normal" required />
                      <TextField label="Port" name="port" value={chSource.port} onChange={handleChSourceChange} fullWidth margin="normal" required />
                      <TextField label="Database" name="database" value={chSource.database} onChange={handleChSourceChange} fullWidth margin="normal" required />
                      <TextField label="Username" name="username" value={chSource.username} onChange={handleChSourceChange} fullWidth margin="normal" required />
                      <TextField label="JWT Token" name="jwtToken" value={chSource.jwtToken} onChange={handleChSourceChange} fullWidth margin="normal" required />
                    </>
                  ) : (
                    <>
                      <TextField label="File Path" name="filePath" value={ffSource.filePath} onChange={handleFfSourceChange} fullWidth margin="normal" required />
                      <TextField label="Delimiter" name="delimiter" value={ffSource.delimiter} onChange={handleFfSourceChange} fullWidth margin="normal" required />
                      <FormControl fullWidth margin="normal">
                        <InputLabel shrink>Has Header</InputLabel>
                        <Select
                          name="hasHeader"
                          value={ffSource.hasHeader ? 'true' : 'false'}
                          onChange={e => setFfSource({ ...ffSource, hasHeader: e.target.value === 'true' })}
                        >
                          <MenuItem value="true">Yes</MenuItem>
                          <MenuItem value="false">No</MenuItem>
                        </Select>
                      </FormControl>
                    </>
                  )}
                </Paper>
              </Box>
              <Box sx={{ width: { xs: '100%', md: 'calc(50% - 8px)' } }}>
                <Paper variant="outlined" sx={{ p: 2 }}>
                  <Typography variant="subtitle1" gutterBottom>Target: {targetType}</Typography>
                  {targetType === 'CLICKHOUSE' ? (
                    <>
                      <TextField label="Host" name="host" value={chTarget.host} onChange={handleChTargetChange} fullWidth margin="normal" required />
                      <TextField label="Port" name="port" value={chTarget.port} onChange={handleChTargetChange} fullWidth margin="normal" required />
                      <TextField label="Database" name="database" value={chTarget.database} onChange={handleChTargetChange} fullWidth margin="normal" required />
                      <TextField label="Username" name="username" value={chTarget.username} onChange={handleChTargetChange} fullWidth margin="normal" required />
                      <TextField label="JWT Token" name="jwtToken" value={chTarget.jwtToken} onChange={handleChTargetChange} fullWidth margin="normal" required />
                    </>
                  ) : (
                    <>
                      <TextField label="File Path" name="filePath" value={ffTarget.filePath} onChange={handleFfTargetChange} fullWidth margin="normal" required />
                      <TextField label="Delimiter" name="delimiter" value={ffTarget.delimiter} onChange={handleFfTargetChange} fullWidth margin="normal" required />
                      <FormControl fullWidth margin="normal">
                        <InputLabel shrink>Has Header</InputLabel>
                        <Select
                          name="hasHeader"
                          value={ffTarget.hasHeader ? 'true' : 'false'}
                          onChange={e => setFfTarget({ ...ffTarget, hasHeader: e.target.value === 'true' })}
                        >
                          <MenuItem value="true">Yes</MenuItem>
                          <MenuItem value="false">No</MenuItem>
                        </Select>
                      </FormControl>
                    </>
                  )}
                </Paper>
              </Box>
              <Box sx={{ width: '100%', mt: 2 }}>
                <Button
                  variant="contained"
                  color="primary"
                  onClick={() => setActiveStep(2)}
                  disabled={!validateConnectionParams()}
                >
                  Next
                </Button>
              </Box>
            </Box>
          )}

          {/* Step 3: Table/Column Selection */}
          {activeStep === 2 && (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
              <Box sx={{ width: { xs: '100%', md: 'calc(50% - 8px)' } }}>
                <FormControl fullWidth margin="normal">
                  <InputLabel>Select Table</InputLabel>
                  <Select
                    value={selectedTable}
                    label="Select Table"
                    onChange={(e) => setSelectedTable(e.target.value)}
                  >
                    {tables.map((table) => (
                      <MenuItem key={table} value={table}>{table}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <FormGroup>
                  {columns.map((column) => (
                    <FormControlLabel
                      key={column}
                      control={<Checkbox checked={selectedColumns.includes(column)} onChange={() => handleColumnChange(column)} />}
                      label={column}
                    />
                  ))}
                </FormGroup>
              </Box>
              <Box sx={{ width: { xs: '100%', md: 'calc(50% - 8px)' } }}>
                <Typography>Flat File source selected. Proceed to preview.</Typography>
              </Box>
            </Box>
          )}

          {/* Step 4: Preview & Ingest */}
          {activeStep === 3 && (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
              <Box sx={{ width: { xs: '100%', md: 'calc(50% - 8px)' } }}>
                <Typography variant="h6" gutterBottom>Preview & Ingestion</Typography>
                {previewData.length > 0 && (
                  <TableContainer component={Paper} sx={{ mb: 2 }}>
                    <Table>
                      <TableHead>
                        <TableRow>
                          {selectedColumns.map((column) => (
                            <TableCell key={column}>{column}</TableCell>
                          ))}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {previewData.map((row, index) => (
                          <TableRow key={index}>
                            {selectedColumns.map((column) => (
                              <TableCell key={column}>{row[column]}</TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                )}
              </Box>
              <Box sx={{ width: { xs: '100%', md: 'calc(50% - 8px)' } }}>
                <Box sx={{ my: 2 }}>
                  <Button variant="outlined" onClick={() => setActiveStep(2)} sx={{ mr: 2 }}>Back</Button>
                  <Button variant="contained" color="success" onClick={handleStartIngestion} disabled={loading}>
                    Start Ingestion
                  </Button>
                </Box>
                {/* Status and result display */}
                <Box sx={{ mt: 2 }}>
                  <Typography>Status: {status}</Typography>
                  <Typography>Result: {result}</Typography>
                </Box>
              </Box>
            </Box>
          )}
        </Box>
      </Paper>
      {loading && <CircularProgress />}
      <Snackbar open={!!error} autoHideDuration={6000} onClose={() => setError('')}>
        <Alert severity="error">{error}</Alert>
      </Snackbar>
    </Container>
  );
}

export default App;
