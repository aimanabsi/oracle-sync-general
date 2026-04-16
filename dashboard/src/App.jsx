import React, { useState, useEffect } from 'react';
import './App.css';
import BranchStatus from './components/BranchStatus';
import SyncLag from './components/SyncLag';
import ConflictLog from './components/ConflictLog';

function App() {
  const [branches, setBranches] = useState([]);
  const [syncMetrics, setSyncMetrics] = useState({});
  const [conflicts, setConflicts] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch branch status
        const branchResponse = await fetch('/api/branches/status');
        const branchData = await branchResponse.json();
        setBranches(branchData);

        // Fetch sync metrics
        const metricsResponse = await fetch('/api/sync-status');
        const metricsData = await metricsResponse.json();
        setSyncMetrics(metricsData);

        // Fetch conflict logs (if available)
        const conflictResponse = await fetch('/api/conflicts');
        if (conflictResponse.ok) {
          const conflictData = await conflictResponse.json();
          setConflicts(conflictData);
        }
      } catch (error) {
        console.error('Error fetching data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 5000); // Refresh every 5 seconds
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return <div className="loading">Loading...</div>;
  }

  return (
    <div className="App">
      <header className="App-header">
        <h1>Oracle Sync General - Monitoring Dashboard</h1>
      </header>
      <div className="dashboard-container">
        <div className="dashboard-section">
          <BranchStatus branches={branches} />
        </div>
        <div className="dashboard-section">
          <SyncLag branches={branches} />
        </div>
        <div className="dashboard-section">
          <ConflictLog conflicts={conflicts} />
        </div>
      </div>
    </div>
  );
}

export default App;
