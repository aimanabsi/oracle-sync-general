import React from 'react';

const SyncLag = ({ branches }) => {
  const formatLagTime = (seconds) => {
    if (!seconds) return 'N/A';
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
    return `${Math.floor(seconds / 3600)}h`;
  };

  const getLagColor = (seconds) => {
    if (!seconds) return '#999';
    if (seconds < 300) return '#28a745'; // Green - good
    if (seconds < 1800) return '#ffc107'; // Yellow - warning
    return '#dc3545'; // Red - critical
  };

  return (
    <div className="sync-lag-card">
      <h3>Synchronization Lag per Branch</h3>
      <table>
        <thead>
          <tr>
            <th>Branch Name</th>
            <th>Sync Lag</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {branches && branches.length > 0 ? (
            branches.map((branch) => (
              <tr key={branch.branchId}>
                <td>{branch.name}</td>
                <td>
                  <span style={{ color: getLagColor(branch.syncLagSeconds), fontWeight: 'bold' }}>
                    {formatLagTime(branch.syncLagSeconds)}
                  </span>
                </td>
                <td>
                  {branch.syncLagSeconds < 300 && '✓ Good'}
                  {branch.syncLagSeconds >= 300 && branch.syncLagSeconds < 1800 && '⚠ Warning'}
                  {branch.syncLagSeconds >= 1800 && '✗ Critical'}
                </td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan="3">No branches available</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
};

export default SyncLag;
