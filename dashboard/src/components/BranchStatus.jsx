import React from 'react';

const BranchStatus = ({ branches }) => {
  const getStatusClass = (status) => {
    switch (status.toLowerCase()) {
      case 'online':
        return 'status-online';
      case 'offline':
        return 'status-offline';
      case 'degraded':
        return 'status-degraded';
      default:
        return '';
    }
  };

  return (
    <div className="branch-status-card">
      <h3>Branch Connectivity Status</h3>
      <table>
        <thead>
          <tr>
            <th>Branch Name</th>
            <th>IP Address</th>
            <th>Status</th>
            <th>Last Seen</th>
          </tr>
        </thead>
        <tbody>
          {branches && branches.length > 0 ? (
            branches.map((branch) => (
              <tr key={branch.branchId}>
                <td>{branch.name}</td>
                <td>{branch.ipAddress}</td>
                <td className={getStatusClass(branch.status)}>
                  {branch.status}
                </td>
                <td>{new Date(branch.lastSeen).toLocaleString()}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan="4">No branches available</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
};

export default BranchStatus;
