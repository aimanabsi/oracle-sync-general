import React from 'react';

const ConflictLog = ({ conflicts }) => {
  return (
    <div className="conflict-log-card">
      <h3>Conflict Resolution Log</h3>
      {conflicts && conflicts.length > 0 ? (
        <div className="conflict-list">
          {conflicts.map((conflict) => (
            <div key={conflict.conflictId} className="conflict-item resolved">
              <div>
                <strong>{conflict.schemaName}.{conflict.tableName}</strong> - Primary Key: {conflict.primaryKey}
              </div>
              <div style={{ fontSize: '0.9rem', marginTop: '5px' }}>
                <span style={{ color: '#28a745', fontWeight: 'bold' }}>Winner: {conflict.winningBranch}</span>
                {' '}vs{' '}
                <span style={{ color: '#dc3545', fontWeight: 'bold' }}>Loser: {conflict.losingBranch}</span>
              </div>
              <div style={{ fontSize: '0.85rem', marginTop: '5px', color: '#666' }}>
                {conflict.description}
              </div>
              <div style={{ fontSize: '0.8rem', marginTop: '5px', color: '#999' }}>
                Resolved at: {new Date(conflict.conflictTimestamp).toLocaleString()}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div style={{ padding: '20px', textAlign: 'center', color: '#999' }}>
          No conflicts detected. System is operating normally.
        </div>
      )}
    </div>
  );
};

export default ConflictLog;
