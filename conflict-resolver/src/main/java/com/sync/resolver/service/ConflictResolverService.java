package com.sync.resolver.service;

import com.sync.resolver.model.ChangeEvent;
import com.sync.resolver.model.ConflictRecord;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Objects;

@Service
public class ConflictResolverService {

    /**
     * Resolves a conflict between two ChangeEvent objects using a Last-Write-Wins (LWW) strategy based on SCN.
     * If SCNs are equal, it falls back to commit timestamp. If timestamps are also equal, the decision might be arbitrary
     * or based on a predefined branch priority (not implemented here).
     *
     * @param event1 The first change event.
     * @param event2 The second change event.
     * @return A ConflictRecord detailing the resolution, or null if no clear winner or conflict.
     */
    public ConflictRecord resolveConflict(ChangeEvent event1, ChangeEvent event2) {
        if (event1 == null || event2 == null) {
            return null; // Cannot resolve if one event is null
        }

        // Ensure events are for the same record (same schema, table, and primary key)
        if (!isSameRecord(event1, event2)) {
            // This should ideally not happen if events are grouped correctly by primary key
            return null; 
        }

        ChangeEvent winner = null;
        ChangeEvent loser = null;

        // LWW based on SCN
        if (event1.getScn() != null && event2.getScn() != null) {
            if (event1.getScn() > event2.getScn()) {
                winner = event1;
                loser = event2;
            } else if (event2.getScn() > event1.getScn()) {
                winner = event2;
                loser = event1;
            } else { // SCNs are equal, fall back to commit timestamp
                if (event1.getCommitTimestamp() != null && event2.getCommitTimestamp() != null) {
                    if (event1.getCommitTimestamp().isAfter(event2.getCommitTimestamp())) {
                        winner = event1;
                        loser = event2;
                    } else if (event2.getCommitTimestamp().isAfter(event1.getCommitTimestamp())) {
                        winner = event2;
                        loser = event1;
                    } else {
                        // Timestamps are also equal, arbitrary winner for now
                        winner = event1; 
                        loser = event2;
                    }
                } else { // One or both timestamps are null, arbitrary winner
                    winner = event1;
                    loser = event2;
                }
            }
        } else { // One or both SCNs are null, arbitrary winner for now
            winner = event1;
            loser = event2;
        }

        if (winner != null && loser != null && !Objects.equals(winner.getBranchId(), loser.getBranchId())) {
            return createConflictRecord(winner, loser);
        }
        return null; // No actual conflict or same branch update
    }

    private boolean isSameRecord(ChangeEvent event1, ChangeEvent event2) {
        return Objects.equals(event1.getSchemaName(), event2.getSchemaName()) &&
               Objects.equals(event1.getTableName(), event2.getTableName()) &&
               Objects.equals(event1.getPrimaryKey(), event2.getPrimaryKey());
    }

    private ConflictRecord createConflictRecord(ChangeEvent winner, ChangeEvent loser) {
        ConflictRecord record = new ConflictRecord();
        record.setConflictId(java.util.UUID.randomUUID().toString());
        record.setSchemaName(winner.getSchemaName());
        record.setTableName(winner.getTableName());
        record.setPrimaryKey(winner.getPrimaryKey() != null ? winner.getPrimaryKey().toString() : "N/A");
        record.setConflictTimestamp(LocalDateTime.now());
        record.setWinningBranch(winner.getBranchId());
        record.setLosingBranch(loser.getBranchId());
        record.setResolutionStrategy("LWW_SCN_TIMESTAMP");
        record.setDescription(String.format("Conflict resolved: %s (SCN: %d, Timestamp: %s) won over %s (SCN: %d, Timestamp: %s).",
                winner.getBranchId(), winner.getScn(), winner.getCommitTimestamp(),
                loser.getBranchId(), loser.getScn(), loser.getCommitTimestamp()));
        return record;
    }
}
