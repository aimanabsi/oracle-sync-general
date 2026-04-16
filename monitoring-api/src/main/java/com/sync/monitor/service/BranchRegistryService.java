package com.sync.monitor.service;

import com.sync.monitor.model.BranchStatus;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Service
public class BranchRegistryService {

    private final Random random = new Random();

    public List<BranchStatus> getAllBranchesStatus() {
        // In a real application, this would fetch data from a persistent store or Kafka topics
        // For now, returning mock data
        return Arrays.asList(
                new BranchStatus("branch1", "Branch 1 (New York)", getRandomStatus(), LocalDateTime.now().minusMinutes(random.nextInt(60)), "192.168.1.101", random.nextLong(3600)),
                new BranchStatus("branch2", "Branch 2 (London)", getRandomStatus(), LocalDateTime.now().minusMinutes(random.nextInt(120)), "192.168.1.102", random.nextLong(7200)),
                new BranchStatus("branch3", "Branch 3 (Tokyo)", getRandomStatus(), LocalDateTime.now().minusMinutes(random.nextInt(30)), "192.168.1.103", random.nextLong(1800))
        );
    }

    private String getRandomStatus() {
        int status = random.nextInt(10);
        if (status < 7) return "Online";
        if (status < 9) return "Degraded";
        return "Offline";
    }
}
