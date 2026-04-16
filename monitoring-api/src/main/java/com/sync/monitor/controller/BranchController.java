package com.sync.monitor.controller;

import com.sync.monitor.model.BranchStatus;
import com.sync.monitor.service.BranchRegistryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/branches")
public class BranchController {

    private final BranchRegistryService branchRegistryService;

    public BranchController(BranchRegistryService branchRegistryService) {
        this.branchRegistryService = branchRegistryService;
    }

    @GetMapping("/status")
    public List<BranchStatus> getAllBranchesStatus() {
        return branchRegistryService.getAllBranchesStatus();
    }
}
