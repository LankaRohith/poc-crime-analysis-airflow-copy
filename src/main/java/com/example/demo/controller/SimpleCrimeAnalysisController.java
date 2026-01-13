package com.example.demo.controller;

import com.example.demo.dto.CrimeStatsDto;
import com.example.demo.dto.WeaponStatsDto;
import com.example.demo.service.SimpleCrimeAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/simple-crimes")
public class SimpleCrimeAnalysisController {
    
    @Autowired
    private SimpleCrimeAnalysisService crimeAnalysisService;
    
    @GetMapping("/top-locations")
    public ResponseEntity<List<CrimeStatsDto>> getTop3Locations() {
        List<CrimeStatsDto> result = crimeAnalysisService.getTop3LocationsForHighestCrimes();
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/common-weapons")
    public ResponseEntity<List<WeaponStatsDto>> getMostCommonWeapons() {
        List<WeaponStatsDto> result = crimeAnalysisService.getMostCommonWeapons();
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/crime-types")
    public ResponseEntity<List<CrimeStatsDto>> getCrimeTypeStatistics() {
        List<CrimeStatsDto> result = crimeAnalysisService.getCrimeTypeStatistics();
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/by-area")
    public ResponseEntity<List<CrimeStatsDto>> getCrimeByArea() {
        List<CrimeStatsDto> result = crimeAnalysisService.getCrimeByArea();
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/summary")
    public ResponseEntity<String> getSummaryStatistics() {
        crimeAnalysisService.printSummaryStatistics();
        return ResponseEntity.ok("Summary statistics printed to console");
    }
}
