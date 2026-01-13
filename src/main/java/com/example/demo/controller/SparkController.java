package com.example.demo.controller;

import com.example.demo.service.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/spark")
public class SparkController {
    @Autowired
    private SparkService sparkService;

    @GetMapping("/process")
    public String process() {
        return sparkService.showCsv();
    }

    @GetMapping("/countByArea")
    public String countByArea() {
        return sparkService.countCrimesByArea() + "\n" +
               sparkService.countCrimesByAreaUsingCache() + "\n" +
               sparkService.countCrimesByAreaWithRDD();
    }

    @GetMapping("/topCrimeTypes")
    public String topCrimeTypes() {
        return sparkService.topCrimeTypes() + "\n" +
               sparkService.topCrimeTypesUsingSelect();
    }

    @GetMapping("/avgVictimAgeByCrime")
    public String avgVictimAgeByCrime() {
        return sparkService.avgVictimAgeByCrime() + "\n" +
               sparkService.avgVictimAgeByCrimeOptimized();
    }

    @GetMapping("/mostCommonWeaponByCrimeType")
    public String mostCommonWeaponByCrimeType() {
        return sparkService.mostCommonWeaponByCrimeType();
    }

    @GetMapping("/crimesInYear")
    public String crimesInYear(@RequestParam int year) {
        return sparkService.monthlyCrimeTrend(year);
    }

    @GetMapping("/crimeTrendByArea")
    public String crimeTrendByArea(@RequestParam String area) {
        return sparkService.crimeTrendByArea(area);
    }

    @GetMapping("/topLocations")
    public String topLocations() {
        return sparkService.topLocations();
    }

    @GetMapping("/crimeCountByGenderAndArea")
    public String crimeCountByGenderAndArea() {
        return sparkService.crimeCountByGenderAndArea();
    }
}
