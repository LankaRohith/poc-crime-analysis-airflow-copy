package com.example.demo.service;

import com.example.demo.dto.CrimeStatsDto;
import com.example.demo.dto.WeaponStatsDto;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class SimpleCrimeAnalysisService {
    
    public List<CrimeStatsDto> getTop3LocationsForHighestCrimes() {
        long startTime = System.currentTimeMillis();
        
        Map<String, Long> locationCounts = new HashMap<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/Crime_Data_from_2020_to_Present.csv"))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 20) {
                    String location = values[20].trim(); // LOCATION column
                    if (!location.isEmpty()) {
                        locationCounts.put(location, locationCounts.getOrDefault(location, 0L) + 1);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV: " + e.getMessage());
            return new ArrayList<>();
        }
        
        List<CrimeStatsDto> result = locationCounts.entrySet().stream()
                .map(entry -> new CrimeStatsDto(entry.getKey(), entry.getValue()))
                .sorted((a, b) -> Long.compare(b.getCrimeCount(), a.getCrimeCount()))
                .limit(3)
                .collect(Collectors.toList());
        
        long endTime = System.currentTimeMillis();
        System.out.println("=== TOP 3 LOCATIONS FOR HIGHEST CRIMES ===");
        result.forEach(stat -> 
            System.out.println("Location: " + stat.getLocation() + ", Crime Count: " + stat.getCrimeCount()));
        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
        System.out.println();
        
        return result;
    }
    
    public List<WeaponStatsDto> getMostCommonWeapons() {
        long startTime = System.currentTimeMillis();
        
        Map<String, Long> weaponCounts = new HashMap<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/Crime_Data_from_2020_to_Present.csv"))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 16) {
                    String weapon = values[16].trim(); // Weapon Desc column
                    if (!weapon.isEmpty()) {
                        weaponCounts.put(weapon, weaponCounts.getOrDefault(weapon, 0L) + 1);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV: " + e.getMessage());
            return new ArrayList<>();
        }
        
        List<WeaponStatsDto> result = weaponCounts.entrySet().stream()
                .map(entry -> new WeaponStatsDto(entry.getKey(), entry.getValue()))
                .sorted((a, b) -> Long.compare(b.getUsageCount(), a.getUsageCount()))
                .limit(5)
                .collect(Collectors.toList());
        
        long endTime = System.currentTimeMillis();
        System.out.println("=== MOST COMMON WEAPONS ===");
        result.forEach(stat -> 
            System.out.println("Weapon: " + stat.getWeapon() + ", Usage Count: " + stat.getUsageCount()));
        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
        System.out.println();
        
        return result;
    }
    
    public List<CrimeStatsDto> getCrimeTypeStatistics() {
        long startTime = System.currentTimeMillis();
        
        Map<String, Long> crimeTypeCounts = new HashMap<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/Crime_Data_from_2020_to_Present.csv"))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 9) {
                    String crimeType = values[9].trim(); // Crm Cd Desc column
                    if (!crimeType.isEmpty()) {
                        crimeTypeCounts.put(crimeType, crimeTypeCounts.getOrDefault(crimeType, 0L) + 1);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV: " + e.getMessage());
            return new ArrayList<>();
        }
        
        List<CrimeStatsDto> result = crimeTypeCounts.entrySet().stream()
                .map(entry -> new CrimeStatsDto(entry.getKey(), entry.getValue()))
                .sorted((a, b) -> Long.compare(b.getCrimeCount(), a.getCrimeCount()))
                .limit(10)
                .collect(Collectors.toList());
        
        long endTime = System.currentTimeMillis();
        System.out.println("=== CRIME TYPE STATISTICS ===");
        result.forEach(stat -> 
            System.out.println("Crime Type: " + stat.getLocation() + ", Count: " + stat.getCrimeCount()));
        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
        System.out.println();
        
        return result;
    }
    
    public List<CrimeStatsDto> getCrimeByArea() {
        long startTime = System.currentTimeMillis();
        
        Map<String, Long> areaCounts = new HashMap<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/Crime_Data_from_2020_to_Present.csv"))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 5) {
                    String area = values[5].trim(); // AREA NAME column
                    if (!area.isEmpty()) {
                        areaCounts.put(area, areaCounts.getOrDefault(area, 0L) + 1);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV: " + e.getMessage());
            return new ArrayList<>();
        }
        
        List<CrimeStatsDto> result = areaCounts.entrySet().stream()
                .map(entry -> new CrimeStatsDto(entry.getKey(), entry.getValue()))
                .sorted((a, b) -> Long.compare(b.getCrimeCount(), a.getCrimeCount()))
                .collect(Collectors.toList());
        
        long endTime = System.currentTimeMillis();
        System.out.println("=== CRIMES BY AREA ===");
        result.forEach(stat -> 
            System.out.println("Area: " + stat.getLocation() + ", Count: " + stat.getCrimeCount()));
        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
        System.out.println();
        
        return result;
    }
    
    public void printSummaryStatistics() {
        long startTime = System.currentTimeMillis();
        
        int totalCrimes = 0;
        Set<String> uniqueLocations = new HashSet<>();
        Set<String> uniqueWeapons = new HashSet<>();
        Set<String> uniqueCrimeTypes = new HashSet<>();
        Set<String> uniqueAreas = new HashSet<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/Crime_Data_from_2020_to_Present.csv"))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 20) {
                    totalCrimes++;
                    String location = values[20].trim();
                    String weapon = values.length > 16 ? values[16].trim() : "";
                    String crimeType = values[9].trim();
                    String area = values[5].trim();
                    
                    if (!location.isEmpty()) uniqueLocations.add(location);
                    if (!weapon.isEmpty()) uniqueWeapons.add(weapon);
                    if (!crimeType.isEmpty()) uniqueCrimeTypes.add(crimeType);
                    if (!area.isEmpty()) uniqueAreas.add(area);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV: " + e.getMessage());
            return;
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("=== SUMMARY STATISTICS ===");
        System.out.println("Total number of crimes: " + totalCrimes);
        System.out.println("Number of unique locations: " + uniqueLocations.size());
        System.out.println("Number of unique weapons: " + uniqueWeapons.size());
        System.out.println("Number of unique crime types: " + uniqueCrimeTypes.size());
        System.out.println("Number of unique areas: " + uniqueAreas.size());
        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
        System.out.println();
    }
}
