package com.example.demo.dto;

public class CrimeStatsDto {
    private String location;
    private Long crimeCount;
    
    public CrimeStatsDto(String location, Long crimeCount) {
        this.location = location;
        this.crimeCount = crimeCount;
    }
    
    public String getLocation() {
        return location;
    }
    
    public void setLocation(String location) {
        this.location = location;
    }
    
    public Long getCrimeCount() {
        return crimeCount;
    }
    
    public void setCrimeCount(Long crimeCount) {
        this.crimeCount = crimeCount;
    }
}
