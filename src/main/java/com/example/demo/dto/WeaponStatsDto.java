package com.example.demo.dto;

public class WeaponStatsDto {
    private String weapon;
    private Long usageCount;
    
    public WeaponStatsDto(String weapon, Long usageCount) {
        this.weapon = weapon;
        this.usageCount = usageCount;
    }
    
    public String getWeapon() {
        return weapon;
    }
    
    public void setWeapon(String weapon) {
        this.weapon = weapon;
    }
    
    public Long getUsageCount() {
        return usageCount;
    }
    
    public void setUsageCount(Long usageCount) {
        this.usageCount = usageCount;
    }
}
