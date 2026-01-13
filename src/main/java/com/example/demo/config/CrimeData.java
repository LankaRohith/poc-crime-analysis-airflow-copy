package com.example.demo.config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CrimeData {
    @Autowired
    private SparkSession sparkSession;

    @Value("${crime.data.file-path}")
    private String filePath;


    @Bean
    public Dataset<Row> readDataset() {
        Dataset<Row> df = sparkSession.read()
                .option("header", "true")
                .csv(filePath);

        return df;
    }
}
