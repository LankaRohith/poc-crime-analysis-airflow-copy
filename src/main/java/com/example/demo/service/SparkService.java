package com.example.demo.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.expressions.Window.*;

import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

@Service
public class SparkService {

    @Autowired
    private Dataset<Row> df;

    private static final Logger logger = LoggerFactory.getLogger(SparkService.class);

    public String showCsv() {
        logger.info("Displaying first 20 rows of the DataFrame");
        df.show(20);
        long count = df.count();
        logger.info("Total number of rows: {}", count);
        return "No.of Rows = " + count;
    }

    public String countCrimesByArea() {
        long start = System.currentTimeMillis();
        logger.debug("Counting crimes by area");
        Dataset<Row> result = df.groupBy("AREA NAME").count();
        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Crime count by area completed in {} ms", (end - start));
        return "Crime count by area Using GroupBY. Time taken: " + (end - start) + " ms";
    }

    public String countCrimesByAreaUsingCache() {
        long start = System.currentTimeMillis();
        logger.debug("Selecting 'AREA NAME' column and caching the DataFrame");
        Dataset<Row> areaDf = df.select("AREA NAME").cache();
        logger.debug("Grouping by 'AREA NAME' and counting crimes");
        Dataset<Row> result = areaDf.groupBy("AREA NAME").count();
        result.show();
        long end = System.currentTimeMillis();
//        areaDf.unpersist();
        logger.debug("Crime count by area with caching completed in {} ms", (end - start));
        return "Crime count by area Using GroupBY & caching. Time taken: " + (end - start) + " ms";
    }


    public String countCrimesByAreaWithRDD() {
        long start = System.currentTimeMillis();
        logger.debug("Counting crimes by area using RDD reduceByKey");

        JavaPairRDD<String, Long> areaCounts = df.javaRDD()
                .mapToPair(row -> new scala.Tuple2<>(row.getAs("AREA NAME").toString(), 1L))
                .reduceByKey(Long::sum);

        List<Tuple2<String, Long>> result = areaCounts.collect();
        result.forEach(t -> logger.debug("{}: {}", t._1, t._2));

        long end = System.currentTimeMillis();
        logger.debug("Crime count by area using RDD completed in {} ms", (end - start));
        return "Crime count by area using RDD reduceByKey. Time taken: " + (end - start) + " ms";
    }


    public String topCrimeTypes() {
        long start = System.currentTimeMillis();
        logger.debug("Finding top 5 crime types by frequency");

        Dataset<Row> result = df.groupBy("Crm Cd Desc")
                                .count()
                                .orderBy(desc("count"))
                                .limit(5);
        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Top 5 crime types displayed. Time taken: {} ms", (end - start));
        return "Top 5 crime types displayed above. Time taken: " + (end - start) + " ms";
    }

    public String topCrimeTypesUsingSelect() {
        long start = System.currentTimeMillis();
        logger.debug("Finding top 5 crime types using select on only required column");

        Dataset<Row> result = df.select("Crm Cd Desc")
                                .groupBy("Crm Cd Desc")
                                .count()
                                .orderBy(desc("count"))
                                .limit(5);
        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Top 5 crime types (using select) displayed. Time taken: {} ms", (end - start));
        return "Top 5 crime types displayed above Using select on only required column. Time taken: " + (end - start) + " ms";
    }

    public String avgVictimAgeByCrime() {
        long start = System.currentTimeMillis();
        logger.debug("Calculating average victim age by crime type");

        Dataset<Row> result = df.groupBy("Crm Cd Desc")
                                .agg(avg("Vict Age").alias("AvgVictimAge"));
        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Average victim age by crime type displayed. Time taken: {} ms", (end - start));
        return "Average victim age by crime type displayed above. Time taken: " + (end - start) + " ms";
    }


    public String avgVictimAgeByCrimeOptimized() {
        long start = System.currentTimeMillis();
        logger.debug("Calculating optimized average victim age by crime type (filtering and repartitioning)");

        // Project only needed columns and filter out null ages
        Dataset<Row> projected = df.select("Crm Cd Desc", "Vict Age")
                                   .filter(col("Vict Age").isNotNull());

        Dataset<Row> repartitioned = projected.repartition(col("Crm Cd Desc"));

        Dataset<Row> result = repartitioned.groupBy("Crm Cd Desc")
                                           .agg(avg("Vict Age").alias("AvgVictimAge"));
        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Optimized average victim age by crime type displayed. Time taken: {} ms", (end - start));
        return "Average victim age by crime type Using Filtering before grouping. Time taken: " + (end - start) + " ms";
    }


    public String mostCommonWeaponByCrimeType() {
        long start = System.currentTimeMillis();
        logger.debug("Finding most common weapon by crime type");

        Dataset<Row> weaponCounts = df.groupBy("Crm Cd Desc", "Weapon Desc")
                                      .count();
        WindowSpec window = partitionBy("Crm Cd Desc").orderBy(desc("count"));

        Dataset<Row> ranked = weaponCounts.withColumn("rank", row_number().over(window));

        Dataset<Row> result = ranked.filter("rank = 1")
                                    .select("Crm Cd Desc", "Weapon Desc", "count");

        result.show(false);

        long end = System.currentTimeMillis();
        logger.debug("Most common weapon by crime type displayed. Time taken: {} ms", (end - start));
        return "Most common weapon by crime type displayed above. Time taken: " + (end - start) + " ms";
    }


    public String monthlyCrimeTrend(int year) {
        long start = System.currentTimeMillis();
        logger.debug("Calculating monthly crime trend for year {}", year);

        Dataset<Row> withMonth = df.withColumn(
                "Month",
                month(to_timestamp(df.col("DATE OCC"), "M/d/yyyy h:mm:ss a"))
        );

        Dataset<Row> filtered = withMonth.filter(
                year(to_timestamp(df.col("DATE OCC"), "M/d/yyyy h:mm:ss a")).equalTo(year)
        );

        Dataset<Row> result = filtered.groupBy("Month").count().orderBy("Month");
        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Monthly crime trend for year {} displayed. Time taken: {} ms", year, (end - start));
        return "Monthly crime trend for year " + year + " displayed above. Time taken: " + (end - start) + " ms";
    }


    public String crimeTrendByArea(String area) {
        long start = System.currentTimeMillis();
        logger.debug("Calculating monthly crime trend for area {}", area);

        Dataset<Row> filtered = df.filter(col("AREA NAME").equalTo(area));

        Dataset<Row> withMonth = filtered.withColumn(
                "Month",
                month(to_timestamp(filtered.col("DATE OCC"), "M/d/yyyy h:mm:ss a"))
        );

        Dataset<Row> result = withMonth.groupBy("Month")
                                       .count()
                                       .orderBy("Month");

        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Crime trend for area {} displayed. Time taken: {} ms", area, (end - start));
        return "Crime trend for area " + area + " displayed above. Time taken: " + (end - start) + " ms";
    }


    public String topLocations() {
        long start = System.currentTimeMillis();
        logger.debug("Finding top 3 locations with highest number of crimes");

        Dataset<Row> result = df.groupBy("LOCATION")
                .count()
                .orderBy(desc("count"))
                .limit(3);

        result.show(false);

        long end = System.currentTimeMillis();
        logger.debug("Top 3 locations displayed. Time taken: {} ms", (end - start));
        return "Top 3 locations with highest number of crimes displayed above. Time taken: " + (end - start) + " ms";
    }

    public String crimeCountByGenderAndArea() {
        long start = System.currentTimeMillis();
        logger.debug("Calculating crime count by victim gender and area");

        Dataset<Row> result = df.groupBy("AREA NAME", "Vict Sex")
                .count()
                .orderBy(col("AREA NAME"), desc("count"));

        result.show();

        long end = System.currentTimeMillis();
        logger.debug("Crime count by victim gender and area displayed. Time taken: {} ms", (end - start));
        return "Crime count by victim gender and area displayed above. Time taken: " + (end - start) + " ms";
    }
}
