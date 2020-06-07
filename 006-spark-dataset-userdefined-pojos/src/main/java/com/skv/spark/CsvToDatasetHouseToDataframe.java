package com.skv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.SparkSession;

import com.skv.spark.mapper.HouseMapper;
import com.skv.spark.pojos.House;

public class CsvToDatasetHouseToDataframe {

	public void start() {

		SparkSession spark=SparkSession.builder()
				.appName("Csv to dataframe to Dataset<House> and back")
				.master("local")
				.getOrCreate();

		String csvFile="src/main/resources/houses.csv";

		Dataset<Row> df=spark.read().format("csv")
				.option("inferSchema", true)
				.option("header", true)
				.option("sep", ";")
				.load(csvFile);
		System.out.println("House Ingested in dataframe : ");
		df.show(10,100);
		df.printSchema();
		
		System.out.println("House Ingested in dataset : ");
		Dataset<House> dsHouse=df.map(new HouseMapper(), Encoders.bean(House.class));
		dsHouse.show(10,100);
		dsHouse.printSchema();
		
		System.out.println("************************** After converting  to Dataframe ");
		Dataset<Row> df2=dsHouse.toDF();
		df2=df2.withColumn("formatted_date", concat(df2.col("vacantBy.date"),lit("_"),df2.col("vacantBy.year")));
		df2.printSchema();
		df2.show(10,100);
		
	}

}
