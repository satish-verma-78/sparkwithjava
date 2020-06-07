package com.skv.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;
public class Application {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkSession sparkSession=SparkSession.builder()
				.appName("Join 2 Data Sets")
				.master("local")
				.getOrCreate();

		Dataset<Row> durhamDf=getDurhamDataFrame(sparkSession);
		durhamDf.printSchema();
		durhamDf.show(5);

		Dataset<Row> recreationDf=getRereationDataFrame(sparkSession);
		recreationDf.printSchema();
		recreationDf.show(5);
		System.out.println(recreationDf.count());

		combineDataFrames(durhamDf,recreationDf);

	}


	private static void combineDataFrames(Dataset<Row> durhamDf, Dataset<Row> recreationDf) {
		// Match by column names using unionByName
		// if we just use union(), it matches the columns based on the order
		
		Dataset<Row> df=durhamDf.unionByName(recreationDf);
		df.show(500);
		df.printSchema();
		System.out.println("Total Records : "+df.count());
		
		
		// Partiotions : at minimum 1 for one data frame 
		Partition [] partions=df.rdd().getPartitions();
		System.out.println("Total partiotions : "+partions.length);
		
		
		// change the partiotions 
		
		df=df.repartition(5);
		String url="jdbc:postgresql://localhost:5432/sparkdemoDB";
		Properties props=new Properties();
		props.setProperty("driver", "org.postgresql.Driver");
		props.setProperty("user", "postgres");
		props.setProperty("password", "system");

		df.write()
		.mode(SaveMode.Overwrite)
		.jdbc(url, "Park_Data", props);

	}


	private static Dataset<Row> getDurhamDataFrame(SparkSession sparkSession) {

		Dataset<Row> df=sparkSession.read()
				.format("json")
				.option("multiline", true)
				.load("src/main/resources/durham-parks.json");

		df=df.withColumn("park_id", concat(df.col("datasetid"),lit("_"),df.col("fields.objectid"),lit("_Durham")))
				.withColumn("park_name", df.col("fields.park_name"))
				.withColumn("city", lit("Durham"))
				.withColumn("address", df.col("fields.address"))
				.withColumn("has_playground", df.col("fields.playground"))
				.withColumn("zipcode", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres") )
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1))
				.drop("datasetid")// drop columns after fetching data else Exception
				.drop("fields")
				.drop("geometry")
				.drop("record_timestamp")
				.drop("recordid");

		return df;
	}
	private static Dataset<Row> getRereationDataFrame(SparkSession sparkSession) {
		Dataset<Row> df= sparkSession.read().format("csv")
				.option("header", true)
				.option("multiline", true)
				.load("src/main/resources/philadelphia_recreations.csv");
		//df=df.filter(lower(df.col("USE_")).like("%park%"));
		//or pure sql
		df=df.filter("lower(USE_) like '%park%'");

		df=df.withColumn("park_id",concat(lit("phil_"),df.col("OBJECTID")))
				.withColumnRenamed("SITE_NAME","park_name")
				.withColumn("city", lit("philadelphia"))
				.withColumnRenamed("ADDRESS", "address")
				.withColumn("has_playground", lit("UNKNOWN"))
				.withColumnRenamed("ZIPCODE","zipcode")
				.withColumnRenamed("ACREAGE","land_in_acres")
				.withColumn("geoX", lit("UNKNOWN"))
				.withColumn("geoY", lit("UNKNOWN"))
				.drop("OBJECTID")
				.drop("ASSET_NAME")
				.drop("CHILD_OF")
				.drop("USE_")
				.drop("DESCRIPTION")
				.drop("SQ_FEET")
				.drop("ALLIAS")
				.drop("CHRONOLOGY")
				.drop("DATE_EDITED")
				.drop("EDITED_BY")
				.drop("OCCUPANT")
				.drop("TENANT")
				.drop("LABEL")
				.drop("TENANT")
				.drop("TYPE")
				.drop("NOTES");
		return df;
	}

}
