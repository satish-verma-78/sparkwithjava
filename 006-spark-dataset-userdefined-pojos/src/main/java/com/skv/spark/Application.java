package com.skv.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
public class Application {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		//ArrayToDataset app=new ArrayToDataset();
		//app.start();
		
		CsvToDatasetHouseToDataframe app = new CsvToDatasetHouseToDataframe();
		app.start();

	}
}
