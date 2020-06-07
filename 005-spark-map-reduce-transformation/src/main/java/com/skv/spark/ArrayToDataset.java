package com.skv.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ArrayToDataset {

	public void start() {
		SparkSession spark=SparkSession.builder().appName("Array to Dataset<String>").master("local").getOrCreate();

		String [] stringArr= {"Java", "Spring","Hibernate", "Java", "Scala", "Spring"};
		List<String> data=Arrays.asList(stringArr);
		
		
	}

}
