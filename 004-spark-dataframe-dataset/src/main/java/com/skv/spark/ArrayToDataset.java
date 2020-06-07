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
		
		//Create a dataset
		Dataset<String> ds=spark.createDataset(data, Encoders.STRING());
		ds.printSchema();
		ds.show();
		
		Dataset<Row> df=ds.groupBy("value").count();// to dataframe
		df.show();
		
		//dataset to DataFrame, aother way 
		Dataset<Row> df1=ds.toDF();
		df1.printSchema();	
		df1.show();
		
		//Data Frame to Data Set
		Dataset<String> ds2=df1.as(Encoders.STRING());
		
	}

}
