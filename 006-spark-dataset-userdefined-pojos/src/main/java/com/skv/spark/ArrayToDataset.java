package com.skv.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ArrayToDataset {

	public void start() {
		SparkSession spark=SparkSession.builder().appName("Array to Dataset<String>").master("local").getOrCreate();

		String [] stringArr= {"Java", "Spring","Hibernate", "Java", "Scala", "Spring"};
		List<String> data=Arrays.asList(stringArr);
		
		Dataset<String> ds=spark.createDataset(data, Encoders.STRING());
		ds.show();
		// Map function 
		//ds=ds.map(new StringMapper(), Encoders.STRING());// using java <8
		ds=ds.map((MapFunction<String,String>) value->" Language:"+value, Encoders.STRING());// Mapper using Java 8 lambda
		ds.show();
		
		//Reduce Function 
		String reducedValues=ds.reduce(new StringReducer());
		System.out.println("reducedValues: "+reducedValues);
		ds.show();
	}
	
	//Mapper should be serialized
	static class StringMapper implements MapFunction<String, String>,Serializable{
		private static final long serialVersionUID = 1L;

		@Override
		public String call(String value) throws Exception {
			return " Language :"+value ;
		}
		
	}
	
	static class StringReducer implements ReduceFunction<String>, Serializable{

		private static final long serialVersionUID = 1L;

		@Override
		public String call(String v1, String v2) throws Exception {
			return v1+v2;
		}
		
	}

}
