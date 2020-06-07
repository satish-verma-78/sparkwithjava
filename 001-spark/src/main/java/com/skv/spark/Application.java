package com.skv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;
public class Application {
	public static void main(String[] args) {
		//Create a session 
		// Connect with master-node
		SparkSession spark=new SparkSession.Builder()
				.appName("")
				.master("local")
				.getOrCreate();

		//Get the data
		Dataset<Row> df=spark.read().format("csv")
				.option("header", true)
				.load("src/main/resources/name_and_comments.txt");
		//dataset.show();
		//dataset.show(3);//top 3 rows

		//Transformation , df is immutable
		df=df.withColumn("full_name",concat(df.col("first_name"),lit(" "),df.col("last_name")));

		//Transformation , df is immutable 
		//filter having comments containing number
		df=	df.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name"));

		df.show();

		
		// Load the Transformed date in Postgres DB
		String url="jdbc:postgresql://localhost:5432/sparkdemoDB";
		Properties props=new Properties();
		props.setProperty("driver", "org.postgresql.Driver");
		props.setProperty("user", "postgres");
		props.setProperty("password", "system");

		df.write()
		.mode(SaveMode.Overwrite)
		.jdbc(url, "People", props);
	}
}
