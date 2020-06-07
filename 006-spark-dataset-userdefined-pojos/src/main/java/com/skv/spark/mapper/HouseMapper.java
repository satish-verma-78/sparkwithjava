package com.skv.spark.mapper;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.skv.spark.pojos.House;
//Row data frame map to House 
public class HouseMapper implements MapFunction<Row, House> {

	private static final long serialVersionUID = 1L;

	@Override
	public House call(Row value) throws Exception {
		House house=new House();
		house.setId(value.getAs("id"));
		house.setAddress(value.getAs("address"));
		house.setPrice(value.getAs("price"));
		house.setSqft(value.getAs("sqft"));
		String date =value.getAs("vacantBy").toString();
		if(date!=null) {
			SimpleDateFormat parser=new SimpleDateFormat("yyyy-mm-dd");
			System.out.println(parser.parse(date));
			house.setVacantBy(parser.parse(date));
		}
		return house;
	}

}
