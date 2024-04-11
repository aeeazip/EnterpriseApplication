import scala.Tuple2;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import java.time.DayOfWeek;
import java.time.LocalDate;

public final class UBERStudent20201019 implements Serializable {

	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("Usage:  <in-file> <out-file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("UBERStudent20201019")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD(); // 한줄한줄을 요소로 하는 RDD 생성
		PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] str = s.split(",");
				String[] date = str[1].split("/");
			
				int year = Integer.parseInt(date[2]);
				int month = Integer.parseInt(date[0]);
				int day = Integer.parseInt(date[1]);
			
				LocalDate localDate = LocalDate.of(year, month, day);
				DayOfWeek dayOfWeek = localDate.getDayOfWeek();
				int dayOfWeekNumber = dayOfWeek.getValue();
				String[] today = {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"};

				return new Tuple2(str[0] + "/" + today[dayOfWeekNumber - 1], str[2] + "/" +  str[3]);
			}
		};
		JavaPairRDD<String, String> words = lines.mapToPair(pf);

		Function2<String, String, String> f2 = new Function2<String, String, String>() {
			public String call(String x, String y) {
				String[] str1 = x.split("/");
				String[] str2 = y.split("/");

				String vehicles = String.valueOf(Integer.parseInt(str1[0]) + Integer.parseInt(str2[0]));
				String trips = String.valueOf(Integer.parseInt(str1[1]) + Integer.parseInt(str2[1]));

				return vehicles + "/" + trips;
			}
		};
		JavaPairRDD<String, String> counts = words.reduceByKey(f2); // 같은 Key의 Value를 합
		counts.saveAsTextFile(args[1]);
		spark.stop();
	}
}
