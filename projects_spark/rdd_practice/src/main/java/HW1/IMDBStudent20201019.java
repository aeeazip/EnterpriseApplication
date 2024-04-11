import scala.Tuple2;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public final  class IMDBStudent20201019 implements Serializable {

	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("Usage:  <in-file> <out-file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("IMDBStudent20201019")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD(); // 한줄한줄을 요소로 하는 RDD 생성
		FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				String[] str = s.split("::");
				return Arrays.asList(str[2].split("\\|")).iterator();
			}
		};
		JavaRDD<String> words = lines.flatMap(fmf); // Genre가 element인 RDD 생성

		PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s, 1);
			}
		};
		JavaPairRDD<String, Integer> ones = words.mapToPair(pf); // <Genre, 1> 형태인 PairRDD 생성

		Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		};
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2); // 같은 Key의 Value를 합
		counts.saveAsTextFile(args[1]);
		spark.stop();
	}
}
