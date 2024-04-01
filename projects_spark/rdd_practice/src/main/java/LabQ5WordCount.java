import scala.Tuple2;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public final  class LabQ5WordCount implements Serializable {

	public static void main(String[] args) throws Exception {
		/*
			실행 결과
			(Redistribution.,1)
			(by,21)
			(If,2)
			(an,6)
		 */
		if(args.length < 2) {
			System.out.println("Usage: JavaWordCount <in-file> <out-file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("LabQ5WordCount")
			.getOrCreate();

		// 한줄한줄을 요소로 하는 RDD 생성
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				return Arrays.asList(s.split(" ")).iterator();
			}
		};

		// 단어가 element인 RDD 생성
		JavaRDD<String> words = lines.flatMap(fmf);
		PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s, 1);
			}
		};

		// <단어, 1> 형태인 PairRDD 생성
		JavaPairRDD<String, Integer> ones = words.mapToPair(pf);
		Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		};

		// 같은 Key의 Value를 합
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
		counts.saveAsTextFile(args[1]);
		spark.stop();
	}
}
