import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class IMDBStudent20201019 {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: <movie-file> <rating-file> <k>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("IMDBStudent20201019")
                .getOrCreate();

        // 영화 데이터 읽기
        JavaRDD<String> movieLines = spark.read().textFile(args[0]).javaRDD();

        // 평점 데이터 읽기
        JavaRDD<String> ratingLines = spark.read().textFile(args[1]).javaRDD();

        // Fantasy 장르인 영화들을 필터링하기 위한 함수 정의
        Function<String, Boolean> fantasyFilter = line -> {
            String[] tokens = line.split("::");
            String genres = tokens[2];
            return genres.contains("Fantasy");
        };

        // movieLines RDD에서 Fantasy 장르인 영화들을 필터링하여 (movieId, title) 형태로 매핑
        JavaPairRDD<Integer, String> fantasyMovies = movieLines
                .filter(fantasyFilter)
                .mapToPair(line -> {
                    String[] tokens = line.split("::");
                    int movieId = Integer.parseInt(tokens[0]);
                    String title = tokens[1];
                    return new Tuple2<>(movieId, title);
                });

        // ratingLines RDD에서 (movieId, rating) 형태로 매핑
        JavaPairRDD<Integer, Double> movieRatings = ratingLines
                .mapToPair(line -> {
                    String[] tokens = line.split("::");
                    int movieId = Integer.parseInt(tokens[1]);
                    double rating = Double.parseDouble(tokens[2]);
                    return new Tuple2<>(movieId, rating);
                });

        // Fantasy 장르인 영화들의 (title, rating) 형태의 RDD 생성
        JavaPairRDD<String, Double> fantasyMoviesAvgRating = fantasyMovies
                // 영화의 평점 데이터를 결합
                .join(movieRatings)
                // 영화 제목을 key로 변경
                .mapToPair(tuple -> new Tuple2<>(tuple._2()._1(), tuple._2()._2()))
                // 중복된 평점 데이터를 제거하고, 해당 영화의 평균 평점 계산
                .groupByKey()
                .mapValues(ratings -> {
                    double sum = 0;
                    int count = 0;
                    for (Double rating : ratings) {
                        sum += rating;
                        count++;
                    }
                    return sum / count;
                });

        // 상위 k개의 Fantasy 영화 선택
        int k = Integer.parseInt(args[2]);
        List<Tuple2<String, Double>> topKFantasyMovies = fantasyMoviesAvgRating
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(k);

        // 결과 출력
        for (Tuple2<String, Double> movie : topKFantasyMovies) {
            System.out.println("(" + movie._1() + "," + movie._2() + ")");
        }

        spark.stop();
    }
}

