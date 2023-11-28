package cs435;

import java.lang.Double;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.codehaus.janino.Java;
import scala.*;

public class RatingTimeAnalysis {

    String gameMovesFile;
    String gameInfoFile;
    String outputFile;

    SparkSession spark;

    public RatingTimeAnalysis(String gameMovesFile, String gameInfoFile, String outputFile) {
        this.gameMovesFile = gameMovesFile;
        this.gameInfoFile = gameInfoFile;
        this.outputFile = outputFile;
    }

    public int runSpark() {
        spark = SparkSession
                .builder()
                .appName("RatingTimeAnalysis")
                .getOrCreate();
        analysis();
        spark.stop();
        return 0;
    }

    private void analysis() {
        /*
        Organize and filter move data
        Key = "game_id player"
        Value = (move_no, move_time)
        The first move for each player in each game (move_no == 1 || move_no == 2) are filtered
        (The player's clock only begins after their first move)
         */
        JavaRDD<Row> rawGameMoves = spark.read().option("header", "true").csv(gameMovesFile).javaRDD();
        JavaPairRDD<String, Tuple2<Integer, Double>> moveDataset = rawGameMoves.mapToPair(x -> new Tuple2<>(
                x.getString(1) + " " + x.getString(2),
                new Tuple2<>(Integer.parseInt(x.getString(3)), Double.parseDouble(x.getString(13)))
        )).filter(x -> x._2._1 != 1 && x._2._1 != 2);

        /*
        Organize and filter player data
        Key = "game_id player"
        Value = (rating, time, increment)
        */
        JavaRDD<Row> rawGameInfo = spark.read().option("header", "true").csv(gameInfoFile).javaRDD().cache();
        JavaPairRDD<String, Tuple3<Integer, Double, Double>> playerDataset = rawGameInfo.mapToPair(x -> {
            String[] timeControl = x.getString(22).split(Pattern.quote("+"));
            return new Tuple2<>(
                    x.getString(0) + " " + x.getString(6),
                    new Tuple3<>(Integer.parseInt(x.getString(9)), Double.parseDouble(timeControl[0]), Double.parseDouble(timeControl[1])
            ));
        }).union(rawGameInfo.mapToPair((x -> {
            String[] timeControl = x.getString(22).split(Pattern.quote("+"));
            return new Tuple2<>(
                    x.getString(0) + " " + x.getString(7),
                    new Tuple3<>(Integer.parseInt(x.getString(11)), Double.parseDouble(timeControl[0]), Double.parseDouble(timeControl[1])
            ));
        })));

        /*
        Join move data and player data on "game_id player"
        Key = "game_id player"
        Value = (rating, time, increment, move_no, move_time)
        Time-based values (time, increment, move_time) are in seconds
        */
        JavaPairRDD<String, Tuple5<Integer, Double, Double, Integer, Double>> playerGameJoin =
                playerDataset.join(moveDataset).mapToPair(x -> {
                    Tuple3<Integer, Double, Double> playerData = x._2._1;
                    Tuple2<Integer, Double> gameData = x._2._2;
                    return new Tuple2<>(x._1, new Tuple5<>(playerData._1(), playerData._2(), playerData._3(), gameData._1, gameData._2));
        }).cache();

        /* Conduct Analysis Based on Different Factors/Profiles
        moveToGameTimes format
        - Key = "game_id player rating"
        - Value = move_time / time
         */
        JavaPairRDD<String, Double> moveToGameTimes = playerGameJoin.mapToPair(x -> {
            double relativeMoveTime = x._2._5() / x._2._2();
            return new Tuple2<>(x._1() + " " + x._2._1(), relativeMoveTime);
        }).cache();
        moveToGameTimes.map(x -> x._1.split(" ")[2] + "," + x._2).coalesce(1).saveAsTextFile(outputFile + "_AllMoveTimes");

        // Profile A: Average Move Time (Relative to Game Time)
        JavaPairRDD<String, Double> profileA = calculateAverage(moveToGameTimes).mapToPair(
                x -> new Tuple2<>(x._1.split(" ")[2], x._2));
        profileA.map(x -> x._1 + "," + x._2).coalesce(1).saveAsTextFile(outputFile + "_AverageMoveTime");

        // Profile B: Top 5 Fastest Moves (Relative to Game Time?)

        // Profile C: Top 5 Slowest Moves (Relative to Game Time?)

    }

    private JavaPairRDD<String, Double> calculateAverage(JavaPairRDD<String, Double> relativeMoveTimes) {
        // Aggregate by key to acquire sum and count, mapValue to calculate average
        return relativeMoveTimes.aggregateByKey(
                new Tuple2<>(0.0, 0),
                (accumulator, moveTime) -> {
                    accumulator = new Tuple2<>(accumulator._1 + moveTime, accumulator._2 + 1);
                    return accumulator;
                },
                (acc1, acc2) -> {
                    acc1 = new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2);
                    return acc1;
                }
        ).mapValues(x -> x._1 / x._2);
    }

    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.err.println("Usage: RatingTimeAnalysis <game-moves-file> <game-info-file> <output-file>");
                System.exit(1);
            }
            System.out.println("Running Application: RatingTimeAnalysis");
            RatingTimeAnalysis sparkProgram = new RatingTimeAnalysis(args[0], args[1], args[2]);
            int exitCode = sparkProgram.runSpark();
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
