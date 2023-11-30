package cs435;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import java.util.regex.Pattern;

public class OutcomeTimeAnalysis {

    String gameMovesFile;
    String gameInfoFile;
    String outputFile;

    SparkSession spark;

    public OutcomeTimeAnalysis(String gameMovesFile, String gameInfoFile, String outputFile) {
        this.gameMovesFile = gameMovesFile;
        this.gameInfoFile = gameInfoFile;
        this.outputFile = outputFile;
    }

    public int runSpark() {
        spark = SparkSession
                .builder()
                .appName("OutcomeTimeAnalysis")
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
        JavaPairRDD<String, Tuple2<Integer, Double>> moveDataset = rawGameMoves.mapToPair(x -> {
            // If checkmate of x moves until checkmate (indicated by an "#" before the score, return a large number
            // Can be modified if need be
            int moveNumber = Integer.parseInt(x.getString(3));
            return new Tuple2<>(
                    x.getString(1) + " " + x.getString(2),
                    new Tuple2<>(moveNumber,
                            Double.parseDouble(x.getString(13)))
            );
        }).filter(x -> x._2._1() != 1 && x._2._1() != 2);

        /*
        Organize and filter player data
        Key = "game_id player"
        Value = (outcome, time, increment)
        */
        JavaRDD<Row> rawGameInfo = spark.read().option("header", "true").csv(gameInfoFile).javaRDD().cache();
        JavaPairRDD<String, Tuple3<String, Double, Double>> playerDataset = rawGameInfo.mapToPair(x -> {
            String outcome = "w" + x.getString(8);
            String[] timeControl = x.getString(22).split(Pattern.quote("+"));
            return new Tuple2<>(
                    x.getString(0) + " " + x.getString(6),
                    new Tuple3<>(outcome, Double.parseDouble(timeControl[0]), Double.parseDouble(timeControl[1])
            ));
        }).union(rawGameInfo.mapToPair((x -> {
            String outcome = "b" + x.getString(8);
            String[] timeControl = x.getString(22).split(Pattern.quote("+"));
            return new Tuple2<>(
                    x.getString(0) + " " + x.getString(7),
                    new Tuple3<>(outcome, Double.parseDouble(timeControl[0]), Double.parseDouble(timeControl[1])
            ));
        })));

        /*
        Join move data and player data on "game_id player"
        Key = "game_id player"
        Value = (outcome, time, increment, move_no, move_time)
        Time-based values (time, increment, move_time) are in seconds
        */
        JavaPairRDD<String, Tuple5<String, Double, Double, Integer, Double>> playerGameJoin =
                playerDataset.join(moveDataset).mapToPair(x -> {
                    Tuple3<String, Double, Double> playerData = x._2._1;
                    Tuple2<Integer, Double> gameData = x._2._2;
                    return new Tuple2<>(x._1, new Tuple5<>(playerData._1(), playerData._2(), playerData._3(), gameData._1, gameData._2));
                }).cache();

        /*
        Establish Six Categories, Calculate sum of move times relative to total game time
        Categories: (White Wins = "w1-0", Black Wins = "b1-0", White Loses = "w0-1", Black Loses = "b0-1",
        White Draws = "w1/2-1/2", Black Draws = "b1/2-1/2")
        Written Output to DFS (For Plotting Distribution):
        - For each player: outcome, average move time per game
        - For each player: outcome, sum of move times per game
         */

        // Profile A: Average move time per game
        JavaPairRDD<String, Double> relativeMoveTimes = playerGameJoin.mapToPair(x -> {
            String key = x._2._1() + " " + x._1;
            double relativeMoveTime = x._2._5() / x._2._2();
            return new Tuple2<>(key, relativeMoveTime);
        });

        JavaPairRDD<String, Double> averageMoveTimes = calculateAverages(relativeMoveTimes).mapToPair(x ->
                new Tuple2<>(x._1.split("\\s+")[0], x._2));

        averageMoveTimes.map(x -> x._1 + "," + x._2).coalesce(1).saveAsTextFile(outputFile + "_AverageMoveTimes");

        // Profile B: Sum of move times per game
        JavaPairRDD<String, Tuple5<String, Double, Double, Integer, Double>> moveTimeSums = playerGameJoin.mapToPair(
                x -> new Tuple2<>(x._1, new Tuple5<>(x._2._1(), x._2._2(), x._2._3(), 1, x._2._5()))
        ).reduceByKey((left, right) ->
                new Tuple5<>(left._1(), left._2(), left._3(), left._4() + right._4(), left._5() + right._5()));

        JavaPairRDD<String, Double> finalizedSumResults = moveTimeSums.mapToPair(x -> {
            double totalClockTime = x._2._2() + (x._2._3() * x._2._4());
            double normalizedSum = x._2._5() / totalClockTime;
            return new Tuple2<>(x._2._1(), normalizedSum);
        });

        finalizedSumResults.map(x -> x._1 + "," + x._2).coalesce(1).saveAsTextFile(outputFile + "_SumMoveTimes");
    }

    private JavaPairRDD<String, Double> calculateAverages(JavaPairRDD<String, Double> moveTimes) {
        // Aggregate by key to acquire sum and count, mapValue to calculate average
        return moveTimes.aggregateByKey(
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
                System.err.println("Usage: OutcomeTimeAnalysis <game-moves-file> <game-info-file> <output-file>");
                System.exit(1);
            }
            System.out.println("Running Application: OutcomeTimeAnalysis");
            OutcomeTimeAnalysis sparkProgram = new OutcomeTimeAnalysis(args[0], args[1], args[2]);
            int exitCode = sparkProgram.runSpark();
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
