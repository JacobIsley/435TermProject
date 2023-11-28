package cs435;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import java.lang.Math;
import java.util.regex.Pattern;

public class ScoreTimeAnalysis {

    String gameMovesFile;
    String gameInfoFile;
    String outputFile;

    SparkSession spark;

    public ScoreTimeAnalysis(String gameMovesFile, String gameInfoFile, String outputFile) {
        this.gameMovesFile = gameMovesFile;
        this.gameInfoFile = gameInfoFile;
        this.outputFile = outputFile;
    }

    public int runSpark() {
        spark = SparkSession
                .builder()
                .appName("ScoreTimeAnalysis")
                .getOrCreate();
        analysis();
        spark.stop();
        return 0;
    }

    private void analysis() {
        /*
        Organize and filter move data
        Key = "game_id player"
        Value = (move_no, move_time, centipawn_score)
        The first move for each player in each game (move_no == 1 || move_no == 2) are filtered
        (The player's clock only begins after their first move)
         */
        JavaRDD<Row> rawGameMoves = spark.read().option("header", "true").csv(gameMovesFile).javaRDD();
        JavaPairRDD<String, Tuple3<Integer, Double, Double>> moveDataset = rawGameMoves.mapToPair(x -> {
            // If checkmate of x moves until checkmate (indicated by an "#" before the score, return a large number
            // Can be modified if need be
            int moveNumber = Integer.parseInt(x.getString(3));
            String scoreStr = x.getString(14);
            double centipawnScore;
            if (scoreStr.contains("#")) {
                centipawnScore = scoreStr.charAt(1) == '-' ? -100000.0 : 100000.0;
            } else {
                centipawnScore = Double.parseDouble(scoreStr);
            }
            // Invert score for black (OR WE CAN DIFFERENTIATE WHITE AND BLACK PLAYERS)
            centipawnScore = centipawnScore % 2 == 0 ? -1.0 * centipawnScore : centipawnScore;
            return new Tuple2<>(
                    x.getString(1) + " " + x.getString(2),
                    new Tuple3<>(moveNumber,
                            Double.parseDouble(x.getString(13)),
                            centipawnScore)
            );
        }).filter(x -> x._2._1() != 1 && x._2._1() != 2);

        /*
        Organize and filter player data
        Key = "game_id player"
        Value = (time, increment)
        */
        JavaRDD<Row> rawGameInfo = spark.read().option("header", "true").csv(gameInfoFile).javaRDD().cache();
        JavaPairRDD<String, Tuple2<Double, Double>> playerDataset = rawGameInfo.mapToPair(x -> {
            String[] timeControl = x.getString(22).split(Pattern.quote("+"));
            return new Tuple2<>(
                    x.getString(0) + " " + x.getString(6),
                    new Tuple2<>(Double.parseDouble(timeControl[0]), Double.parseDouble(timeControl[1])
            ));
        }).union(rawGameInfo.mapToPair((x -> {
            String[] timeControl = x.getString(22).split(Pattern.quote("+"));
            return new Tuple2<>(
                    x.getString(0) + " " + x.getString(7),
                    new Tuple2<>(Double.parseDouble(timeControl[0]), Double.parseDouble(timeControl[1])
            ));
        })));

        /*
        Join move data and player data on "game_id player"
        Key = "game_id player"
        Value = (time, increment, move_no, move_time, centipawn_score)
        Time-based values (time, increment, move_time) are in seconds
        */
        JavaPairRDD<String, Tuple5<Double, Double, Integer, Double, Double>> playerGameJoin =
                playerDataset.join(moveDataset).mapToPair(x -> {
                    Tuple2<Double, Double> playerData = x._2._1;
                    Tuple3<Integer, Double, Double> gameData = x._2._2;
                    return new Tuple2<>(x._1, new Tuple5<>(playerData._1, playerData._2, gameData._1(), gameData._2(), gameData._3()));
        }).cache();

        /* Conduct Analysis Based on Different Factors/Profiles
        moveToGameTimes format
        - Key = "game_id player"
        - Value = (move_time / time, centipawn_score)
         */
        JavaPairRDD<String, Tuple2<Double, Double>> moveToGameTimes = playerGameJoin.mapToPair(x -> {
            double relativeMoveTime = x._2._4() / x._2._1();
            return new Tuple2<>(x._1(), new Tuple2<>(relativeMoveTime, x._2._5()));
        }).cache();

        // Profile A: Move Time to Score (Relative to Game Time)
        JavaPairRDD<Double, Double> profileA = moveToGameTimes.mapToPair(x ->
                new Tuple2<>(x._2._1, x._2._2)
        );

        // TEMP: Check Max and Min score values
        /*
        double maxScore = profileA.reduceByKey(Math::max).values().reduce((Math::max));
        double minScore = profileA.reduceByKey(Math::min).values().reduce((Math::min));
        System.out.println(maxScore);
        System.out.println(minScore);
         */

        profileA.map(x -> x._1 + "," + x._2).coalesce(1).saveAsTextFile(outputFile + "_AllMoveTimes");

        // NOTE: Consider whether we should consider checkmate moves, games with less than 5 or 10 moves per player
        // Profile B: Top 5 Fastest Moves (Relative to Game Time)

        // Profile C: Top 5 Slowest Moves (Relative to Game Time)
    }

    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.err.println("Usage: RatingTimeAnalysis <game-moves-file> <game-info-file> <output-file>");
                System.exit(1);
            }
            System.out.println("Running Application: RatingTimeAnalysis");
            ScoreTimeAnalysis sparkProgram = new ScoreTimeAnalysis(args[0], args[1], args[2]);
            int exitCode = sparkProgram.runSpark();
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
