# Analysis Software
Chess movetime analysis software available here:

## OutcomeTimeAnalysis.java
Used to collect total game time data as it relates to game outcomes (wins, losses, draws). Developed using IntelliJ and Apache Spark 

Example usage: spark-submit --class cs435.OutcomeTimeAnalysis --master spark://MASTER.cs.colostate.edu:PORT path-to/OutcomeTimeAnalysis-1.0-SNAPSHOT.jar /hdfs-path-to/move_data.csv /hdfs-path-to/game_info.csv /hdfs-path-to/OutputFolder

## RatingTimeAnalysis.java
Used to collect data comparing player rating to move time statistics (Average, Top N, etc.) Developed using IntelliJ and Apache Spark 

Example usage: spark-submit --class cs435.RatingTimeAnalysis --master spark://MASTER.cs.colostate.edu:PORT path-to/RatingTimeAnalysis-1.0-SNAPSHOT.jar /hdfs-path-to/move_data.csv /hdfs-path-to/game_info.csv /hdfs-path-to/OutputFolder

## ScoreTimeAnalysis.java
Used to collect data comparing move times to centipawn scores (Average, Top N, etc.) Developed using IntelliJ and Apache Spark 

Example usage: spark-submit --class cs435.ScoreTimeAnalysis --master spark://MASTER.cs.colostate.edu:PORT path-to/ScoreTimeAnalysis-1.0-SNAPSHOT.jar /hdfs-path-to/move_data.csv /hdfs-path-to/game_info.csv /hdfs-path-to/OutputFolder
