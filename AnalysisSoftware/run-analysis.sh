#!/bin/bash
if [[ $# != 2 ]];
then
    echo "Uh-oh! Make sure to include the type (Rating/Score/Outcome) and month in that order. (Warning: Case-sensitive)"
else
    type=$1
    month=$2
    movefile="2020${month}_moves.csv"
    gamefile="fics2020${month}_game_info.csv"
    outdir="${month}OutcomeAnalysis"
    spark-submit --class cs435.${type}TimeAnalysis --master spark://charleston.cs.colostate.edu:30335 ~/TermProject/${type}TimeAnalysis-1.0-SNAPSHOT.jar /TermProject/Input/$movefile /TermProject/Input/$gamefile /TermProject/Output/$outdir
fi

