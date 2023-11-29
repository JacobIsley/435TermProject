# OutcomeTimeAnalysis
Output Format:
PlayerOutcome,Sum of Move Times relative to Total Clock Time

Player Outcome Codes:
- w1-0 = White player who won
- b1-0 = Black player who lost
- w0-1 = White player who lost
- b0-1 = Black player who won
- w1/2-1/2 = White player who drew
- b1/2-1/2 = Black player who drew

Example Output: White Player lost, took 450 seconds to make 30 moves in 600 second game with 10 second increment (600+10):
- Total Time = 600 + (10 * 30) = 900 seconds
- Sum of Move Times relative to Total Time = 450 / 900 = 0.5
- Output: w0-1,0.5
