from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from datetime import datetime, timedelta
from typing import Any


class TopPlayers(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "TopPlayers"
        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        This transform calculates the top 1% of players by total spend
        over the user defined time period, defaulting to the past week.
        """
        # Get the current date if one is not provided 
        # and calculate the date for one week ago
        end_date = kwargs.get("end_date", datetime.now())

        start_date = kwargs.get("start_date", end_date - timedelta(days=7))

        num_partitions = kwargs.get("num_partitions")
        if num_partitions is not None:
            self.bets = self.bets.repartition(num_partitions)

        # Filter bets for the past week and group by player_id
        player_spend = (
            self.bets
            .filter(F.col("timestamp").between(start_date, end_date))
            .groupBy("player_id")
            .agg(F.sum("bet_amount").alias("total_spend"))
        )

        # Calculate the threshold for the top 1% of players using approxQuantile,
        # which is fast and a good approximation for the 99th percentile.
        top_1p_threshold = player_spend.approxQuantile("total_spend", [0.99], 0.01)[0]

        # Filter to get the top 1% of players
        top_1p_players = player_spend.filter(F.col("total_spend") >= top_1p_threshold) 

        return top_1p_players