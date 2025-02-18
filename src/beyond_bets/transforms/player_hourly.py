from pyspark.sql import DataFrame
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F
from typing import Any

class PlayerHourly(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "PlayerHourly"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        This transform calculates the total bets for each player, date and hour.
        """
        num_partitions = kwargs.get("num_partitions")
        if num_partitions is not None:
            self.bets = self.bets.repartition(num_partitions)

        return (
            self.bets
            .withColumn("date", F.to_date(F.col("timestamp")))
            .withColumn("hour", F.date_trunc("hour", F.col("timestamp")))
            .groupBy("player_id", "date", "hour")
            .agg(F.sum(F.col("bet_amount")).alias("total_bets"))
        )
