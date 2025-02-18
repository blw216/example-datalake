from pyspark.sql import DataFrame
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F
from typing import Any

class MarketDaily(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "MarketDaily"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        This transform calculates the total bets for each market and date.
        """
        num_partitions = kwargs.get("num_partitions")
        if num_partitions is not None:
            self.bets = self.bets.repartition(num_partitions)

        return (
            self.bets
            .withColumn("date", F.to_date(F.col("timestamp")))
            .groupBy("market", "date")
            .agg(F.sum(F.col("bet_amount")).alias("total_bets"))
        )
