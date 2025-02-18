from pyspark.sql import DataFrame
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Any

class BetGrader(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "BetGrader"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        This transform grades the bets by the average bet amount over the last 15 minutes.
        """
        num_partitions = kwargs.get("num_partitions")
        if num_partitions is not None:
            self.bets = self.bets.repartition(num_partitions)

        self.bets = self.bets.withColumn("unix_timestamp", F.unix_timestamp(F.col("timestamp")))

        window_spec = Window.partitionBy("market").orderBy("unix_timestamp").rangeBetween(-900, 0)

        self.bets = self.bets.withColumn("grade", F.round(F.avg(F.col("bet_amount")).over(window_spec), 2))

        self.bets = self.bets.withColumn("market_grade", F.concat(
                    F.lit("$"), F.col("bet_amount"), F.lit("/"), F.lit("$"), F.col("grade")
                )
            ).drop("unix_timestamp", "grade")

        return self.bets
