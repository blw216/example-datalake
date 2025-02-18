import math

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)
from beyond_bets.base.dataset import Dataset
from beyond_bets.utils.spark import SparkManager
from datetime import datetime, date, timedelta
from random import Random

spark = SparkManager.get_session()


def get_rand_stream(seed: int) -> Random:
    stream: Random = Random()
    stream.seed(seed)
    return stream


class Bets(Dataset):

    def __init__(self):
        super().__init__()
        self._name: str = "Bets"

    def reader(self, data_points: int = 1000000, **kwargs) -> DataFrame:

        # truncate now to keep data from churning too much
        _today: date = date.today()
        _now: datetime = datetime(_today.year, _today.month, _today.day)
        _days: int = 10
        _markets: list[str] = [
            "MIA @ BOS Over 110.5",
            "LAL @ CHI Under 107.5",
            "DEN @ GSW Over 115.5",
            "DET @ OKC Over 111.5",
        ]
        _odds: list[int] = [-115, -110, -105]

        # individual streams to help with determinism
        market_rand_stream: Random = get_rand_stream(500)
        time_rand_stream: Random = get_rand_stream(333)
        odds_rand_stream: Random = get_rand_stream(27)
        amt_rand_stream: Random = get_rand_stream(88)
        player_rand_stream: Random = get_rand_stream(100)

        return spark.createDataFrame(
            [
                (
                    market_rand_stream.choice(_markets),
                    odds_rand_stream.choice(_odds),
                    _now
                    - timedelta(seconds=time_rand_stream.randint(0, _days * 3600 * 24)),
                    (amt_rand_stream.random() * 1999) + 1,
                    player_rand_stream.randint(1, math.ceil(data_points / 100)),
                )
                for _ in range(data_points)
            ],
            schema=StructType(
                [
                    StructField("market", StringType(), False),
                    StructField("odds", IntegerType(), False),
                    StructField("timestamp", TimestampType(), False),
                    StructField("bet_amount", DoubleType(), False),
                    StructField("player_id", IntegerType(), False),
                ]
            ),
        )

    def writer(self, df: DataFrame, **kwargs: dict[str, any]) -> DataFrame:
        return self.reader(**kwargs)
