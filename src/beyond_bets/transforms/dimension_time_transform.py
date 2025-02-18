from pyspark.sql import DataFrame
from beyond_bets.base.dataset import Dataset
from typing import Any
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F

class DimensionTimeTransform(Transform):
    """
    Abstract base class for time-based transformations.
    """
    def __init__(self):
        super().__init__()
        self._name: str = "<needs to be set>"
        self._inputs: dict[str, Dataset] = {"bets": Bets()}

    def _validate_dimensions(self, dimensions: list[str]) -> None:
        """
        Validates that all dimensions exist as columns in all input datasets.
        
        Args:
            dimensions: List of dimension column names to validate
            
        Raises:
            ValueError: If any dimension is missing from any input dataset
        """
        for dataset_name, dataset in self._inputs.items():
            missing_dims = [dim for dim in dimensions if dim not in list(dataset.reader().columns)]
            if missing_dims:
                raise ValueError(
                    f"Dataset '{dataset_name}' is missing required dimensions: {missing_dims}"
                )
    
    def _validate_time_period(self, time_period: str) -> None:
        """
        Validates that the time period is a valid time period.
        """
        valid_time_periods = ["hour", "day"]
        if time_period not in valid_time_periods:
            raise ValueError(f"Invalid time period: {time_period}. Must be one of: {valid_time_periods}")

    def _transformation(
            self,
            **kwargs: dict[str, Any]
        ) -> DataFrame:
        """
        Abstract method for the transformation logic.
        """
        if "dimensions" in kwargs.keys():
            self._validate_dimensions(kwargs["dimensions"])
        else:
            raise ValueError("Dimensions are required")
        if "time_period" in kwargs.keys():
            self._validate_time_period(kwargs["time_period"])
        else:
            raise ValueError("Time period is required")

        if kwargs["time_period"] == "hour":
            self.bets = self.bets.withColumn("timeDimension", F.hour(F.col("timestamp")))
        elif kwargs["time_period"] == "day":
            self.bets = self.bets.withColumn("timeDimension", F.to_date(F.col("timestamp")))

        return (
            self.bets
            .groupBy(kwargs["dimensions"] + ["timeDimension"])
            .agg(F.sum(F.col("bet_amount")).alias("total_bets"))
        )
