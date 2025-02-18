from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Any


class Dataset(ABC):
    """
    Abstract base class for dataset access.
    
    This class provides a consistent interface for reading and writing datasets,
    regardless of their underlying storage mechanism.

    Attributes:
        _name (str): The name of the dataset
    """

    def __init__(self):
        self._name: str = "<needs to be set>"

    @abstractmethod
    def reader(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        Returns a DataFrame representing the dataset.
        
        Args:
            **kwargs: Additional parameters specific to the dataset implementation
            
        Returns:
            DataFrame: A Spark DataFrame of the dataset
            
        Raises:
            NotImplementedError: If the subclass doesn't implement this method
        """
        raise NotImplementedError

    @abstractmethod
    def writer(self, df: DataFrame, **kwargs: dict[str, Any]) -> DataFrame:
        """
        Persists the input DataFrame and returns a reader to the persisted version.
        
        Args:
            df: The DataFrame to persist
            **kwargs: Additional parameters specific to the dataset implementation
            
        Returns:
            DataFrame: A Spark DataFrame reader of the dataset
            
        Raises:
            NotImplementedError: If the subclass doesn't implement this method
        """
        raise NotImplementedError

    @property
    def name(self) -> str:
        """The name of the dataset."""
        return self._name
