from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from beyond_bets.base.dataset import Dataset
from typing import Union, Any


class Transform(ABC):
    """
    Abstract base class for data transformations.
    
    This class provides a framework for creating data transformations that can
    be chained together. Each transformation can take both raw datasets and
    other transformations as inputs.
    
    Attributes:
        _name (str): Name of the transformation.
        _inputs (dict): Dictionary of input sources (Dataset or Transform objects).
    """

    def __init__(self):
        """
        Initializes the Transform class with a default name and an empty inputs dictionary.
        """
        self._name: str = "<needs to be set>"
        self._inputs: dict[str, Union[Dataset, Transform]] = dict()

    def _ingest(self):
        """
        Ingests input datasets or transformations into the current transformation.
        
        This method validates the inputs and sets them as attributes of the class.
        
        Raises:
            TypeError: If _inputs is not a dictionary or if any input key is not a string.
            ValueError: If no inputs are provided.
            TypeError: If an input is neither a Dataset nor a Transform.
        """
        if not isinstance(self._inputs, dict):
            raise TypeError("_inputs must be a dictionary")
        elif not len(self._inputs):
            raise ValueError("Transform must have at least 1 input!")

        for k, v in self._inputs.items():
            if not isinstance(k, str):
                raise TypeError(f"Input key {k} must be a string")
            if isinstance(v, Dataset):
                setattr(self, k, v.reader())
            elif isinstance(v, Transform):
                setattr(self, k, v.result())
            else:
                raise TypeError(f"Invalid ingest object type {type(v)} for {k}")

    @abstractmethod
    def _transformation(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        Abstract method that must be implemented by subclasses to define the transformation logic.
        
        Args:
            **kwargs: Additional parameters for the transformation logic.
        
        Returns:
            DataFrame: The resulting DataFrame after applying the transformation.
        
        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError

    def result(self, **kwargs: dict[str, Any]) -> DataFrame:
        """
        Executes the transformation and returns the resulting DataFrame.
        
        This method first ingests the inputs and then applies the transformation logic.
        
        Args:
            **kwargs: Additional parameters for the transformation logic.
        
        Returns:
            DataFrame: The resulting DataFrame after the transformation.
        """
        self._ingest()
        return self._transformation(**kwargs)

    @property
    def name(self) -> str:
        """
        Property that returns the name of the transformation.
        
        Returns:
            str: The name of the transformation.
        """
        return self._name
