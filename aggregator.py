import numpy as np

from abc import ABCMeta, abstractmethod, abstractproperty


class AggregatorBase():
    __metaclass__ = ABCMeta

    def __init__(self):
        self._agg_func = None
        self._default_agg_func_value = None

    @property
    def agg_func(self):
        return self._agg_func
        
    @property
    def default_agg_func_value(self):
        return self._default_agg_func_value
    
    @abstractmethod
    def aggregate(self, data):
        if len(data) > 0:
            return [self._agg_func(data_part) if len(data_part) > 0 else self._default_agg_func_value for data_part in data]
        else:
            return self._default_agg_func_value

class CounterAggregator(AggregatorBase):
    def __init__(self):
        super().__init__()
        self._agg_func = np.sum
        self._default_agg_func_value = 0


class AverageAggregator(AggregatorBase):
    def __init__(self):
        super().__init__()
        self._agg_func = np.average
        self._default_agg_func_value = 0


class PointAverageAggregator(AggregatorBase):
    def __init__(self):
        super().__init__()
        self._agg_func = np.average
        self._default_agg_func_value = 0


class CustomAggregator(AggregatorBase):
    def __init__(self, agg_func, default_agg_func_value):
        super().__init__()
        self._agg_func = agg_func
        self._default_agg_func_value = default_agg_func_value


class AggregatorFactory():
    # This is the factory method
    @staticmethod
    def get_aggregator(bucket_type, **kwargs):
        if bucket_type == 'counter':
            return CounterAggregator()
        elif bucket_type == 'average':
            return AverageAggregator()
        elif bucket_type == 'datapoint':
            return PointAverageAggregator()
        else:
            # default behavious for unknown bucket_type is averaging
            agg_func = kwargs.get('agg_func', np.average)
            default_agg_func_value = kwargs.get('default_agg_func_value', 0)
            return CustomAggregator(agg_func, default_agg_func_value)