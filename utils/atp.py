import numpy as np
from typing import Callable
from numpy.typing import ArrayLike
from numpy.polynomial.polynomial import Polynomial
import scipy.integrate as spi
import pandas
class ATP():
    """
    Reference: https://dl.acm.org/doi/abs/10.1145/2825236.2825248
        Half-Latency Rule for Finding the Knee of the Latency Curve by Naresh M Patel
        
    """
    def __init__(self, 
                 data: pandas.DataFrame,
                 alpha: int = 1, 
                 do_the_math: bool = False) -> None:
        
        self.data = data
        self.data.sort_index(inplace=True)
        self.throughput: np.ArrayLike = data.total_throughput
        self.latency: np.ArrayLike = data.avg_latency
        self.alpha: int = alpha
        self.latency_curve: Polynomial = self.calculate_latency_curve()
        self.throughput_x_range: np.ndarray = np.linspace(self.throughput.min(), self.throughput.max(), 1000)
        self.latency_y_range: np.ndarray = self.__calculate_latency_points()
        self.ort_curve_x_range = np.array([self.__calculate_ort(x=x, w=self.calculate_latency_curve) for x in self.throughput_x_range])
        # self.atp = self.ort.max()
        # self.atp: float = self.__calculate_atp()
        # self.knee: float = self.__calculate_knee()
        # self.knee_throughput: float = self.throughput[self.knee]
        # self.knee_latency: float = self.latency[self.knee]
        # self.optimal_throughput = self.throughput_x_range[np.argmax(self.ort)]
        # self.optimal_latency = self.latency_y_vals[np.argmax(self.ort)]
        # self.optimal_iodepth = self.data[self.data.total_throughput == self.optimal_throughput].io_depth
        # self.optimal_iodepth = self.optimal_iodepth.iloc[0]
    # def get_atp(self, fio: FioBase) -> float:
    #     return fio.iops_latency_ratio

    def calculate_latency_curve(self) -> Polynomial:
        """
        Generates a polynomial curve that fits the latency values for a given throughput range
        """
        latency_curve = Polynomial.fit(x=self.throughput, y=self.latency, deg=2, w=1/self.throughput)
        return latency_curve
    
    def __calculate_latency_points(self) -> Polynomial:
        """
        Computes the latency values for a given throughput range

        Args:
        
        Returns:
        w(x) over the range 0,x
        """
        latency_mathed = self.latency_curve(self.throughput_x_range) 
        return latency_mathed
    
    def __calculate_ort(self, x: float, w: Callable) -> float:
        """
        Computes the integral (1 / x) * integral(from 0 to x) of { w(u) du }
        
        Args:
        x (float): upper limit of integration
        w (function): a function that returns the value of w(u) for a given u
        
        Returns:
        The value of the integral
        """
        numerator, _ = spi.quad(self.latency_curve, 0, x)
        return numerator / x
    
    def __calculate_atp(self) -> float:
        """
        Computes the Average Throughput Point (ATP) for a given set of data
        """
        atp = self.__calculate_ort(self.throughput.max(), self.latency_curve)
        return atp

    def find_points_of_intersection(self) -> dict:
        """
        Finds the point of intersection between the latency curve and the Overall Response Time (ORT) curve
        """
        
        ort_y_vals = np.array([self.ORT(x=x, w=self.latency_curve) for x in self.throughput_x_range])
        # Interpolate response_time_vals onto the x-values of latency_mathed
        response_time_interp = np.interp(self.throughput_x_range, self.throughput_x_range, ort_y_vals)

        # Find the indices of the points of intersection
        intersection_indices = np.where(np.isclose(self.latency_y_vals, ort_y_vals*2, rtol=0.01))

        # Extract the x and y values of the intersection points
        intersection_x_vals = np.average(self.throughput_x_range[intersection_indices])
        intersection_y_vals = np.average(self.latency_y_vals[intersection_indices])

        return {'x': intersection_x_vals, 'y': intersection_y_vals}
    
    def find_closest_queue_depth(self):
        """
        Finds the queue depth that is closest to the point of intersection between 
            the latency curve and the Overall Response Time (ORT) curve
        """
        intersection_point = self.find_points_of_intersection()
        closest_queue_depth = self.data[self.data.throughput <= intersection_point['x']].max()
        return closest_queue_depth['io_depth']

    def __str__(self) -> str:
        return self.__dict__.__str__()