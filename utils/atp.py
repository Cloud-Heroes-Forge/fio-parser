import numpy as np
from typing import Callable
from numpy.typing import ArrayLike
from numpy.polynomial.polynomial import Polynomial
import scipy.integrate as spi
import pandas

def calculate_latency_curve(throughput, latency) -> Polynomial:
    """
    Generates a polynomial curve that fits the latency values for a given throughput range
    """
    latency_curve: Callable = Polynomial.fit(x=throughput, y=latency, deg=2, w=1/throughput)
    return latency_curve

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
        self.latency_curve: Polynomial = None
        self.throughput_x_range: np.ndarray = None
        self.latency_y_range: np.ndarray = None
        self.ort_curve_y_range = None
        self.atp = None
        self.points_of_intersection = None

    def do_the_math(self) -> None:
        self.latency_curve: Polynomial = calculate_latency_curve(self.throughput, self.latency)
        self.throughput_x_range: np.ndarray = np.linspace(self.throughput.min(), self.throughput.max(), 1000)
        self.latency_y_range: np.ndarray = self.__calculate_latency_points()
        self.ort_curve_y_range = np.array([self.__calculate_ort(x=x, w=self.latency_curve) for x in self.throughput_x_range])
        self.atp = self.ort_curve_y_range.max()
        self.points_of_intersection = self.find_points_of_intersection()
        # self.knee: float = self.__calculate_knee()
        # self.knee_throughput: float = self.throughput[self.knee]
        # self.knee_latency: float = self.latency[self.knee]
        # self.optimal_throughput = self.throughput_x_range[np.argmax(self.ort)]
        # self.optimal_latency = self.latency_y_vals[np.argmax(self.ort)]
        # self.optimal_iodepth = self.data[self.data.total_throughput == self.optimal_throughput].io_depth
        # self.optimal_iodepth = self.optimal_iodepth.iloc[0]
    # def get_atp(self, fio: FioBase) -> float:
    #     return fio.iops_latency_ratio


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


        def integrand(u):
            return w(u) 
        
        numerator, _ = spi.quad(integrand, 0, x)
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
        
        ort_y_vals = np.array([self.__calculate_ort(x=x, w=self.latency_curve) for x in self.throughput_x_range])
        # Interpolate response_time_vals onto the x-values of latency_mathed
        response_time_interp = np.interp(self.throughput_x_range, self.throughput_x_range, ort_y_vals)

        # Find the indices of the points of intersection
        intersection_indices = None 
        tolerance = 0.01
        while intersection_indices is None:
            intersection_indices = np.where(np.isclose(self.latency_y_range, ort_y_vals*2, rtol=tolerance))
            tolerance *= 2  

        # Extract the x and y values of the intersection points
        intersection_x_vals = np.average(self.throughput_x_range[intersection_indices])
        intersection_y_vals = np.average(self.latency_y_range[intersection_indices])

        return {'x': intersection_x_vals, 'y': intersection_y_vals, 'tolerance': tolerance}
    
    def find_closest_queue_depth(self):
        """
        Finds the queue depth that is closest to the point of intersection between 
            the latency curve and the Overall Response Time (ORT) curve
        """
        intersection_point = self.find_points_of_intersection()
        closest_queue_depth = self.data[self.data.total_throughput <= intersection_point['x']].max()
        two = 1+1
        return closest_queue_depth.iodepth

    def __str__(self) -> str:
        return self.__dict__.__str__()
    
    def get_target_iodepth(self) -> int:
        """
        Returns the optimal iodepth for a given set of data
        """
        return self.find_closest_queue_depth()