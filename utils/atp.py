import numpy as np
from numpy.typing import ArrayLike
import scipy.integrate as spi
import pandas
class ATP():
    """
    Reference: https://dl.acm.org/doi/abs/10.1145/2825236.2825248
        Half-Latency Rule for Finding the Knee of the Latency Curve by Naresh M Patel
        
    """
    def __init__(self, 
                 data: pandas.DataFrame,
                 alpha: int = 1) -> None:
        
        self.data = data.sort_index()
        self.throughput: ArrayLike = data.total_throughput
        self.latency: ArrayLike = data.avg_latency
        self.alpha: int = alpha
        self.queue_depth: int = 0
        self.latency_curve: np.polynomial.polynomial.Polynomial = self.calculate_latency_curve()
        self.throughput_x_range = np.linspace(self.throughput.min(), self.throughput.max(), 1000)
    # def get_atp(self, fio: FioBase) -> float:
    #     return fio.iops_latency_ratio

    def calculate_latency_curve(self) -> np.polynomial.polynomial.Polynomial:
        """
        Generates a polynomial curve that fits the latency values for a given throughput range
        """
        latency_curve = np.polynomial.polynomial.Polynomial.fit(x=self.throughput, y=self.latency, deg=2, w=1/self.throughput)
        return latency_curve
    
    def calculate_latency_points(self) -> np.polynomial.polynomial.Polynomial:
        """
        Computes the latency values for a given throughput range

        Args:
        
        Returns:
        w(x) over the range 0,x
        """
        latency_mathed = self.latency_curve(self.throughput_x_range) 
        return latency_mathed
    
    def ORT(x:float , w) -> float:
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
        denominator = x
        return numerator / denominator

    def find_points_of_intersection(self) -> dict:
        """
        Finds the point of intersection between the latency curve and the Overall Response Time (ORT) curve
        """
        latency_y_vals = self.calculate_latency_points()
        ort_y_vals = np.array([self.ORT(x=x, w=self.latency_curve) for x in self.throughput_x_range])
        # Interpolate response_time_vals onto the x-values of latency_mathed
        response_time_interp = np.interp(self.throughput_x_range, self.throughput_x_range, ort_y_vals)

        # Find the indices of the points of intersection
        intersection_indices = np.where(np.isclose(latency_y_vals, ort_y_vals*2, rtol=0.01))

        # Extract the x and y values of the intersection points
        intersection_x_vals = np.average(self.throughput_x_range[intersection_indices])
        intersection_y_vals = np.average(latency_y_vals[intersection_indices])

        return {'x': intersection_x_vals, 'y': intersection_y_vals}
    
    def find_closest_queue_depth(self):
        """
        Finds the queue depth that is closest to the point of intersection between 
            the latency curve and the Overall Response Time (ORT) curve
        """
        intersection_point = self.find_points_of_intersection()
        closest_queue_depth = self.data[self.data.throughput <= intersection_point['x']].max()
        return closest_queue_depth['io_depth']
