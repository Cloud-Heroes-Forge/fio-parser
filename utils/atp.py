import numpy as np
from typing import Callable, Dict, List, Tuple
from numpy.typing import ArrayLike
from numpy.polynomial.polynomial import Polynomial
import scipy.integrate as spi
import pandas as pd
import logging

def calculate_latency_curve(throughput, latency) -> Polynomial:
    """
    Generates a polynomial curve that fits the latency values for a given throughput range
    """
    latency_curve: Polynomial = Polynomial.fit(x=throughput, y=latency, deg=2, w=1/throughput)
    return latency_curve

class ATP():
    """
    Reference: https://dl.acm.org/doi/abs/10.1145/2825236.2825248
        Half-Latency Rule for Finding the Knee of the Latency Curve by Naresh M Patel
        
    """
    def __init__(self, 
                 data: pd.DataFrame,
                 alpha: int = 1, 
                 do_the_math: bool = False) -> None:
        
        self.data: pd.DataFrame = data
        self.data.sort_index(inplace=True)
        self.data['through_normalized']: np.ArrayLike = self.data['total_throughput'] / self.data['total_throughput'].max()
        self.alpha: int = alpha
        self.latency_curve: Polynomial = calculate_latency_curve(self.data['total_throughput'], self.data['avg_latency'])
        self.data['ORT']: np.ArrayLike = np.array([self.__calculate_ort(x=x, curve=self.latency_curve) for x in self.data['total_throughput']])
        self.data['ATP']: np.ArrayLike = np.array([self.__calculate_atp(x=x, curve=self.latency_curve) for x in self.data['total_throughput']])
        # Applying the half-latency rule, we can find j, the smallest index for which 2r(x) − w(x) ( 2*ORT - latency ) is negative
        self.j: pd.Series = self.data[2 * self.data['ORT'] - self.data['avg_latency'] < 0].sort_values(by='ATP', ascending=True).iloc[0]
        # x_i = throughput at point x   
        # w_i = latency at point x
        # r_i = ort at point x
        # x*_1 = throughput at point of intersection
        # w*_1 = latency at point of intersection
        # Applying the half-latency rule, we can easily find j, the smallest index for which 2r_j − w_j is negative
        # j = self.data[self.data['ORT'] - self.data['avg_latency'] < 0].iloc[0]
        # # The four coordinates enclosing the cross-over point are (x_j−1,2r_j−1) and (x_j ,2r_j ) for the doubled r-curve and (x_j−1,w_j−1) and (x_j ,w_j ) for the w-curve
        # x_j_minus_1 = self.data.iloc[j-1]['total_throughput']
        # x_j = self.data.iloc[j]['total_throughput']
        # r_j_minus_1 = self.data.iloc[j-1]['ORT']
        # r_j = self.data.iloc[j]['ORT']
        # w_j_minus_1 = self.data.iloc[j-1]['avg_latency']
        # w_j = self.data.iloc[j]['avg_latency']
        # #Using linear interpolation between points, the value of the cross-over point (x∗ , w∗ ) of the doubled r-curve and the w-curve, and hence the ATP knee is given by:
        # self.x_star = ((w_j - (2*r_j))*x_j_minus_1 + (2*r_j_minus_1 - w_j_minus_1)*x_j) / ((w_j - w_j_minus_1)- (2*(r_j - r_j_minus_1)))
        # self.w_star = (2*(r_j_minus_1*w_j) - 2*(r_j*w_j_minus_1)) / ((w_j - w_j_minus_1) - (2*(r_j - r_j_minus_1))) 
        # logging.debug(f'x_star: {self.x_star}, w_star: {self.w_star}')
        


        self.throughput_x_range: np.ndarray = None
        self.latency_y_range: np.ndarray = None
        self.ort_curve_y_range: np.ndarray = None
        # self.atp: np.int64 = None
        # self.points_of_intersection: Dict[str, any] = None
        if do_the_math: 
            self.do_the_math()

    def do_the_math(self) -> None:
        self.latency_curve = calculate_latency_curve(self.throughput, self.latency)
        self.throughput_x_range = np.linspace(self.throughput.min(), self.throughput.max(), 1000)
        self.latency_y_range = self.__calculate_latency_points()
        self.ort_curve_y_range = np.array([self.__calculate_ort(x=x, w=self.latency_curve) for x in self.throughput_x_range])

        self.atp = self.ort_curve_y_range.max()
        # self.points_of_intersection = self.find_points_of_intersection()
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
    
    def __calculate_ort(self, x: float, curve: Polynomial) -> float:
        """
        Computes the integral (1 / x) * integral(from 0 to x) of { w(u) du }
        
        Args:
        x (float): upper limit of integration
        curve (function): a function that returns the value of w(u) for a given u
        
        Returns:
        The value of the integral
        """
        numerator, abserr = spi.quad(curve, 0, x)
        return numerator / x
    
    def __calculate_atp(self, x: float, curve: Polynomial) -> float:
        """
        Computes the Average Throughput Point (ATP) for a given set of data
        """
        atp = ((x**self.alpha) / (self.__calculate_ort(x=x, curve=curve)))
        return atp
    # def __calculate_atp(self) -> float:

    #     """
    #     Computes the Average Throughput Point (ATP) for a given set of data
    #     """

    #     atp = self.__calculate_ort(self.throughput.max(), self.latency_curve)
    #     return atp

    # def find_points_of_intersection(self) -> dict:
        
    #     """
    #     Finds the point of intersection between the latency curve and the Overall Response Time (ORT) curve
    #     """

    #     # Interpolate ort_y_vals onto the x-values of latency_mathed
    #     response_time_interp = np.interp(self.throughput_x_range, self.throughput_x_range, self.ort_curve_y_range)

    #     # Find the indices of the points of intersection
    #     tolerance = 0.01
    #     intersection_indices = np.where(np.isclose(self.latency_y_range, response_time_interp*2, rtol=tolerance))
    #     while not intersection_indices[0].size:
    #         tolerance = tolerance * 2
    #         logging.debug(f"increased tolerance: {tolerance}")
    #         logging.debug(f"intersection_indices: {intersection_indices}")
    #         intersection_indices = np.where(np.isclose(self.latency_y_range, self.ort_curve_y_range*2, rtol=tolerance))

    #     # Extract the x and y values of the intersection points
    #     intersection_x_vals = np.average(self.throughput_x_range[intersection_indices])
    #     intersection_y_vals = np.average(self.latency_y_range[intersection_indices])

    #     return {'x': intersection_x_vals, 'y': intersection_y_vals, 'tolerance': tolerance}

    def __str__(self) -> str:
        return self.__dict__.__str__()
