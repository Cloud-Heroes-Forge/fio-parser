import numpy as np
class ATP():
    """
    Reference: https://dl.acm.org/doi/abs/10.1145/2825236.2825248
        Half-Latency Rule for Finding the Knee of the Latency Curve by Naresh M Patel
        
    """
    def __init__(self) -> None:
        self.atp: float = 0.0
        self.atp_pass: bool = False
        self.atp_threshold: float = 0.0
        self.queue_depth: int = 0

        pass

    # def get_atp(self, fio: FioBase) -> float:
    #     return fio.iops_latency_ratio
    
    def fit_latency_curve(self, throughput: list, latency: list) -> function:
        """
        Fit a curve to the latency data using a weighted least squares method.
        """
        curve = np.polynomial.polynomial.Polynomial.fit(x=throughput, y=latency, deg=2, w=1/latency)
        return curve
    
    def overall_response_time(self, throughput: list, latency: list) -> function:
        """
        Calculate the overall response time for a given throughput and latency.
        """
        curve = self.fit_latency_curve(throughput, latency)
        area_under_curve = curve.integ()
        response_time = (1/throughput) * area_under_curve

    
    # Overall Response Time = r(x) = (a(x))/x = (1/x) * Integral from 0 to x of w(u)du


    # ATP(x) = (x^a)/r(x) = (x^(1+a))/a(x)
    
     
    # 
