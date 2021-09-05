import numpy as np
from scipy.stats import norm as phi
from matplotlib import pyplot as plt
from scipy.stats import binom
import scipy.optimize as so
import mpmath


class BaseGeoCurve:
    """ Geometric Mechanism advanced composition with rational allocations having a common denominator

    Given a single histogram query answered with the geometric mechanism with privacy parameter eps,
    the privacy loss random variable depends on whether we are dealing with bounded neighbors ("modify a record")
    or unbounded neighbors ("add or remove a record").

    For unbounded neighbors, the privacy loss random variable plrv has the following distribution:
    P(plrv = eps) = 1/(1+e^{-eps})
    P(plrv = -eps) = 1/(1+e^{eps})
    because only 1 entry can change and the noise distribution is p(k) = (1-e^{-eps)})/(1+e^{-eps}) e^{-eps|k|}

    For bounded neighbors, 2 entries will change and the noise distribution is p(k) = (1-e^{-eps/2)})/(1+e^{-eps/2}) e^{-eps|k|/2}.
    The distribution of the privacy loss variable is:
    P(plrv = eps) = 1/(1+e^{-eps/2})^2
    P(plrv = 0) = 2 * 1/(1+e^{-eps/2}) * 1/(1+e^{eps/2})
    P(plrv = -eps) = 1/(1+e^{eps/2})^2

    """
    def __init__(self, geo_prop_int, query_prop_int, pure_eps, bounded=True, dps=50):
        """ Inputs:
            geo_prop_int: integer vector proportional to the geographic allocation
            query_prop_int: integer vector proportional to the query allocation
            pure_eps: the pure dp epsilon
            bounded: whether to use the "modify a record" version of DP (True) or "add/remove" (False)
            dps: number of decimal digits of precision to use in calculations
        """
        self.geo_prop_int = geo_prop_int  # vector of proportions of privacy budget allocated to each geolevel
        self.query_prop_int = query_prop_int  # vector of proportions of privacy budget allocated to each query
        self.pure_eps = pure_eps
        self.bounded = bounded
        self.mp = mpmath.mp.clone()  # set our own context
        self.mp.dps = dps
        self.geo_denom = int(np.sum(self.geo_prop_int))
        self.query_denom = int(np.sum(self.query_prop_int))
        self.cdf = self.compute_cdf()

    def compute_cdf(self):
        """ Computes the cdf  of the privacy loss random variable plrv
        It is stored as an array of size 1+2(geo_denom * query_denom) as this is the number
        of distinct values of the privacy loss random variable. In the CDF array, cdf[i] is
        P(plrv <= (i - (geo_denom * query_denom)) * pure_eps/(geo_denom * query_denom)
        """
        focal = self.geo_denom * self.query_denom
        # int_min = -focal
        # int_max = focal
        allocs = [p*q for p in self.geo_prop_int for q in self.query_prop_int]  # proportional to allocations of each query
        zero = self.mp.mpf(0)
        one = self.mp.mpf(1)
        two = self.mp.mpf(2)
        base_eps = self.mp.mpf(self.pure_eps) / (two if self.bounded else one) / self.mp.mpf(focal)

        cdf = self.mp.matrix(([0] * focal) + ([1] * (focal+1)))  # column vector, pdf of the constant 0
        oldcdf = cdf.copy()
        for query in allocs:
            oldcdf[:] = cdf[:]  # copy in place
            denom_high = one + self.mp.exp(-base_eps * self.mp.mpf(query))
            denom_low = one + self.mp.exp(base_eps * self.mp.mpf(query))
            if self.bounded:
                p_top = one/(denom_high * denom_high)
                p_bottom = one/(denom_low * denom_low)
                p_middle = one - p_top - p_bottom
            else:
                p_top = one/denom_high
                p_bottom = one/denom_low
                p_middle = zero
            for i in range(cdf.rows):
                cdf[i] = (p_bottom * oldcdf[i + query] if i + query < oldcdf.rows else p_bottom) + \
                              (p_middle * oldcdf[i]) +  \
                              (p_top * oldcdf[i - query] if i >= query else zero)
        return cdf

    def get_delta(self, target_eps, tol=0.0000001):
        # delta = P(sum_i X_i >= target_eps) - e^{target_eps}P(sum_i X_i <= -target_eps)
        # cdf array satisfies P(plrv <= (i - (geo_denom * query_denom)) * pure_eps/(geo_denom * query_denom)
        focal = self.geo_denom * self.query_denom
        exppart = self.mp.exp(self.mp.mpf(target_eps))
        # get the approximate index in the cdf array that corresponds to target_eps
        approx_first_index = target_eps * focal/self.pure_eps + focal - tol
        first_index = int(np.floor(approx_first_index))
        # integer correction
        if first_index == approx_first_index:
            first_index = first_index-1
        # now get the left half of the delta computation
        if first_index < 0:
            left = self.mp.mpf(1)
        elif first_index >= self.cdf.rows:
            left = self.mp.mpf(0)
        else:
            left = self.mp.mpf(1) - self.cdf[first_index]
        # now get index for second half
        approx_second_index = -target_eps * focal/self.pure_eps + focal - tol
        second_index = int(np.floor(approx_second_index))
        if second_index < 0:
            right = self.mp.mpf(0)
        elif second_index >= self.cdf.rows:
            right = exppart
        else:
            right = exppart * self.cdf[second_index]
        return left-right


class UniformGeoCurve:
    """ Geometric mechanism with uniform query allocation """
    def __init__(self, geo_prop, query_prop):
        self.geo_prop = geo_prop  # vector of proportions of privacy budget allocated to each geolevel
        self.query_prop = query_prop  # vector of proportions of privacy budget allocated to each query
        assert np.min(self.geo_prop) == np.max(self.geo_prop) and np.min(self.query_prop) == np.max(self.query_prop), "Only uniform allocations currently supported"

    def get_delta(self, target_eps, pure_eps, *, bounded):
        """ get the target_eps, delta-DP guarantees when the geometric mechanism satisfies pure_eps-DP """
        assert not bounded, "bounded is not supported yet"
        num = np.size(self.geo_prop) * np.size(self.query_prop)  # total number of queries affected by 1 person
        param = pure_eps * (self.geo_prop[0] * self.query_prop[1]) / (2.0 if bounded else 1.0)  # eps param per query
        # the privacy loss random variable is a sum of num independent variables X where
        #      P(X=param) = 1/(1+e^{-param})
        #      P(X=-param) = 1/(1+e^{param})
        #  Let Z be a Bernoulli(1/(1+e^{-param})) random variable. Then X = 2*param*Z - param
        #  P(sum_i X_i >= y) = P(2*param*Binomial(n, 1/(1+e^{-param})) - n*param >= y) = P(Binomial(n, 1/(1+e^{-param})) >= y/(2 * param) + n/2)
        #  delta = P(sum_i X_i >= target_eps) - e^{target_eps}P(sum_i X_i <= -target_eps)
        #        = P(Binomial(n, 1/(1+e^{-param})) >= target_eps/(2 * param) + n/2)   -   e^{target_eps)P(Binomial(n, 1/(1+e^{-param})) <= -target_eps/(2 * param) + n/2)
        z = binom(num, 1/(1+np.exp(-param)))
        upper = target_eps/(2*param) + num/2.0
        lower = -target_eps/(2*param) + num/2.0
        if np.ceil(upper) == upper:
            upper = upper - 0.5
        left = z.sf(upper)  # 1-z.cdf(upper)
        right = np.exp(target_eps) * z.cdf(lower)
        return left - right


class BaseCurve:
    """ Computes approxiate dp information given the relative proportion of noise scales added to each query.
    assumes easy query has column L2 norm bounded by 1 for each column and all entries in query matrix are nonnegative.
     """
    def __init__(self, scales):
        self.scales = scales  # relative proportion of noise scales

    def get_hit(self, nscale, bounded=True):
        """ Generalization of the Delta/sigma term to account for multiple queries at different noise scales
        assumes all queries have non-negative coefficients and each column has squared L2 norm equal to 1

        tot_hit = sum_{(q,g) in (query_props, geolevel_props)} 1 / [ ( nscale )^2 * p*q ]
        returns sqrt( 2 * sum_{(q,g) in (query_props, geolevel_props)} 1 / [ ( nscale )^2 * p*q ] ) [ for bounded DP ]
        """
        tot_hit = 0
        for s in self.scales:
            tot_hit += 1.0 / ((nscale ** 2) * s)  # nscale is linear scale, but self.scales are input in config in squared units
        return np.sqrt(tot_hit * (2.0 if bounded else 1.0))  # double the term because squared sensitivity doubles in bounded case

    def get_rho(self, nscale, bounded=True):
        """
            returns sum_{(q,g) in (query_props, geolevel_props)} 1 / ( nscale / p*q )^2 ) [ for bounded DP ]

            To justify rho calculation, apply Theorem 14 of Cannone et al to each marginal query in each geolevel to compute
            rho specific to that query and geolevel, then sum using zCDP composition
        """
        hit = self.get_hit(nscale, bounded)
        rho = hit * hit / 2.0
        return rho

    def get_zcdp_delta(self, eps, nscale, bounded=True):
        rho = self.get_rho(nscale, bounded)
        if self.verbose:
            print(f"For nscale {nscale}, eps {eps}, zCDP rho calculated as: {rho}")

        def g(alpha):  # Cannone et al Corollary 13 (zCDP => eps, delta-DP conversion for delta = g(alpha)); see (Eq 18)
            # g(a) = (a-1)(a*r - e) + (a-1)log(1-1/a) - log(a)
            #      = (a-1)(a*r - e) + a*log(1-1/a) -log(1-1/a) - log(a)
            # Last 2 terms have been re-written as follows:
            #      = ... -log(1-1/a) - log(a) = ... -log((a-1)/a) - log(a) = ... -log(a-1) + log(a) - log(a) = ... -log(a-1)
            return (alpha - 1.0) * (alpha * rho - eps) + alpha * np.log(1 - 1.0 / alpha) - np.log(alpha - 1)

        def gprime(alpha):  # Cannone et al Eq 19 (derivative of g; use to optimize over alpha in (1,infty) to approximate delta)
            return (2 * alpha - 1) * rho - eps + np.log(1 - 1.0 / alpha)

        maxiter = 1000
        # start, end as proposed in Cannone et al, top of page 15
        # NOTE:
        #   - the proposed Cannone et al start value can be <1 if guessed eps is <rho. Added max(..., 1)
        #   - when eps<rho, bisect will start at alpha=1, and gprime=-inf
        start = max((eps + rho)/(2 * rho), 1)
        end = max((eps + rho + 1)/(2 * rho), 2)
        opt_alpha = so.bisect(f=gprime, a=start, b=end, maxiter=maxiter)  # Optimize g over alpha by finding root of derivative
        search_msg = f"Searched for optimal alpha on interval I=[{start}, {end}]. g(I)=[{g(start)},{g(end)}]."
        search_msg += f" g'(I)=[{gprime(start)},{gprime(end)}]"
        search_msg += f" alpha* found: {opt_alpha}"
        if self.verbose:
            print(search_msg)
        # Below should be tighter than np.exp(-(eps - rho)**2/(4.0 * rho))] (looser, standard delta bound; Cannone et al Eq 16)
        return np.exp(g(opt_alpha))

    def get_bw_delta(self, eps, nscale, bounded=True):
        """
            Given a noise scale and an epsilon, find the corresponding delta using the method of Borja & Wang
        """
        # NOTE: this is currently specific to continuous Gaussian, and only heuristic for discrete Gaussian?
        hit = self.get_hit(nscale, bounded)
        first = phi.cdf(hit / 2.0 - eps / hit)
        second = phi.cdf(-hit / 2.0 - eps / hit)
        delta = first - np.exp(eps) * second
        return delta

    def get_delta(self, eps, nscale, bounded=True, zcdp=False):
        if zcdp:
            delta = self.get_zcdp_delta(eps, nscale, bounded)
        else:
            delta = self.get_bw_delta(eps, nscale, bounded)
        return delta

    def get_scale(self, eps, delta, bounded=True, tol=1e-7, zcdp=False):
        """ given epsilon and delta, find the noise scale that achieves it using binary search """
        # get upper bound on sigma
        up_sigma = 1.0
        up_delta = self.get_delta(eps, up_sigma, bounded, zcdp)
        while up_delta > delta:  # Increase noise scale u.b. until computed delta is below requested delta at requested eps
            up_sigma = up_sigma * 2
            up_delta = self.get_delta(eps, up_sigma, bounded, zcdp)
        # get lower bound on sigma
        low_sigma = 1.0
        low_delta = self.get_delta(eps, low_sigma, bounded, zcdp)
        while low_delta < delta:  # Decrease noise_scale l.b. until computed delta is above requested delta at requested eps
            low_sigma = low_sigma / 2
            low_delta = self.get_delta(eps, low_sigma, bounded, zcdp)
        # now approximate desired sigma to w/in tol using binary search
        while np.abs(low_sigma-up_sigma) > tol:
            mid_sigma = (low_sigma+up_sigma) / 2.0
            mid_delta = self.get_delta(eps, mid_sigma, bounded, zcdp)
            if delta > mid_delta:  # if mid_delta too small, search for decreased noise scale
                up_sigma = mid_sigma
            elif delta < mid_delta:  # if mid_delta too large, search for increased noise_scale
                low_sigma = mid_sigma
            else:  # exact float equality delta==mid_delta match; terminate on next iteration
                low_sigma = up_sigma
        return up_sigma  # return upper bounding/conservative approximation to ideal sigma

    def get_epsilon(self, delta, nscale, bounded=True, tol=1e-7, zcdp=False):
        """
            Given a delta and noise scale, find the epsilon using binary search
        """
        max_delta = self.get_delta(0.0, nscale, bounded, zcdp)
        if delta >= max_delta:
            return 0.0
        # get upper bound on epsilon
        up_eps = 10.0
        up_delta = self.get_delta(up_eps, nscale, bounded, zcdp)
        while delta < up_delta:
            up_eps = up_eps * 2.0
            up_delta = self.get_delta(up_eps, nscale, bounded, zcdp)
        # now bracket it
        low_eps = 0.0
        while np.abs(low_eps-up_eps) > tol:
            mid_eps = (low_eps + up_eps) / 2.0
            mid_delta = self.get_delta(mid_eps, nscale, bounded, zcdp)
            if delta > mid_delta:
                up_eps = mid_eps
            elif delta < mid_delta:
                low_eps = mid_eps
            else:
                low_eps = up_eps
        return low_eps

    def get_eps_equivalent(self, pure_dp_eps, delta, bounded=True, zcdp=False):
        """
            Given a pure dp epsilon, we consider what would happen if
            Gaussian noise with the same scale (as the laplace with this epsilon) was added instead.
            Returns the approx dp epsilon associated with the input delta
        """
        pure_dp_scale = np.sqrt(2)/pure_dp_eps * (2.0 if bounded else 1.0)
        approx_eps = self.get_epsilon(delta, pure_dp_scale, bounded, zcdp)
        return approx_eps, pure_dp_scale

    def get_curve(self, nscale, bounded=True, start=None, end=None, plot=True, zcdp=False):
        """
            Prints and optionally plots an epsilon delta curve associated with a noise scale
        """
        if start is None:
            start = nscale/2
        if end is None:
            end = 10 * start
        eps_values = np.arange(start, end, 0.01)
        delta_values = np.array([self.get_delta(eps, nscale, bounded, zcdp) for eps in eps_values])
        plt.plot(eps_values, delta_values)
        plt.xaxis = "epsilon"
        plt.yaxis = "delta (log scale)"
        plt.yscale("log")
        plt.title(f"eps,delta plot for nscale={nscale},\n comparable pure dp eps: {2*np.sqrt(2)/nscale}")
        printable_eps = [0.1, 0.5, 0.7, 0.9] + list(range(1, 21))
        print(f"noise scale: {nscale}")
        print(f"equivalent pure dp eps: {2*np.sqrt(2)/nscale}")
        for e in printable_eps:
            d = self.get_delta(e, nscale, bounded, zcdp)
            print(f"eps: {e}, delta: {d}")
        print("-------------------------------")
        if plot:
            plt.show()


class zCDPEpsDeltaCurve(BaseCurve):
    """
        Computes approx dp information when given a factored way of representing noise scales. The noise
        scale is proportional to 1.0/(q * g) where q is the allocation for the query and g is the allocation for the
        geolevel. query_prop lists the query allocations and geo_prop lists the geolevel allocations. Note that
        allocations are assumed to be given as squares in config file, so that zCDP rho is linear in 1/(p*g), not in
        1/(p*g)^2.
    """
    def __init__(self, geo_allocations_dict, verbose=True):
        self.verbose = verbose
        scales = []
        # assert len(geo_props) == len(query_props_dict), f"geo_props ({geo_props}) and query_prop_dict ({query_props_dict}) of unequal length sent to Curve"
        # for geoprop, geolevel in zip(geo_props, query_props_dict.keys()):
        for geolevel, (geoprop, qprops) in geo_allocations_dict.items():
            if geoprop < 1e-7:  # supposed to check "== 0", but can be a float
                continue
            scales += [float(1.0 / (geoprop * q)) for q in qprops]
        super().__init__(scales)


class GeographicScaleCurve(BaseCurve):
    """
        Computes the approx dp information when scale for each query is determined as follows:
        geo_scale is the scale assigned to a geolevel.
        For each query in the geolevel g, it has an allocation q.
        The noise scale added to the query is proportional to g/q, where g is the noise scale for that geolevel
    """
    def __init__(self, *, geo_scale, query_prop):
        self.geo_scale = geo_scale  # vector  noise scale to geography levels
        self.query_prop = query_prop  # vector of noise scale allocations to each query within a geography
        scales = [float(g)/q for g in geo_scale for q in query_prop]
        super().__init__(scales)


##################
### The following functions are all demonstrations, hence they use particular hard-coded PLB proportions:

def disc_vs_continuous(nscale=0.35, bounded=True):
    geo_ddp = [0.2, 0.2, 0.12, 0.12, 0.12, 0.12, 0.12]
    query_ddp = [0.1, 0.2, 0.5, 0.05, 0.05, 0.05, 0.05]
    curve = zCDPEpsDeltaCurve(geo_ddp, query_ddp)
    for eps in np.arange(1, 8, 0.1):
        delta1 = curve.get_delta(eps, nscale, bounded, zcdp=False)
        delta2 = curve.get_delta(eps, nscale, bounded, zcdp=True)
        # first delta is from pure gaussian, second is numerically computed for discrete
        print(f"eps: {eps}, cont_delta: {delta1}, disc_delta: {delta2}")


def apples2apples_gauss_v_unif(pure_eps=4.0):
    gauss, geom = get_demo2()
    allocation = geom.geo_prop[0] * geom.query_prop[0]
    # compute per query std for bounded DP under geo mechanism
    param = pure_eps * allocation/2.0
    p = 1-np.exp(-param)
    std = np.sqrt((1-p))/p
    # noise scale to use with gaussian to get the same per query std
    noise_scale = std * allocation
    for target in np.arange(1.0, pure_eps+1.0, 0.5):
        geo_delta = geom.get_delta(target, pure_eps, bounded=False)
        gauss_delta = gauss.get_delta(target, noise_scale, bounded=False)
        print(f"target_eps: {target}, geo_delta: {geo_delta}, gauss_delta: {gauss_delta}")


def explore(pure_eps=4.0, bounded=True):
    # geo_ddp = [0.2, 0.2, 0.12, 0.12, 0.12, 0.12, 0.12]
    # query_ddp = [0.1, 0.2, 0.5, 0.05, 0.05, 0.05, 0.05]
    int_geo_ddp = [5, 5, 3, 3, 3, 3, 3]  # same relative proportion as geo_ddp
    int_query_ddp = [2, 4, 10, 1, 1, 1, 1]  # same realtive proportion as query_ddp
    geometric = BaseGeoCurve(geo_prop_int=int_geo_ddp, query_prop_int=int_query_ddp, pure_eps=pure_eps, bounded=bounded)
    for target in np.arange(1.0, pure_eps+1.0, 0.125):
        geo_delta = geometric.get_delta(target)
        print(f"target_eps: {target}, geo_delta: {geo_delta} for a pure_eps of {pure_eps}")


def get_demo():
    """
        Return proportions from the DDP
    """
    return zCDPEpsDeltaCurve(geo_prop=[0.2, 0.2, 0.12, 0.12, 0.12, 0.12, 0.12],
                             query_prop=[0.1, 0.2, 0.5, 0.05, 0.05, 0.05, 0.05])


def get_demo2():
    """
        return curve and uniformgeocurve objects for a uniform plb allocation
    """
    geo_prop = [1.0/7] * 7
    query_prop = [1.0/7] * 7
    return zCDPEpsDeltaCurve(geo_prop, query_prop), UniformGeoCurve(geo_prop, query_prop)


def run_demo():
    c = get_demo()
    delta = 1e-10
    epsilons = range(1, 21)
    for e in epsilons:
        s = c.get_scale(e, delta)
        print(f"For eps={e}, delta={delta}:")
        print(f"    Gaussian scale: {s}")
        print(f"    Config PLB setting: {2 / s}")
    # c.get_curve(1.0, plot=False)
    # c.get_curve(0.5, plot=False)
    # c.get_curve(0.25, plot=False)
    # c.get_curve(0.1, plot=False)
    # c.get_curve(0.05, plot=False)


if __name__ == "__main__":
    run_demo()
