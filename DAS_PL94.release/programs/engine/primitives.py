"""
    This module contains primitive operations used in differential privacy.
"""
from programs.engine.rngs import DASRDRandIntegers
import numpy as np
import scipy.stats
import time

from programs.queries.querybase import AbstractLinearQuery
from programs.engine.discrete_gaussian_utility import RationalSimpleDiscreteLaplace
from programs.engine.discrete_gaussian_utility import RationalSimpleDiscreteGaussian
from programs.engine.discrete_gaussian_utility import limit_denominator as dg_limit_denominator

## DO NOT IMPORT  programs.engine.tests please
# If you need something in the engine, please move it up here.
# the tests directory is for unit tests; they do not get sent to the workers.
#from programs.engine.tests.test_eps_delta_utility import SimpleDiscreteGaussian as FloatDiscreteGaussian  # TODO: remove this? Uses copious float-ops; but, its speed is useful for large-scale experiments

FloatDiscreteGaussian = None    # do not use at the moment

from fractions import Fraction
from constants import CC

_called_from_test = False  # Used to disallow overriding of rngs except in unit tests.
# _rng_factory = DASRandom  # RDRand draws wrapped in numpy.random module
_rng_factory = DASRDRandIntegers  # RDRand draws unlimited int, with OpenBSD rejection sampling unbiasing


class DPMechanism:
    """
        To check type in other parts of the code, use as dummy for node aggregation; also, to hold common structure
    """
    protected_answer: np.ndarray
    epsilon: float
    variance: float
    sensitivity: int

    def __repr__(self):
        return "Dummy DP Mechanism"


def basic_dp_answer(*, true_data, query: AbstractLinearQuery, inverse_scale: Fraction, bounded_dp=True,
                        mechanism_name=CC.GEOMETRIC_MECHANISM) -> DPMechanism:

    if not isinstance(inverse_scale, Fraction):
        raise TypeError(f"inverse scale should be {Fraction.__name__}, not {type(inverse_scale)}")

    if inverse_scale.numerator == 0:
        return OnlyNoiseMechanism(query.numAnswers())

    mechanism_dict = {
        CC.GEOMETRIC_MECHANISM: RationalGeometricMechanism,
        CC.DISCRETE_GAUSSIAN_MECHANISM: RationalDiscreteGaussianMechanism,
        CC.ROUNDED_CONTINUOUS_GAUSSIAN_MECHANISM: RoundedContinuousGaussianMechanism,
        CC.FLOAT_DISCRETE_GAUSSIAN_MECHANISM: FloatDiscreteGaussianMechanism,
    }

    mycls = mechanism_dict[mechanism_name]
    prim_time1 = time.time()
    # print(f"At time {prim_time1}, primitive mechanism in use: {mechanism_name} -> {mycls}")
    if mycls in (RationalGeometricMechanism, RationalDiscreteGaussianMechanism,) and not query.isIntegerQuery():
        raise ValueError(f"{query.name} is non-integer; cannot use {mycls}")
    if mycls in (RationalGeometricMechanism,):
        sensitivity = query.unboundedDPSensitivity() * 2 if bounded_dp else 1
        prim_cls = mycls(inverse_scale=inverse_scale, sensitivity=sensitivity, true_answer=query.answer(true_data))
    elif mycls in (RationalDiscreteGaussianMechanism, RoundedContinuousGaussianMechanism, FloatDiscreteGaussianMechanism):
        if query.unboundedL2Sensitivity() > 1.:
            raise ValueError(f"Gaussian mechanism implementations only support marginal queries.\nFor this query {query}, use RationalGeometric")
        prim_cls = mycls(inverse_scale=inverse_scale, true_answer=query.answer(true_data))
    else:
        raise ValueError(f"{mechanism_name}, {mycls} noise primitive class not currently supported.")
    prim_time2 = time.time()
    print(f"At time {prim_time2}, returning sampled values. Elapsed time: {prim_time2 - prim_time1}. Query shape: {prim_cls.protected_answer.shape}")
    return prim_cls


class OnlyNoiseMechanism(DPMechanism):
    """
    To return NaN with np.inf variance when PLB spent is zero, regardless of privacy flavor
    """
    def __init__(self, shape):
        self.protected_answer = np.nan + np.zeros(shape)
        self.variance = np.inf
        self.epsilon = Fraction(0)
        self.sensitivity = 2

class NoNoiseMechanism(DPMechanism):
    """No noise mechanism"""
    def __init__(self, epsilon: float, sensitivity: float, true_answer: np.ndarray):
        self.epsilon = np.inf
        self.variance = 1.0  # Can't be zero b/c inverse is used in optimization; constant is fine when no noise
        self.protected_answer = true_answer
        self.sensitivity = sensitivity

class RationalGeometricMechanism(DPMechanism):
    """
    Implementation of the Geometric Mechanism for differential privacy Used by DAS for (epsilon, 0)-differential privacy (bounded or unbounded). Adds noise from the (two-sided) Geometric distribution (a.k.a. Discrete Laplace).
    """
    def __init__(self, inverse_scale: Fraction, sensitivity: int, true_answer: np.ndarray):
        r"""
        :param: inverse_scale (Fraction): Epsilon
        :param: sensitivity (int): the sensitivity of the query. Addition or deletion of one
                                   person from the database must change the query answer vector
                                   by an integer amount
        :param: true_answer (float or numpy array): the true answer

        Output:

            Mechanism object / self.protected_answer = true_answer + DiscreteLaplace_perturbations

        The exact Discrete Laplace implemented here draws from the p.m.f.:

                                exp(inverse_scale) - 1
                Pr[X = x] = ─────────────────────────── exp(-inverse_scale * |x|)
                                exp(inverse_scale) + 1

        """
        assert isinstance(inverse_scale, Fraction)
        # inverse_scale = inverse_scale.limit_denominator(CC.PRIMITIVE_FRACTION_DENOM_LIMIT)
        inverse_scale_n, inverse_scale_d = dg_limit_denominator((inverse_scale.numerator, inverse_scale.denominator),
                                                                    max_denominator=CC.PRIMITIVE_FRACTION_DENOM_LIMIT,
                                                                    mode="lower")

        assert sensitivity == np.floor(sensitivity)
        sensitivity = int(sensitivity)

        # Save for __repr__ and other reporting
        self.epsilon: float = inverse_scale_n/inverse_scale_d
        self.sensitivity = sensitivity

        self.p = 1 - np.exp(-float(self.epsilon/self.sensitivity))
        self.variance = 2 * (1 - self.p) / self.p**2

        shape = np.shape(true_answer)
        size = np.prod(shape)

        assert _called_from_test or _rng_factory is DASRDRandIntegers, "Only unit tests may override _rng_factory."
        rng = _rng_factory()
        perturbations = RationalSimpleDiscreteLaplace(s=inverse_scale_n, t=inverse_scale_d * self.sensitivity, size=size, rng=rng)
        self.protected_answer = true_answer + np.reshape(perturbations, shape)

    def pmf(self, x, location=0):
        """
            f(x) = p/(2-p) (1-p)^(abs(x))
        """
        centered_x = np.abs(x-location)
        centered_int_x = int(centered_x)
        if np.abs(centered_x - centered_int_x) > 1e-5:
            return 0

        return (self.p / (2 - self.p)) * ((1 - self.p)**centered_int_x)

    def inverseCDF(self, quantile, location=0.0):
        r"""
            To be used to create CIs
            quantile: float between 0 and 1
            location: center of distribution

            for all geometric points {0, ... inf} the pdf for the two-sided
            geometric distribution is 1/(2-p) times the pdf for the one-sided
            geometric distribution with the same parameterization. Therefore,
            the area in the rhs tail (>0) for the two-sided cdf for a given
            x value should be 1/(2-p) that of the one-sided geometric.

            Say we want the x s.t P(X<=x) = 0.9 for the two-sided geometric.  Then if we find
            the P(X<=x) = 1 - (0.1*(2-p)), the x value should be the same.

            Because this is a discrete distribution, cdf(quantile) will give
            the smallest x s.t P(X>=x) >= quantile and P(P>=x-1) < quantile
        """
        tail_prob = 1.0 - quantile if quantile > 0.5 else quantile
        one_sided_geom_tail_prob = tail_prob * (2 - self.p)

        answer = scipy.stats.geom.ppf(q=1-one_sided_geom_tail_prob, p=self.p, loc=-1)  # Start at 1, not 0
        answer = location-answer+1 if quantile < 0.5 else location+answer+1

        return answer

    def __repr__(self):
        return f"Geometric(eps={self.epsilon}, sens={self.sensitivity}, variance={self.variance})"


class RationalDiscreteGaussianMechanism(DPMechanism):
    r"""
    IMPORTANT:
        This mechanism can only be used for queries with the following properties:
            1) Coefficients are integer (since it's Discrete Gaussian)
            2) sum of squares in each column to add up to 1 (which, with integer coefficients means they are 1 or 0 and sum to one),
               which guarantees unbounded L2 sensitibity of 1 (as checked in basic_dp_answer function above)

    Implements Canonne, Kamath, & Steinke's Discrete Gaussian Mechanism, with perturbations drawn from:

                                    exp( -x^2 inverse_scale / 2 )
                Pr[X = x] = ─────────────────────────────────────────
                                \sum_{z in Z} exp(-z^2 inverse_scale / 2 )

    This implementation trusts the rng to produce Discrete Uniform samples, but otherwise implements Cannone et al exactly, and in particular completely avoids float ops.
    """
    def __init__(self, inverse_scale: Fraction, true_answer: np.ndarray):
        """
        :param inverse_scale (Fraction): 1/sigma^2
        :param true_answer: true_answer (float or numpy array): the true answer
        """
        # inverse_scale = Fraction(inverse_scale)  # TODO: Remove this when p-adic rationals merged in.
        assert isinstance(inverse_scale, Fraction), "Rational exact Discrete Gaussian requires inverse_scale as Fraction."
        self.epsilon = None     # Not used but constraints_dpqueries expects it
        self.sensitivity = 2.0  # Not used, but constraints_dpqueries expects it

        self.sigma_sq = 1/inverse_scale

        self.variance = computeDiscreteGaussianVariance(self.sigma_sq)

        shape = np.shape(true_answer)
        size = int(np.prod(shape))

        assert _called_from_test or _rng_factory is DASRDRandIntegers, "Only unit tests may override _rng_factory."
        rng = _rng_factory()
        perturbations = RationalSimpleDiscreteGaussian(sigma_sq=self.sigma_sq, size=size, rng=rng)
        self.protected_answer = true_answer + np.reshape(perturbations, shape)

    def __repr__(self):
        return f"RationalDiscreteGaussianMechanism(sigma_sq={self.sigma_sq}, variance={self.variance})"


class RoundedContinuousGaussianMechanism(DPMechanism):
    """
        Insecure / inexact mechanism that does:
            - compute variance as if doing exact Rational Discrete Gaussian
            - use this variance to draw a continuous Gaussian sample of perturbations
            - return rounded(true_answer + perturbations)
        This is intended only for fast measurements during experiments where exact samplers are unnecessary.
    """
    def __init__(self, inverse_scale: Fraction, true_answer: np.ndarray):
        """
        :param inverse_scale (Fraction): 1/sigma^2
        :param true_answer: true_answer (float or numpy array): the true answer
        """
        # inverse_scale = Fraction(inverse_scale)  # TODO: Remove this when p-adic rationals merged in.
        assert isinstance(inverse_scale, Fraction), "Rounded continuous Gaussian requires inverse_scale as Fraction."
        self.epsilon = None     # Not used but constraints_dpqueries expects it
        self.sensitivity = 2.0  # Not used, but constraints_dpqueries expects it

        self.sigma_sq = 1/inverse_scale

        self.variance = computeDiscreteGaussianVariance(self.sigma_sq)
        self.stddev = np.sqrt(self.variance)

        shape = np.shape(true_answer)
        size = int(np.prod(shape))

        print(f"WARNING: inexact (float ops, etc) formally private noise primitive in use! Not appropriate for production!")
        assert _called_from_test or _rng_factory is DASRDRandIntegers, "Only unit tests may override _rng_factory."
        rng = _rng_factory()
        # perturbations = RationalSimpleDiscreteGaussian(sigma_sq=self.sigma_sq, size=size, rng=rng)
        perturbations = rng.normal(loc=np.zeros(size), scale=self.stddev, size=size)
        self.protected_answer = np.around(true_answer + np.reshape(perturbations, shape))

    def __repr__(self):
        return f"RationalDiscreteGaussianMechanism(sigma_sq={self.sigma_sq}, variance={self.variance})"


class FloatDiscreteGaussianMechanism(DPMechanism):
    """
    Implements Canonne, Kamath, & Steinke's Discrete Gaussian Mechanism, but with many floating-point approximations.
    """
    def __init__(self, inverse_scale: Fraction, true_answer: np.ndarray):
        """
        :param inverse_scale (Fraction): 1/sigma^2
        :param true_answer: true_answer (float or numpy array): the true answer
        """
        # inverse_scale = Fraction(inverse_scale)  # TODO: Remove this when p-adic rationals merged in.
        assert isinstance(inverse_scale, Fraction), "Rational exact Discrete Gaussian requires inverse_scale as Fraction."
        self.epsilon = None     # Not used but constraints_dpqueries expects it
        self.sensitivity = 2.0  # Not used, but constraints_dpqueries expects it

        self.sigma_sq = 1/inverse_scale

        self.variance = computeDiscreteGaussianVariance(self.sigma_sq)

        shape = np.shape(true_answer)
        size = int(np.prod(shape))

        print(f"WARNING: inexact (float ops, etc) formally private noise primitive in use! Not appropriate for production!")
        assert _called_from_test or _rng_factory is DASRDRandIntegers, "Only unit tests may override _rng_factory."
        rng = _rng_factory()
        perturbations = FloatDiscreteGaussian(variance=float(self.sigma_sq), size=size, rng=rng)
        self.protected_answer = true_answer + np.reshape(perturbations, shape)

    def __repr__(self):
        return f"RationalDiscreteGaussianMechanism(sigma_sq={self.sigma_sq}, variance={self.variance})"


def computeDiscreteGaussianVariance(sigma_sq):
    """
        Calculate variance by definition  ∑ n^2 ⋅ p(n) / ∑ p(n), where p(n) is the probability density function with zero mean.
        Discrete Gaussian is defined from (-∞, ∞) and those are the summation limits.
        Using symmetry of the Discrete Gaussian, we can change limits to (0, ∞), making sure n=0 is only counted once.

                  ∞  2       2    2   /    ∞       2    2
        Var = 2 ⋅ ∑ n  exp(-n / 2σ )  / 2 ⋅ ∑ exp(-n / 2σ ) + 1
                 n=1                 /     n=1

        where 1 is added to the denominator for n=0 (numerator term for n=0 is 0)
    """

    sigma_sq_fl = float(sigma_sq)

    # Variance converges sigma_sq (like that of continous Gaussian) as sigma -> infinity. The absolute error is <1e-16 at sigma_sq = 10 already
    if sigma_sq_fl > 100:
        return sigma_sq_fl

    bound = np.floor(50. * np.sqrt(sigma_sq_fl))  # Bound to represent infinity, 50 s.d.

    # Sum only (0, bound), because of symmetry. 0 should only be counted once, so will treat it separately and not include in the summing range
    # Numerator term for n=0 is 0 (0^2 * p(0)) and denominator term is p(0)=1, which we will add explicitly
    n = np.arange(-bound, 0)  # Using the negative half to start summation with smaller numbers, to control rounding error, 0 not included
    n2 = n * n
    p = np.exp(-n2 / (2. * sigma_sq_fl))
    v = 2 * np.sum(n2 * p) / (2 * np.sum(p) + 1)  # Adding 1 to account for the n=0 term
    # Use math.fsum instead of np.sum in the very unlikely case of needing more precision

    return v
