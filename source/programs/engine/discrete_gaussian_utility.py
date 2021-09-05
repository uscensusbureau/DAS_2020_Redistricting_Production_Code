from fractions import Fraction
import math
from typing import List, Tuple, Union

# Last modified by P. Zhuravlev. 8/4/2021. Removed RationalBernolli and RationalDiscreteUniform, inlining appropriate rng.integer calls. Removed application of limit_denominator to sigma_sq
# Last modified by P. Zhuravlev. 29/3/2021. Removed RationalBernolliExpFactors implementation
# Last modified by P. Zhuravlev. 15/1/2021. RationalBernolliExpFactors implementation
# Last modified by P. Zhuravlev. 11/3/2020. Shortcuts for Fraction arithmetic via integers.
# Last modified by P. Leclerc. Module contains an exact/rational implementation
# of the Exact Discrete Gaussian Mechanism, with no use of float ops (& limited use of numpy.intXX types),
# as described in https://arxiv.org/abs/2004.00010

# Some code in this module (particularly Implementation # 1) borrows in part or whole, exactly or approximately, from https://github.com/IBM/discrete-gaussian-differential-privacy/blob/master/discretegauss.py
#
# The following usage terms apply to all modifications/contributions made to that code in creating the present module:
#
# “The following applies to the contributions made by Census/Department of Commerce:  Works created by United States Government employees are not subject to copyright in the United States, pursuant to 17 U.S.C. § 105.  Accordingly, no permission is required for their use within the United States, as there is no copyright to either assign or license.  Therefore, the Department of Commerce has no objection to the use and/or reproduction and distribution of such works within the United States.  The Department of Commerce reserves the right to assert copyright protection internationally; however, the Department of Commerce hereby grants to the Recipient a royalty-free, nonexclusive license to reproduce and disseminate the Work in all media outside of the United States.  Works to be published and/or distributed overseas should bear the following notice: “International copyright, [year], U.S. Department of Commerce, U.S. Government.”

# ---
# Exact Discrete Gaussian with fractional arithmetic (loosely modeled on https://github.com/IBM/discrete-gaussian-differential-privacy/blob/master/discretegauss.py) (see programs.engine.tests.test_eps_delta_utility.py for other implementations)
# ---


def RationalSimpleDiscreteGaussian(*, sigma_sq: Union[Fraction, int] = 1, size=1, rng) -> List[int]:
    r"""
        This function is used to draw a len-<size> list of Python integers from the Discrete Gaussian distribution with mass fxn:
                                                                        exp[-(x^2) / 2*sigma_sq]
                                                    Pr[X = x] = ______________________________________, for integer x
                                                                \sum_{z in Z} exp[-(z^2) / 2*sigma_sq]
        This implementation is intended to draw from the target distribution as exactly as is possible. To this end, use of
        floating-point values/arithmetic is avoided, and use of numpy.intXX types is limited (to its use in initial draws from
        the rng, and when combining the Discrete Gaussian perturbations with the true values, in programs.engine.primitives).
        Note that this implementation still deviates from exact Discrete Gaussian draws in that (1) its support is limited by
        the RAM of the machine and (2) it must trust a PRNG to approximate draws from a Discrete Uniform distribution.
        Inputs:
                sigma_sq: Fraction > 0, sigma_sq of Discrete Gaussian mass function to sample from
                size: int (*not* shape tuple), number of samples to return
                rng: pseudo-random number generator (see programs.engine.rngs)
        Output:
                discrete_gaussian_samples: [int, ....]
    """
    assert isinstance(size, int) and size >= 1
    try:
        n, d = sigma_sq.numerator, sigma_sq.denominator
    except AttributeError as err:
        raise AttributeError(f"Argument to RationalSimpleDiscreteGaussian should be a Fraction or int, got {sigma_sq} ({type(sigma_sq)}") from err
    discrete_gaussian_samples = [RationalScalarDiscreteGaussian(sigma_sq=(n, d), rng=rng) for _ in range(size)]
    return discrete_gaussian_samples


def RationalScalarDiscreteGaussian(*, sigma_sq: Tuple[int, int] = (1, 1), rng) -> int:
    r"""
        Draw an exact Discrete Gaussian scalar.
        Inputs:
                sigma_sq: Fraction, sigma_sq of Discrete Gaussian mass function to sample from
                rng: pseudo-random number generator (see programs.engine.rngs)
        Output:
                y: int, scalar Discrete Gaussian sample
    """
    ssqn, ssqd = sigma_sq
    t: int = floorsqrt(ssqn, ssqd) + 1
    c: bool = False
    while not c:
        y: int = RationalScalarDiscreteLaplace(s=1, t=t, rng=rng)
        aux1n: int = abs(y) * t * ssqd - ssqn
        gamma = (aux1n * aux1n, t * ssqd * t * ssqn * 2)
        c: bool = bool(RationalScalarBernoulliExp(gamma=gamma, rng=rng))
        if c:
            return y


def RationalSimpleDiscreteLaplace(*, s=1, t=1, size=1, rng) -> List[int]:
    r"""
        This function is used to draw a len-<size> list of Python integers from the Discrete Laplace (aka 2-sided Geometric)
        distribution with mass fxn:
                                                                        exp[1/(t/s)] - 1.
                                                    Pr[X = x] = _______________________ * exp[-|x|/(t/s)], for integer x
                                                                        exp[1/(t/s)] + 1.
        This implementation is intended to draw from the target distribution as exactly as is possible. To this end, use of
        floating-point values/arithmetic is avoided, and use of numpy.intXX types is limited (to its use in initial draws from
        the rng, and when combining the Discrete Gaussian perturbations with the true values, in programs.engine.primitives).
        Note that this implementation still deviates from exact Discrete Laplace draws in that (1) its support is limited by
        the RAM of the machine and (2) it must trust a PRNG to approximate draws from a Discrete Uniform distribution.
        Inputs:
                s: int >= 1, used to generate intermediate 1-sided Geometric[1-exp(-1/t)] r.v.
                t: int >= 1, used to generate intermediate 1-sided Geometric[1-exp(-s/t)] r.v.
                size: int (*not* shape tuple), number of samples to return
                rng: pseudo-random number generator (see programs.engine.rngs)
        Output:
                discrete_laplace_samples: [int, ...] (each with scale t/s; equivalently, with pure-DP epsilon (s/t)/Delta, for a query
                    with L1-sensitivity Delta)
    """
    discrete_laplace_samples = [RationalScalarDiscreteLaplace(s=s, t=t, rng=rng) for _ in range(size)]
    return discrete_laplace_samples


def RationalScalarDiscreteLaplace(*, s=1, t=1, rng) -> int:
    r"""
        Draw an exact Discrete Laplace scalar.
        Inputs:
                s: int, used to generate intermediate 1-sided Geometric[1-exp(-s/t)] r.v.
                t: int, used to generate intermediate 1-sided Geometric[1-exp(-1/t)] r.v.
                rng: pseudo-random number generator (see programs.engine.rngs)
        Output:
                (1-2*b)*y: int, scalar Discrete Laplace sample (with scale t/s)
    """
    assert isinstance(s, int)
    assert isinstance(t, int)
    assert s >= 1 and t >= 1
    while True:
        d: bool = False
        while not d:
            u: int = rng.integers(low=0, high=t)
            d = bool(RationalScalarBernoulliExp(gamma=(u, t), rng=rng))
        v: int = 0
        a: bool = True
        while a:
            a = bool(RationalScalarBernoulliExp(gamma=(1, 1), rng=rng))
            v = v+1 if a else v
        x: int = u + t*v
        y: int = x // s
        b: int = rng.integers(low=0, high=2) < 1  # Rational Bernoulli with p=1/2
        if not (b == 1 and y == 0):
            return (1 - 2 * b) * y


def RationalSimpleBernoulliExp(*, gamma: Union[Fraction, int] = 0, size=1, rng) -> List[int]:
    r"""
        This function is used to draw a len-<size> list of i.i.d. Python (0/1) integers, each from the Bernoulli(exp(-gamma))
        distribution with mass fxn:
                                                    Pr[X = 1] = exp(-gamma)
                                                    Pr[X = 0] = 1 - Pr[X = 1]
        This implementation is intended to draw from the target distribution as exactly as is possible. To this end, use of
        floating-point values/arithmetic is avoided, and use of numpy.intXX types is limited (to its use in initial draws from
        the rng). Note that this implementation still deviates from exact Bernoulli(exp(-gamma)) draws in that it must trust a
        PRNG to approximate draws from a Discrete Uniform distribution.
        Inputs:
                gamma: Fraction >= 0, used to define Pr[X = 1] = exp(-gamma)
                size: int (*not* shape tuple), number of samples to return
                rng: pseudo-random number generator (see programs.engine.rngs)
        Output:
                bernoulli_exp_samples: [int, ...] (each in {0,1})
    """
    assert isinstance(size, int) and size >= 1
    try:
        n, d = gamma.numerator, gamma.denominator
    except AttributeError as err:
        raise AttributeError(f"Argument to RationalSimpleBernoulliExp should be a Fraction or int, got {gamma} ({type(gamma)})") from err
    bernoulli_exp_samples = [RationalScalarBernoulliExp(gamma=(n, d), rng=rng) for _ in range(size)]
    return bernoulli_exp_samples


def RationalScalarBernoulliExp(*, gamma: Tuple[int, int] = (0, 1), rng) -> int:
    """
        Draw an exact Bernoulli(exp(-gamma)) scalar.
        Inputs:
                gamma: Fraction >= 0, used to define Pr[X = 1] = exp(-gamma)
                rng: pseudo-random number generator (see programs.engine.rngs)
        Output:
                c: int in {0,1}, Bernoulli(exp(-gamma)) distributed scalar
    """
    try:
        gn, gdn = gamma
    except (TypeError, ValueError) as err:
        errclass = TypeError if isinstance(err, TypeError) else ValueError
        raise errclass(f"Argument to RationalScalarBernoulliExp should be a fraction represented as tuple(int, int), got {gamma} ({type(gamma)})") from err
    if 0 <= gn <= gdn:
        k: int = 1
        a: bool = True
        while a:
            a = rng.integers(low=0, high=gdn * k) < gn  # Bernoulli with p = gn/(gdn*k) == gamma/k
            k = k+1 if a else k
        return k % 2
    else:
        for k in range(1, gn // gdn + 1):
            b: bool = bool(RationalScalarBernoulliExp(gamma=(1, 1), rng=rng))
            if not b:
                return 0
        c: int = RationalScalarBernoulliExp(gamma=(gn % gdn, gdn), rng=rng)
        return c


def floorsqrt(num, denom) -> int:
    r"""
    Compute floor(sqrt(x)) exactly. Only requires comparisons between x (which we require to be Fraction) and integer. See:
    https://github.com/IBM/discrete-gaussian-differential-privacy/blob/cb190d2a990a78eff6e21159203bc888e095f01b/discretegauss.py#L99-L118
        Inputs:
                num, denom: fraction num/denom, input value to be evaluated like floor(sqrt(num/denom))
        Output:
                a: int, exactly computed value of floor(sqrt(num/denom))
    """
    assert isinstance(num, int)
    assert isinstance(denom, int)
    assert num >= 0 and denom > 0
    # a, b integers
    a: int = 0  # maintain a^2<=x
    b: int = 1  # maintain b^2>x
    while b * b * denom <= num:  # int < Fraction comparison
        b = 2 * b  # double to get upper bound
    # now do binary search
    while a + 1 < b:
        c = (a + b) // 2  # c = floor((a+b)/2)
        if c * c * denom <= num:  # If interval midpoint is below target, move to upper half-interval
            a = c
        else:  # If interval midpoint is above target, move to lower half-interval
            b = c
    assert isinstance(a, int)  # python 3; checking that nothing unexpected happen to type(a)
    return a


def limit_denominator(fraction: Tuple[int, int], max_denominator=1000000, mode="best"):
    r"""
    This is modified version of fractions.Fraction .limit_denominator()
    method, which works with tuple of ints to avoid calling Fraction
    constructor in tight loops.
    The original source is this
    https://github.com/python/cpython/blob/6b7a90db362253d67201c2a438a3f38f1ec6180c/Lib/fractions.py#L201

    If mode=='best':
        Return (n,d) such that n/d is a best-fitting rational estimate of fraction with d <= max_denominator
    If mode=='upper':
        Return (n,d) such that n/d is a best-fitting rational estimate of fraction with d <= max_denominator & n/d >= fraction
    If mode=='lower':
        Return (n,d) such that n/d is a best-fitting rational estimate of fraction with d <= max_denominator & n/d <= fraction

    >>> limit_denominator((3141592653589793,1000000000000000), 10)
    (22, 7)

    >>> limit_denominator((3141592653589793,1000000000000000), 100)
    (311, 99)

    >>> limit_denominator((4321, 8765), 10000)
    (4321, 8765)

    """
    # Algorithm notes: For any real number x, define a *best upper
    # approximation* to x to be a rational number p/q such that:
    #
    #   (1) p/q >= x, and
    #   (2) if p/q > r/s >= x then s > q, for any rational r/s.
    #
    # Define *best lower approximation* similarly.  Then it can be
    # proved that a rational number is a best upper or lower
    # approximation to x if, and only if, it is a convergent or
    # semiconvergent of the (unique shortest) continued fraction
    # associated to x.
    #
    # To find a best rational approximation with denominator <= M,
    # we find the best upper and lower approximations with
    # denominator <= M and take whichever of these is closer to x.
    # In the event of a tie, the bound with smaller denominator is
    # chosen.  If both denominators are equal (which can happen
    # only when max_denominator == 1 and fraction is midway between
    # two integers) the lower bound---i.e., the floor of fraction, is
    # taken.

    try:
        n, d = fraction
    except (TypeError, ValueError) as err:
        errclass = TypeError if isinstance(err, TypeError) else ValueError
        raise errclass(f"Argument to limit_denominator should be a fraction represented as tuple(int, int), got {fraction} ({type(fraction)}) ") from err

    gcd = math.gcd(n, d)
    n //= gcd
    d //= gcd

    if d <= max_denominator:
        return (n, d)

    p0, q0, p1, q1 = 0, 1, 1, 0
    while True:
        a = n // d
        q2 = q0 + a * q1
        if q2 > max_denominator:
            break
        p0, q0, p1, q1 = p1, q1, p0 + a * p1, q2
        n, d = d, n - a * d

    n, d = fraction  # get back original n and d, to use for choosing between bounds.
    n //= gcd
    d //= gcd

    k = (max_denominator - q0) // q1
    # bound1 = Fraction(p0 + k * p1, q0 + k * q1)
    # bound2 = Fraction(p1, q1)
    # if abs(bound2 - fraction) <= abs(bound1 - fraction):
    #     return bound2
    # else:
    #     return bound1
    b1n, b1d = (p0 + k * p1, q0 + k * q1)   # Upper bound
    b2n, b2d = (p1, q1)                     # Lower bound
    if mode=="best":
        if abs((b2n * d - b2d * n) * b1d) <= abs((b1n * d - n * b1d) * b2d):
            return (b2n, b2d)
        else:
            return (b1n, b1d)
    elif mode=="upper":
        return (b1n, b1d) if b1n * b2d > b2n * b1d else (b2n, b2d)
    elif mode=="lower":
        return (b2n, b2d) if b1n * b2d > b2n * b1d else (b1n, b1d)
    else:
        raise ValueError(f"limit_denominator mode {mode} not recognized.")
