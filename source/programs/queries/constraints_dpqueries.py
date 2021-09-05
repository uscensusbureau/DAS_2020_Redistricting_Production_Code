"""
Module implements Constraint class and DPQuery class.
Constraint is a querybase query with sign and answer to that query.
DPQuery is a querybase query with noisy answer and information about DP mechanism used to get that answer (budget, variance, mechanism name)

Also StackedConstraint and StackedDPQuery classes are implemented.
"""

# python imports
from typing import Tuple, Iterable, Union
import io
import numpy as np
# das-created imports
import programs.utilities.numpy_utils as np_utils
from programs.queries.querybase import AbstractLinearQuery
from programs.engine.primitives import DPMechanism
from programs.sparse import multiSparse

from exceptions import IncompatibleAddendsError


def check_query(query: AbstractLinearQuery) -> AbstractLinearQuery:
    """ Check whether query is actually a query (i.e. proper type)
    :param query: query to check
    :return: query if it passes the check
    """
    if isinstance(query, AbstractLinearQuery):
        return query
    raise TypeError(f"Query must be of class {AbstractLinearQuery.__module__}.{AbstractLinearQuery.__name__}")


def check_rhs(rhs: np.ndarray, query: AbstractLinearQuery) -> np.ndarray:
    """
    Check if right hand size is of the correct type (numpy array) and of the correct size for the :query:
    :param rhs: right hand side to check
    :param query: query to which rhs corresponds to
    :return: rhs if it passes the check
    """
    if isinstance(rhs, np.ndarray):
        data_size = rhs.flatten().size
        num_answers = query.numAnswers()
        if data_size == num_answers:
            return rhs.flatten()
        raise ValueError(f"rhs size must be the same as the query answer, Query size: {num_answers}, data size: {data_size}")
    raise TypeError(f"rhs must be of class {np.ndarray.__module__}.{np.ndarray.__name__}")


class Constraint:
    """
    This class combines the Query class with a right hand side (rhs) and a sign input to create a constraint.

    rhs must be a numpy array with shape/size that corresponds to the query.
    sign must be `=`, `ge`, or `le`. All constraints in a given instance must have the same sign.

    """
    __slots__ = ["name", "_rhs", "_sign", "_query"]
    _rhs: np.ndarray  # Internal var (@property rhs), right-hand-side of the Constraint
    name: str
    _sign: str        # Internal var (@property sign), sign of the Constraint, i.e '=', 'ge' or 'le'
    _query: AbstractLinearQuery  # Internal var (@property query), query that, applied to the data should be 'sign' than/to 'rhs'

    def __init__(self, query, rhs, sign, name=""):
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                query: querybase object
                rhs: numpy array (either flattened or multidimensional), stored rhs will be flattened
                sign: must be of form mentioned above and all constraints must have this same sign.
                name: string
        """
        self.name = name
        self.query = query
        self.rhs = rhs
        self.sign = sign

    @property
    def query(self) -> AbstractLinearQuery:
        """ Query getter """
        return self._query

    @query.setter
    def query(self, value) -> None:
        """ Query setter, checks type before setting """
        self._query = check_query(value)

    @property
    def rhs(self) -> np.ndarray:
        """ Right hand side getter """
        return self._rhs

    @rhs.setter
    def rhs(self, value):
        """ Right hand side setter. Checks type and shape before setting"""
        self._rhs = check_rhs(value, self.query)

    @property
    def sign(self):
        """ Sign getter"""
        return self._sign

    @sign.setter
    def sign(self, value):
        """ Sign setter. Check the sign to be among allowed before setting"""
        allowed = ("=", "ge", "le")
        if value in allowed:
            self._sign = value
        else:
            raise ValueError(f"Sign {value} is given for constraint {self.name} with query {self.query}. Constraint sign must be one of {allowed}")

    def __repr__(self):
        """
            This yields the representation of the Constraint instance.
        """

        output = f" ------------- constraint looks like -----------------\n " \
            f"Constraint: {self.name}\n " \
            f"Constraint contains query: {self.query}\n " \
            f"Constraint rhs size is: {self.rhs.size}\n " \
            f"Constraint contains rhs: {self.rhs}\n " \
            f"rhs data type: {self.rhs.dtype}\n " \
            f"Constraint contains sign: {self.sign}\n " \
            f"-----------------------------------------------------\n "
        return output

    def __eq__(self, other):
        """ Equal if every one of the four attributes are equal. Watch out for the query, its equality is implemented in querybase"""
        for attr in ['name', 'query', 'sign']:
            if self.__getattribute__(attr) != other.__getattribute__(attr):
                return False

        array_comp_func = np.allclose if np.issubsctype(self.rhs, float) else np.array_equal
        if not array_comp_func(self.rhs, other.rhs):
            return False

        return True

    def __add__(self, other):
        """
        Function for adding constraints of the two nodes at the same geolevel together, when adding the nodes.
        Essentially, adding the right-hand sides.
        Check that names, queries and signs are identical, check shapes of right-hand sides and add right-hand sides.
        """

        if not isinstance(other, Constraint):
            raise IncompatibleAddendsError(f"Constraint '{self.name}' + {other}", "type",  type(self), type(other))

        for attr in ['name', 'sign']:  # Comparing queries is too expensive, calculates matrixReps, otherwise add 'query' to the list:
            selfattr = self.__getattribute__(attr)
            otherattr = other.__getattribute__(attr)
            if selfattr != otherattr:
                raise IncompatibleAddendsError(f"Constraints '{self.name}'", attr, selfattr, otherattr)

        # Should not be possible (And won't be, after converting rhs to property)
        # if self.rhs.shape != other.rhs.shape:
        #     msg = f"Constraints cannot be added, having different shapes of right hand sides, '{self.name}': {self.rhs.shape} {other.rhs.shape}"
        #     logging.error(msg)
        #     raise ValueError(msg)

        addrhs = np.array(np.add(self.rhs, other.rhs))
        return Constraint(query=self.query, rhs=addrhs, sign=self.sign, name=self.name)

    def __radd__(self, other):
        """ Let regular __add__ take care of raising the TypeError"""
        return self.__add__(other)

    def check(self, data, tol=0.001):
        """
        This checks that the data meets the constraint
        Inputs:
            data: numpy multiarray that corresponds to the shape of the query
            tol: float, tolerance for checking the constraint
        Outputs:
            status: bool indicating if the check was passed
        """
        # Difference between query applied to the data and the right hand side
        diff = self.query.answer(data) - self.rhs

        if self.sign == "=":
            status = abs(diff) <= tol
        elif self.sign == "ge":
            status = diff + tol >= 0
        elif self.sign == "le":
            status = diff - tol <= 0
        else:
            raise Exception
        return np.all(status)


class DPquery:
    """
        This class combines the Query class with a DP answer to the query to be used in future algorithms.
        DPanswer must be a numpy array with shape/size that corresponds to the query.
    """
    __slots__ = ["name", "query", "DPanswer", "epsilon", "DPmechanism", "Var", "mechanism_sensitivity", "budget_id"]
    DPanswer: Union[np.ndarray, multiSparse, bytes]
    epsilon: float
    DPmechanism: str
    Var: float
    name: str
    query: AbstractLinearQuery
    mechanism_sensitivity: float
    budget_id: Union[int, str]  # Which budget the query should be accounted into if we have more than one

    def __init__(self, query: AbstractLinearQuery, dp_mechanism: DPMechanism, budget_id=0):
        """
            This initializes the necessary matrices/arrays.

            Inputs:
            :param query: querybase object
            :param dp_mechanism: mechanism used, from programs.primitives

            Output:
            :return query with DP answer
        """
        dp_answer: np.ndarray = np.array(dp_mechanism.protected_answer)

        self.query = check_query(query)
        self.DPanswer = check_rhs(dp_answer, query)
        self.epsilon = dp_mechanism.epsilon
        self.DPmechanism = str(dp_mechanism)
        self.Var = dp_mechanism.variance
        self.name = query.name
        self.mechanism_sensitivity = dp_mechanism.sensitivity
        self.budget_id = budget_id

    def __repr__(self):
        """
            This checks the representation of the self object.
        """
        dp_answer_size = self.query.numAnswers()
        output = f"------------- DPquery looks like ---------------\n" \
            f"query: {self.query}\nDPanswer size is {dp_answer_size}\n" \
            f"DPanswer: {self.DPanswer}\nepsilon: {self.epsilon}\n" \
            f"DPmechanism: {self.DPmechanism}\n" \
            f"Variance: {self.Var}\n" \
            f"------------------------------------------------\n"
        return output

    def __add__(self, other):
        """ Add two DP answers for queries (for purpose of aggregation of lower-level nodes to a higher level node"""
        assert isinstance(other, DPquery), f"Cannot add DPQuery with {type(other)}"
        assert self.name == other.name, f"Trying to add DPanswers of different queries: {self.name}, {other.name}"
        assert self.query == other.query, f"Trying to add DPanswers of different queries: {self.query}, {other.query}"
        self.unzipDPanswer()
        other.unzipDPanswer()
        assert self.DPanswer.shape == other.DPanswer.shape, f"Trying to add DPanswers of different shapes: {self.DPanswer}, {other.DPanswer}"

        # These are not fundamentally meaningless like the ones above, but should not happen in current logic, so checking them too
        # (And should generally be there, because of parallel composition)
        assert self.epsilon == other.epsilon, f"Trying to add DPanswers of queries with different epsilons: {self.epsilon}, {other.epsilon}"
        # Variances can actually be different in the middle of reduce function, when, say, adding a node to a sum of two nodes, so not checking
        # assert self.Var == other.Var, f"Trying to add DPanswers of nodes with different variances: {self.Var}, {other.Var}"

        # Add DP answers and variances
        dpm = DPMechanism()
        dpm.protected_answer = self.DPanswer + other.DPanswer
        if isinstance(dpm.protected_answer, multiSparse):
            dpm.protected_answer = dpm.protected_answer.toDense()
        dpm.variance = self.Var + other.Var
        dpm.epsilon = self.epsilon
        dpm.sensitivity = self.mechanism_sensitivity

        return DPquery(self.query, dpm).zipDPanswer()

    def __radd__(self, other):
        """ Let regular __add__ take care of raising the TypeError"""
        return self.__add__(other)

    def poolAnswers(self, other):
        """ Pool DP answers for two DPqueries (for the same query), with weights minimizing the variance"""
        assert self.name == other.name, f"Trying to pool DPanswers of different queries: {self.name}, {other.name}"
        assert self.query == other.query, f"Trying to pool DPanswers of different queries: {self.query}, {other.query}"
        alpha = self.Var / (self.Var + other.Var)
        self.DPanswer = alpha * other.DPanswer + (1. - alpha) * self.DPanswer
        self.Var = alpha ** 2 * other.Var + (1. - alpha) ** 2 * self.Var
        return self

    def zipDPanswer(self):
        """Zip DPAnswer numpy array to a byte string with zipfile.ZIP_DEFLATED"""
        if isinstance(self.DPanswer, bytes) or isinstance(self.DPanswer, multiSparse) or self.DPanswer is None:
            # Already zipped or is not to be zipped, just return
            return self
        if isinstance(self.DPanswer, np.ndarray):
            with io.BytesIO() as compressed:
                np.savez_compressed(compressed, self.DPanswer)
                self.DPanswer = compressed.getbuffer().tobytes()
            return self
        raise TypeError(f"Trying to zip DPanswer that is neither zipped (bytes type) nor unzipped (np.ndarray)")

    def unzipDPanswer(self):
        """Unzip zipped numpy array DPanswer"""
        if isinstance(self.DPanswer, np.ndarray) or isinstance(self.DPanswer, multiSparse) or self.DPanswer is None:
            # Not zipped, just return
            return self
        if isinstance(self.DPanswer, bytes):
            self.DPanswer = np.load(io.BytesIO(self.DPanswer))['arr_0']
            return self
        raise TypeError(f"Trying to unzip DPanswer that is neither zipped (bytes type) nor unzipped (np.ndarray)")


class StackedConstraint:
    """
    A representation of a common constraint across multiple geographies,
    Inputs:
        constraints: a list of common constraints
        indices: list of indexes indicating which geos that have the constraint
    """

    def __init__(self, constraints_indices: Iterable[Tuple[Constraint, int]]):
        """

        :type constraints_indices: Iterable[Tuple[Constraint, int], ...]
        :param constraints_indices: iterable of (constraint, index) tuples, where constraints are those common for the nodes,
            and index of those nodes in the list of children in the optimization problem
        """
        constraints, self.indices = zip(*constraints_indices)

        self.query = constraints[0].query
        self.sign = constraints[0].sign
        self.name = constraints[0].name

        self.rhsList = [c.rhs for c in constraints]

        ### These asserts take too much time (~5% of total (+5% in StackedDPquery)), so they are off
        # self.rhsList = []
        # for c in constraints:
        #     assert c.query == self.query
        #     assert c.sign == self.sign
        #     assert c.name == self.name
        #     self.rhsList.append(c.rhs)

    # NOTE: replace counter in here with self.indices (which contains indices for children w/ positive weight on query)
    def check(self, data, tol=0.001):
        """
        Check the constraints using data across all geos (child)
        Inputs:
            data: multidimensional numpy array where the last dimension is geography
            tol: the tolerance for the check
        Outputs:
            check_list: a list of bools indicating if the constraint was passed for each georaphy in the indices
        """
        data_list = np_utils.sliceArray(data)
        check_list = []
        for i, index in enumerate(self.indices):
            constraint = Constraint(self.query, self.rhsList[i], self.sign)
            check_list.append(constraint.check(data_list[index], tol=tol))
        return check_list


class StackedDPquery:
    """
    A representation of common DPqueries across geographies
    Inputs:
        DPqueries: a list of common DPqueries
        indices: list of indexes indicating which geos that have the DPqueries
    """

    def __init__(self, dpqueries_indices: Iterable[Tuple[DPquery, int]]):
        """
        :type dpqueries_indices: Iterable[Tuple[DPquery, int], ...]
        :param dpqueries_indices: iterable of (DPquery, index) tuples, where dp_queries are those common for the nodes,
            and index of those nodes in the list of children in the optimization problem
        """
        dp_queries, self.indices = zip(*dpqueries_indices)

        self.query = dp_queries[0].query
        self.epsilon = dp_queries[0].epsilon
        self.DPmechanism = dp_queries[0].DPmechanism
        # self.Var = dp_queries[0].Var
        self.name = dp_queries[0].name

        self.DPanswerList = [dpq.DPanswer for dpq in dp_queries]
        self.VarList = [dpq.Var for dpq in dp_queries]
        self.epsilonList = [dpq.epsilon for dpq in dp_queries]

        ### These asserts take too much time (~5% of total (+5% in StackedConstraint)), so they are off
        # self.DPanswerList = []
        # for dpq in dp_queries:
        #     assert dpq.query == self.query
        #     assert dpq.name == self.name
        #
        #     # These don't necessarily have to be the same conceptually, but should not happen in current program logic
        #     assert dpq.epsilon == self.epsilon
        #     assert dpq.DPmechanism == self.DPmechanism
        #
        #     # Variance can be different in the nodes of the same level if we had pooled data from lower levels before, so not checking
        #     # assert dpq.Var == self.Var
        #
        #     self.DPanswerList.append(dpq.DPanswer)

    @property
    def Var(self):
        """ For backward compatibility. Raise and error when Var attribute is called"""
        raise NotImplementedError("Stacked DPQuery no longer has Var attribute, since different children can have different variances. Use VarList.")

    @Var.setter
    def Var(self, value):
        """ For backward compatibility. Raise and error when Var attribute is called"""
        raise NotImplementedError("Stacked DPQuery no longer has Var attribute, since different children can have different variances. Use VarList.")


class StackedQuery:
    """
    A representation of common AbstractLinearQueries across geographies (usually child geounits of some parent geounit)
        (note: usually you will want StackedDPquery; this class is for special situations in which PLB is not expended
        on the queries but their representation is needed nonetheless, e.g., in some Rounders)
    Inputs:
        queries: a list of common queries
        indices: list of indexes indicating which geounits have the queries
    """

    #TODO: pull out common components of this & StackedDPquery into a parent class
    def __init__(self, queries_indices: Iterable[Tuple[AbstractLinearQuery, int]]):
        """
        :param queries_indices: Iterable of (AbstractLinearQuery, index) tuples, where AbstractLinearQueries are common for the
            nodes, and index of geonodes in the list of children in the optimization problem
        """
        queries, self.indices = zip(*queries_indices)
        self.query = queries[0]
        self.name = queries[0].name
