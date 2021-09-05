""" Implements Geographical Unit Node class"""
import logging
import warnings
import time
import os
import json
from typing import Dict, List, Tuple, Any, Union, Iterable
# from functools import reduce
# from operator import add
import numpy as np
# import scipy.sparse as ss
from programs.utilities.geo import GeoHierarchy

#from programs.optimization.gurobi_stats import model_info
from programs.constraints.constraint_dict import ConstraintDict
from programs.queries.constraints_dpqueries import DPquery
from programs.queries.querybase import AbstractLinearQuery
from programs.sparse import multiSparse
from programs.das_setup import DASDecennialSetup

from exceptions import Error, IncompatibleAddendsError, DASValueError
from constants import CC

import das_framework.ctools.clogging as clogging
import programs.dashboard as dashboard
# import constants as C

__HistData__ = Union[multiSparse, np.ndarray]

RECORD_CALL_STACK = False
MAX_JSON_STATS_LENGTH = 2400

# These have to be the same as attribute names (__slots__) and arguments of __init__ in the GeounitNode class
# these have been moreved to constants.py

from constants import RAW, RAW_HOUSING, SYN, UNIT_SYN, CONS, INVAR, GEOCODEDICT, GEOCODE, GEOLEVEL, DP_QUERIES, UNIT_DP_QUERIES, PARENTGEOCODE, DETAILED

class GeounitNode:
    """
    This is a simple class to better organize geounit node data.

    This class lives in RDDs on the Spark core nodes.

    Geounit node is constructed from (geocode, **argsDict) pair, where argsDict contains at least the keys:
        geocode: the geocode of the node
        geocode_dict: dictionary of {length:'level_name'} where length is length of the geocode corresponding to the geolevel 'level_name'

    Optionally:
        raw: np.array of the true values
        syn: np.array of the post-imputation synthetic detailed counts
        syn_unrounded : np.array of the mid-imputation pre-rounded synthetic detailed counts
        cons = ["const_name1":const1, "const_name2":const2] where const1 and const2 are querybase.Constraint objects
        invar = {"name1":invariant1, "name2":invariant2} where invariant1 and invariant2 are numpy arrays
        dp_queries = {"dp_name1":dp1, "dp_name2":dp2} where dp1 and dp2 are querybase.DPquery objects
        opt_dict: Dict[str, AbstractLinearQuery]  # dictionary with queries which a optimization targets

    Not used in constructor:
        geolevel: the geocode level (US, state, etc.)

    Properties:
        dp: The detailed DPquery, i.e. full histogram (returns dp_dpqueries["detailed"]) (setter will be removed)
        invar: returns _invar (setter checks type)
        cons: returns _cons (setter forces type)

    Methods:
        setGeolevel - sets geolevel from geocode
        setParentGeocode - sets parent geocode from geocode and geodict (just cutting geocode to appropriate length)
        fromDict - create GeounitNode form a dictionary
        __add__ - adds two nodes, the sum is a node with raw, syn and raw_housing histograms that are sums
        checkSameAttr - checks that attribute values of two GeounitNodes are identical
        sumAttr - returns sum of attr of two GeounitNodes, checking for existence in both first
        stripForSave - removes invariants, constraints and dp_queries (and syn_unrounded) to save pickled node
        deleteTrueArray - delete raw and raw_housing histograms, to save protected data
        checkTrueArrayIsDeleted - check that there is no raw data
        shiftGeocodesUp - replace geocode with parent geocode (and then recalculate parent geocode)
                          after geolevel-aggregation into an upper level node
        getEpsilon - get and sum all epsilons spent by the dp_queries (including the detailed one, dp) of the node to get epsilon spent by the node
        mixMeasurements - get measurements (for each of the queries) pooled with those of another node (usually, obtained by aggregating the children)
        addInReduce - add two nodes, summing the histograms (calling +) and then possibly also adding invariants and constraints and/or dp_queries
        addInvariants - and invariants (rhs) of two nodes
        makeAdditionalInvariantsConstraints - after aggregation, add inv&con for the level that nodes were aggregated to
        toDict - convert to dictionary for saving
        getBoundedDPPrivacyImpact - a check to make sure we are getting the privacy we think we are getting

    """
    # pylint: disable=too-many-instance-attributes
    # Seventeen is reasonable in this case.
    __slots__ = ["geocode", "geocodeDict", "geolevel", "parentGeocode", "raw", "raw_housing", "syn", "syn_unrounded", "unit_syn",
                 "_cons", "_invar", "dp_queries", "_unit_dp_queries", "rounder_queries", "query_ordering", "opt_dict"]
    geocode: str                # TODO: Make a geocode object that can do complex geographies
    raw: __HistData__
    raw_housing: __HistData__
    syn: __HistData__
    unit_syn: __HistData__
    syn_unrounded: __HistData__
    _cons: ConstraintDict          # Internal var (@property), used as "cons" with setter/getter
    _invar: Dict[str, np.ndarray]  # Internal var (@property), used as "invar" with setter/getter
    dp_queries: Dict[str, DPquery]
    _unit_dp_queries: Dict[str, DPquery]
    rounder_queries: Dict[str, AbstractLinearQuery]
    query_ordering: Dict[str, Dict[int, str]]  # two entries "L2" and "Rounder", each containing ordering of queries in multipass optimizations
    parentGeocode: str                         # TODO: deprecate
    geolevel: str                              # TODO: deprecate (make a method that calls a method on the geocode)
    opt_dict: Dict[str, AbstractLinearQuery]   # dictionary with queries which a optimization targets
    geocodeDict: Dict[int, str]                # TODO: deprecate (make a method on the geocode)

    def __init__(self, geocode, *, raw=None, raw_housing=None, syn=None, syn_unrounded=None, unit_syn=None,
                 cons=None, invar=None, dp_queries=None, unit_dp_queries=None, opt_dict=None, geocode_dict):

        # assert geocode is not None and len(geocode) >= 2,
        #         # 'construction of geounit node requires >=2-digit geocode string'

        self.geocode = geocode

        self.raw = raw
        self.raw_housing = raw_housing
        self.syn = syn
        self.syn_unrounded = syn_unrounded

        self.unit_syn = unit_syn

        # _invar and _cons are the internal variables:
        self._cons = None
        self._invar = None

        # invar and cons are properties; setters and getters defined below
        self.cons = cons
        self.invar = invar

        self.dp_queries = dp_queries
        self._unit_dp_queries = unit_dp_queries
        self.rounder_queries = None

        self.query_ordering = None

        self.parentGeocode = None
        self.geolevel = None

        self.geocodeDict = geocode_dict
        self.setParentGeocode()
        self.setGeolevel()

        self.opt_dict = opt_dict

    @classmethod
    def fromDict(cls, argdict: dict) -> 'GeounitNode':
        """
        Create GeounitNode from a dictionary. Drops the dictionary entries that are not taken by __init__ as
        arguments. May be enhanced for other checks / functionality.
        :param argdict:
        :return:
        """
        args_taken = [RAW, RAW_HOUSING, GEOCODE, GEOCODEDICT, CONS, INVAR, DP_QUERIES]
        argdict[CONS] = argdict.pop('_cons', None)
        argdict[INVAR] = argdict.pop('_invar', None)
        args = {key: value for key, value in argdict.items() if key in args_taken}
        args['geocode_dict'] = args.pop(GEOCODEDICT, None)
        return GeounitNode(**args)

    @property
    def cons(self) -> ConstraintDict:
        """Getter for cons. May be used to return empty dict"""
        if self._cons is None:
            return ConstraintDict({})
        return self._cons

    @cons.setter
    def cons(self, value):
        """Setter for cons. Makes sure it's ConstraintDict"""
        if value is None:
            self._cons = None
        else:
            self._cons = ConstraintDict(value)

    @property
    def invar(self) -> Dict[str, np.ndarray]:
        """Getter for invar. May be used to return empty dict"""
        if self._invar is None:
            return {}
        return self._invar

    @invar.setter
    def invar(self, value):
        """Setter for invar"""
        if value is None:
            self._invar = None  # {}
        else:
            for key, v in value.items():
                if not (isinstance(v, np.ndarray) or v == v + 0):
                    raise TypeError(f"Invariants dict [{key}] value {v} ({type(v)}) is not a numpy array, nor a number")
        self._invar = value

    @property
    def dp(self) -> DPquery:
        """ Returns the detailed query """
        if self.dp_queries is not None:
            try:
                return self.dp_queries[DETAILED]
            except KeyError:
                pass

    @dp.setter
    def dp(self, value):
        """
        Sets the dp query.
        Don't set it like this, set as part of self.dp_queries
        """
        #self.dp_queries[DETAILED] = value
        raise NotImplementedError("Set the value of the detailed query via dp_queries dictionary attribute, not via .dp attribute")

    @property
    def unit_dp_queries(self):
        """ For backward compatibility. If the attribute doesn't exist, return None"""
        try:
            return self._unit_dp_queries
        except AttributeError:
            return None

    @unit_dp_queries.setter
    def unit_dp_queries(self, value):
        """ Trivial setter"""
        self._unit_dp_queries = value

    def querySets2Stack(self):
        """ Returns list (as tuple) of all dp_query dicts, i.e. for all histograms. Here, the main and the unit"""
        query_set_list = []
        for dpq_dict in (self.dp_queries, self.unit_dp_queries):
            if dpq_dict is not None:
                query_set_list.append({k:v for k,v in dpq_dict.items() if k!=DETAILED})
            else:
                query_set_list.append(None)
        return query_set_list

    def __repr__(self):
        """
            printable str representation of a GeounitNode
        """
        raw_shape = str(self.getDenseRaw().shape) if self.raw is not None else "None"
        raw = str(self.getDenseRaw()) if self.raw is not None else ""
        raw_housing = str(self.getDenseRawHousing().shape) if self.raw_housing is not None else "None"
        output = f"--- geounit Node ---\n" \
            f"geocode: {self.geocode}, geolevel {self.geolevel}\n" \
            f"parent geocode: {self.parentGeocode}\n" \
            f"raw_shape: {raw_shape}\n" \
            f"raw: {raw}\n" \
            f"raw_housing: {raw_housing} \n" \
            f"dp: {self.dp}\n" \
            f"cons: {self.cons}\n" \
            f"invar: {self.invar}\n" \
            f"syn: {self.syn}\n" \
            f"syn_unrounded: {self.syn_unrounded}\n" \
            f"dp_queries: {self.dp_queries}\n"
        return output

    def __eq__(self, other):
        """
        Two nodes are equal if all attributes from the list are equal
        """
        for attr in set(self.__slots__) - {'_invar', SYN, RAW, RAW_HOUSING, UNIT_SYN}:
            if self.__getattribute__(attr) != other.__getattribute__(attr):
                return False

        # invar is dictionary of numpy arrays
        if self.invar.keys() != other.invar.keys():
            return False

        # If a single invariant is different, nodes are not equal
        for inv_name in self.invar.keys():
            array_comp_func = np.allclose if np.issubsctype(self.invar[inv_name], float) else np.array_equal
            if not array_comp_func(self.invar[inv_name], other.invar[inv_name]):
                return False

        # When raw/syn is np.array have to call np.array_equal
        if not np.array_equal(self.getDenseSyn(), other.getDenseSyn()):
            return False

        if not np.array_equal(self.getDenseRaw(), other.getDenseRaw()):
            return False

        if not np.array_equal(self.getDenseRawHousing(), other.getDenseRawHousing()):
            return False

        if not np.array_equal(self.getDenseSynHousing(), other.getDenseSynHousing()):
            return False


        return True

    def __add__(self, other: 'GeounitNode'):
        """
        Add two geounit nodes (when aggregating them in reduce operation to obtain the parent node).
        Check that they are the same geolevel, have same parentGeocode and geodict, and then sum
        their histograms and return a new node with these histograms
        :param other:
        :return:
        """
        for attr in (GEOLEVEL, GEOCODEDICT, PARENTGEOCODE):
            self.checkSameAttr(other, attr)

        sum_raw = self.sumAttr(other, RAW)
        sum_raw_housing = self.sumAttr(other, RAW_HOUSING)
        sum_syn = self.sumAttr(other, SYN)
        sum_syn_housing = self.sumAttr(other, UNIT_SYN)

        return GeounitNode(self.geocode, raw=sum_raw, raw_housing=sum_raw_housing, syn=sum_syn, unit_syn=sum_syn_housing, geocode_dict=self.geocodeDict)

    def checkSameAttr(self, other, attr):
        """Check that the addends have identical attribute (when otherwise addition is meaningless)"""
        selfattr = self.__getattribute__(attr)
        otherattr = other.__getattribute__(attr)
        if selfattr != otherattr:
            raise IncompatibleAddendsError("GeoNodes", attr, selfattr, otherattr)

    def sumAttr(self, other, attr):
        """
        Check that the addends have both either None or not None attribute (when otherwise addition is meaningless).
        Return sum of attributes if both not None
        """
        selfattr = self.__getattribute__(attr)
        otherattr = other.__getattribute__(attr)
        if selfattr is not None and otherattr is not None:
            return selfattr + otherattr
        elif selfattr is not None or otherattr is not None:
            msg = f"One of the GeoUnit-node addends has {attr} data, while the other does not ({GEOCODE}s: {self.geocode} and {other.geocode})"
            logging.warning(msg)
            warnings.warn(msg, RuntimeWarning)

    def setParentGeocode(self):
        """
        Takes the node's geocode and determines its parent's geocode
        """
        #TODO: deprecate
        mykeys = [key for key in self.geocodeDict.keys()]
        mykeys_array = np.array(mykeys)
        state_cutoff = np.min(mykeys_array[np.where(mykeys_array > 0)[0]])

        c_p_dict = {child: parent for child, parent in zip(mykeys[:-1], mykeys[1:])}

        geocode_len = len(self.geocode)
        try:
            parent_geocode_len = c_p_dict[geocode_len] if geocode_len > state_cutoff else ''
        except KeyError:
            raise DASValueError(
                f"Node has {GEOCODE} {self.geocode}, but the {GEOCODEDICT} {self.geocodeDict} does not refer to keys of length {geocode_len}", '')
        self.parentGeocode = self.geocode[:parent_geocode_len] if geocode_len > state_cutoff else CC.ROOT_GEOCODE

    def setGeolevel(self):
        """
        Takes the node's geocode and determines its geolevel
        """
        #TODO: deprecate
        geocode_len = len(self.geocode)
        try:
            self.geolevel = self.geocodeDict[geocode_len]
        except KeyError:
            raise DASValueError(
                f"Node has {GEOCODE} {self.geocode}, but the {GEOCODEDICT} {self.geocodeDict} does not refer to keys of length {geocode_len}", '')

    def stripForSave(self):
        """ Remove auxiliary information from the node"""
        self.syn_unrounded = None
        self.cons = None
        self.invar = None
        self.dp_queries = None

        return self

    def deleteTrueArray(self):
        """
        This function explicitly deletes the GeounitNode "raw" true data array.

        Input:
            node: a GeounitNode object

        Output:
            node: a GeounitNode object
        """
        self.raw = None
        self.raw_housing = None
        return self

    def checkTrueArrayIsDeleted(self):
        """
        This function checks to see if the node.raw is None for a GeounitNode object.
        If not it raises an exception.

        Input:
            node: a GeounitNode object
        """
        if self.raw is not None or self.raw_housing is not None:
            raise RuntimeError("The true data array has not been deleted")

    def setNewGeocode(self, self_code, geo_hierarchy: GeoHierarchy):
        """
        Returns changes geocode and returns self
        :param self_code: new geocode for self
        :param geo_hierarchy:
        :return: self
        """
        # TODO: remove geo_hierarchy, they were introduced for backwards comparitibility
        # once deprecated usage of node is removed, modify this function
        self.geocode = self_code
        self.geolevel = geo_hierarchy.getNameOfLevel(geo_hierarchy.getLevel(self_code))
        self.parentGeocode = geo_hierarchy.getParentGeocode(self_code)
        return self


    def shiftGeocodesUp(self):
        """
        This sets the parent geocode and the parent geolevel for a specific GeounitNode.

        Input:
            geocode_GeounitNode: This is a node that stores the geocode, raw data, and DP measurements for a specific geounit.

        Output:
            GeounitNode: This is a node that stores the geocode, raw data, and DP measurements for a specific geounit.

        """
        #TODO deprecate
        self.geocode = self.parentGeocode
        self.setParentGeocode()
        self.setGeolevel()
        return self

    def getEpsilon(self):
        """ Return the total epsilon spent on the node"""
        if not self.dp_queries:
            return {0: 0, 1: 0}
        budget_dict = {}
        for dpq in list(self.dp_queries.values()) + list(self.unit_dp_queries.values()):
            if dpq.budget_id not in budget_dict:
                budget_dict[dpq.budget_id] = 0
            budget_dict[dpq.budget_id] += dpq.epsilon
        return budget_dict

    def mixMeasurements(self, other):
        """ Mix noisy measurements with those from another node (e.g. aggregated from a lower level)"""
        if not self.dp_queries:
            return None
        assert set(self.dp_queries.keys()) == set(other.dp_queries.keys()), "DPqueries sets of two nodes mixing measurements are different"
        for qname in self.dp_queries.keys():
            self.dp_queries[qname] = self.dp_queries[qname].poolAnswers(other.dp_queries[qname])

        return self

    def addInReduce(self, other: 'GeounitNode', inv_con=True, add_dpqueries=False) -> 'GeounitNode':
        """
        Called in a reduce operation.
        Add two nodes (raw, raw_housing and syn histograms) and also invariants and constraints right hand sides (if inv_con),
        and dp query answers and variances (if add_dpqueries, used for subsequent measurements pooling).
        :param other: other addend, GeounitNode
        :param inv_con: whether to include summed constraints and invariants into the sum GeounitNode
        :param add_dpqueries: whether to include DPqueries (and the detailed, dp) into the sum GeounitNode
        :return:
        """
        # Create a new GeounitNode with raw, raw_housing and syn being sums
        sum_node: GeounitNode = self + other

        if inv_con:
            if self.cons and other.cons:  # Think on what actually should be checked here. None? Empty dict?
                sum_node.cons = self.cons + other.cons

            if self.invar and other.invar:  # Think on what actually should be checked here. None? Empty dict?
                sum_node.invar = self.addInvariants(other)

        if add_dpqueries:
            if self.dp_queries and other.dp_queries:
                assert set(self.dp_queries.keys()) == set(other.dp_queries.keys()), "DPqueries sets of two added nodes are different"
                sum_node.dp_queries = {qname: self.dp_queries[qname] + other.dp_queries[qname] for qname in self.dp_queries.keys()}

            if self.unit_dp_queries and other.unit_dp_queries:
                assert set(self.unit_dp_queries.keys()) == set(other.unit_dp_queries.keys()), "Unit DPqueries sets of two added nodes are different"
                sum_node.unit_dp_queries = {qname: self.unit_dp_queries[qname] + other.unit_dp_queries[qname] for qname in self.unit_dp_queries.keys()}

            # The following are not DP Queries just query objects, without the answers. Thus the answers don't have to be added together.
            # Still, we'll add them under add_dpqueries option (since it's needed for optimization) and make sure that it's the same set and copy it over into the aggregated node.
            if self.rounder_queries and other.rounder_queries:
                assert set(self.rounder_queries.keys()) == set(other.rounder_queries.keys()), "Rounder DPqueries sets of two added nodes are different"
                sum_node.rounder_queries = self.rounder_queries.copy()

            if self.opt_dict and other.opt_dict:
                assert set(self.opt_dict.keys()) == set(other.opt_dict.keys()), "Opt Dict queries sets of two added nodes are different"
                sum_node.opt_dict = self.opt_dict.copy()

            # These are dict (with key being optimizer, l2/rounder) of dicts of query names (keyed by order)
            if self.query_ordering and other.query_ordering:
                assert set(self.query_ordering.keys()) == set(other.query_ordering.keys()), "Query ordering of two added nodes is different"
                for optimizer_name in self.query_ordering:
                    if not (self.query_ordering[optimizer_name] and other.query_ordering[optimizer_name]):
                        continue
                    assert set(self.query_ordering[optimizer_name].keys()) == set(other.query_ordering[optimizer_name].keys()), f"Query ordering passes set of two added nodes is different for {optimizer_name}"
                    for passNum in self.query_ordering[optimizer_name]:
                        assert set(self.query_ordering[optimizer_name][passNum]) == set(other.query_ordering[optimizer_name][passNum]), f"Query ordering of two added nodes is different for {optimizer_name} for pass #{passNum}"
                sum_node.query_ordering = self.query_ordering
        return sum_node

    def addInvariants(self, other: 'GeounitNode') -> Dict[str, np.ndarray]:
        """
        For each invariant in the dictionary, this function adds the two invariant arrays together
        and returns a dictionary with the aggregated numpy.array invariants.

        Inputs:
            other: another GeounitNode, whose invar attribute is another addend

        Note: self and other must be at the same geolevel.

        Output:
            invar_sum: a dictionary containing the aggregated numpy.array invariants from self.invar and other.invar

        """
        # # This is checked in __add__, but leaving it here in case some logic changes
        # for attr in ("geolevel", "geocodeDict", "parentGeocode"):
        #     self.checkSameAttr(other, attr)

        if set(self.invar.keys()) != set(other.invar.keys()):
            raise IncompatibleAddendsError("Invariant dicts", "key set", set(self.invar.keys()), set(other.invar.keys()))

        invar_sum = {}
        for key in self.invar.keys():
            shape1 = np.array(self.invar[key]).shape
            shape2 = np.array(other.invar[key]).shape
            if shape1 != shape2:
                raise IncompatibleAddendsError(f"Invariant '{key}'", "shapes", shape1, shape2)
            invar_sum[key] = np.array(np.add(self.invar[key], other.invar[key]))

        return invar_sum

    def makeAdditionalInvariantsConstraints(self, setup: DASDecennialSetup) -> 'GeounitNode':
        """
        This function takes a node which has just been aggregated from a lower level and adds invariants and
        constraints that are indicated for the level it has been aggregated to.
        """

        ic_level = setup.inv_con_by_level[self.geolevel]

        invariants_dict = setup.makeInvariants(raw=self.raw, raw_housing=self.raw_housing, invariant_names=ic_level["invar_names"])
        if self._invar is not None:
            self._invar.update(invariants_dict)
        else:
            self.invar = invariants_dict

        constraints_dict = setup.makeConstraints(hist_shape=(self.raw.shape, self.raw_housing.shape), invariants=self.invar, constraint_names=ic_level["cons_names"])
        for cname in self.cons.keys():
            if cname in constraints_dict:
                from_lower = self.cons[cname]  # constraint coming from aggregating lower level
                for_this = constraints_dict[cname]  # constraint indicated for this level in config and calculated here
                assert for_this.sign == from_lower.sign, \
                    f"The constraint {cname} at {ic_level} level has a different sign ({for_this.sign}) than on lower level ({from_lower.sign})"
                le_less_tight = from_lower.sign == 'le' and np.any(from_lower.rhs < for_this.rhs)
                ge_less_tight = from_lower.sign == 'ge' and np.any(from_lower.rhs > for_this.rhs)
                if le_less_tight or ge_less_tight:
                    sign_str = '<' if le_less_tight else '>'
                    get_strictest = np.minimum if le_less_tight else np.maximum
                    msg = f"Constraints {cname} for {self.geolevel} for geocode {self.geocode} are less tight than below!"
                    f"Sign: {from_lower.sign}; Lower level rhs: {from_lower.rhs} {sign_str} This level rhs: {for_this.rhs}."
                    "Updating to the strictest"
                    constraints_dict[cname].rhs = get_strictest(from_lower.rhs, for_this.rhs)

                    logging.warning(msg)  # Note, this happens on executor, so an effort is needed to see this warning
                    # raise ValueError(msg)
        if self._cons is not None:
            self._cons.update(constraints_dict)
        else:
            self.cons = constraints_dict

        return self

    def histsAreEmpty(self):
        """
        Check that the node is empty. Used for empty parent node, to assign children SYN (optimized) histograms to zeros
        (NOTE: all histograms should be empty. Also, the sum check obviously works only if values are non-negative)
        :return:
        """
        return self.syn.sum() == 0 and self.unit_syn.sum() == 0

    def copyParentSyn(self, parent, keep_debug_info, zerosyn=False):
        """
        Set node SYN (optimized) histograms to those of the parent. Used when it's the only child.
        Also, when parent is empty, set the unrounded histogram
        :param parent:
        :param keep_debug_info:
        :param zerosyn:
        :return:
        """
        if not zerosyn:
            self.syn = parent.syn
            self.unit_syn = parent.unit_syn
        else:
            # Set to zeros if parent is zero
            self.syn = multiSparse(np.zeros(parent.syn.shape, dtype=int))
            self.unit_syn = multiSparse(np.zeros(parent.unit_syn.shape, dtype=int))
        if not keep_debug_info:
            # dp_queries (noisy measurements) take a lot of space / memory
            if self.dp_queries:
                self.dp_queries.clear()
            if self.unit_dp_queries:
                self.unit_dp_queries.clear()
        else:
            # If we want to keep dp_queries (noisy measurements) and unrounded results
            self.syn_unrounded = parent.syn_unrounded
        return self

    def stackDetailedDPAnswers(self, parent_shape):
        """
        Stack main and unit detailed dp answer into a single flat array.
        We don't take unit detailed measurements currently, so fill that with zeros.
        """
        dpa = self.dp_queries[DETAILED].unzipDPanswer().DPanswer
        dpa_dense = dpa.toDense() if isinstance(dpa, multiSparse) else dpa
        return np.hstack((dpa_dense, np.zeros(np.prod(parent_shape[1]), dtype=int)))

    def detailedWeight(self):
        if self.dp:
            return 1. / self.dp.Var, 0
        return 0, 0

    def filterToNZ(self, nz):
        """
        Filter detailed query dp_answer aka noisy child, leaving only cells that are non-zero in the parent, and cast as sparse array
        :param nz:
        :return:
        """
        try:
            dpa = self.dp_queries[DETAILED].unzipDPanswer().DPanswer
            self.dp_queries[DETAILED].DPanswer = multiSparse.fromJK(nz, dpa.ravel()[nz], dpa.shape)
        except KeyError:
            pass

        return self

    def removeConstraintByName(self, name):
        self._cons.pop(name, None)
        return self

    def getAllVariances(self):
        variances = []
        if self.dp_queries:
            variances.extend([query.Var for query in self.dp_queries.values()])

        if self.unit_dp_queries:
            variances.extend([query.Var for query in self.unit_dp_queries.values()])

        return variances

    def setDPQVar(self, func=lambda v: 1.0):
        """
        Sets the variance of each DPQuery to a value func(Var) (defaults to just set to 1.0)
        :param func: function to transform the variance, default just sets it to 1
        :return:
        """
        if self.dp_queries:
            for qname, query in self.dp_queries.items():
                self.dp_queries[qname].Var = func(query.Var)

        if self.unit_dp_queries:
            for qname, query in self.unit_dp_queries.items():
                self.unit_dp_queries[qname].Var = func(query.Var)
        return self

    def toDict(self, keep_attrs: Iterable[str]) -> dict:
        """
        Extract desired attributes from the node and place them in a dictionary.
        Primarily, for saving the RDDs with GeounitNodes, and so is called from within Spark map function.
        :return dict
        """
        if keep_attrs is None:
            keep_attrs = (GEOCODE, RAW, SYN)  # Default attributes to keep when saving
        node_dict = {}
        for attr in keep_attrs:
            item = self.__getattribute__(attr)
            if item is not None:
                node_dict[attr] = item
        return node_dict

    def checkConstraints(self, raw=True, return_list=False):
        """
        Check that all constraints on the node are satisfied
        :param raw: whether to check raw data (as opposed to synthetic)
        :param return_list: whether to return list of failed constraints (as opposed to just the check flag)
        :return: either check status or list of failed constraints
        """
        if raw:
            data = np.hstack((self.getDenseRaw().ravel(), self.getDenseRawHousing().ravel()))
        else:
            data = np.hstack((self.getDenseSyn().ravel(), self.getDenseSynHousing().ravel()))  # These have to be reshaped and stacked with the unit histogram for it to work
        check = True
        fail_list = []
        for c in self.cons.values():
            if not c.check(data):
                if not return_list:
                    return False
                check = False
                fail_list.append((c.name, c.query.answer(data), c.rhs, c.sign))

        if return_list and not check:
            return fail_list
        return check

    def getDenseRaw(self):
        """ Return dense array, regardless of whether dense or sparse is kept in the node"""
        if isinstance(self.raw, multiSparse):
            return self.raw.toDense()
        return self.raw

    def getDenseRawHousing(self):
        """ Return dense array, regardless of whether dense or sparse is kept in the node"""
        if isinstance(self.raw_housing, multiSparse):
            return self.raw_housing.toDense()
        return self.raw_housing

    def getDenseSyn(self):
        """ Return dense array, regardless of whether dense or sparse is kept in the node"""
        if isinstance(self.syn, multiSparse):
            return self.syn.toDense()
        return self.syn

    def getDenseSynHousing(self):
        """ Return dense array, regardless of whether dense or sparse is kept in the node"""
        if isinstance(self.unit_syn, multiSparse):
            return self.unit_syn.toDense()
        return self.unit_syn

    def getDense(self, histdata):
        if isinstance(histdata, multiSparse):
            return histdata.toDense()
        return histdata

    def getBoundedDPPrivacyImpact(self) -> (float, float):
        """
           Checks the privacy impact of the queries. For each node in a level, it gets the matrix representations m_i of each query i,
           and also the sensitivity s_i and epsilon e_i used. It computes sum_i eps_i/s_i * (1^T * abs(q_i)) * 2, which represents how much privacy budget
           is used in each cell of the resulting histogram. It then takes the max and min of the cells.
        """
        if not self.dp_queries:
            return None
        mydpqueries = list(self.dp_queries.values())
        domainsize = mydpqueries[0].query.domainSize()
        impact = np.zeros(shape=domainsize)
        for dpquery in mydpqueries:
            query = dpquery.query
            used_eps = float(dpquery.epsilon)
            used_sens = dpquery.mechanism_sensitivity
            impact += ((np.ones(query.numAnswers()) * np.abs(query.matrixRep())) * (2.0 * used_eps / used_sens))
        return impact.max(), impact.min()

    def zipNoisy(self):
        if self.dp_queries:
            for qname, query in self.dp_queries.items():
                self.dp_queries[qname] = query.zipDPanswer()

        if self.unit_dp_queries:
            for qname, query in self.unit_dp_queries.items():
                self.unit_dp_queries[qname] = query.zipDPanswer()
        return self

    def unzipNoisy(self):
        if self.dp_queries:
            for qname, query in self.dp_queries.items():
                self.dp_queries[qname] = query.unzipDPanswer()

        if self.unit_dp_queries:
            for qname, query in self.unit_dp_queries.items():
                self.unit_dp_queries[qname] = query.unzipDPanswer()
        return self

    def setOptDict(self, opt_dict):
        self.opt_dict = opt_dict

    def redefineGeocodes(self, geocode_dict):
        is_block_node = len(self.geocode) == max(list(self.geocodeDict.keys()))
        assert is_block_node, "redefineGeocodes cannot redefine geocodes to geocode16 format above the block geolevel."
        self.geocode = self.geocode[-16:]
        self.geocodeDict = geocode_dict
        self.setParentGeocode()
        self.setGeolevel()
        return self


class NodeMissingAttributeError(Error):
    def __init__(self, msg, attribute):
        Error.__init__(self, f"Node missing attribute '{attribute}': {msg}")
        self.msg = msg
        self.attribute = attribute
        self.args = (msg, attribute,)


def hasNodeAttr(n: Union[Dict[str, Any], GeounitNode], key) -> bool:
    """ Check if node has an attribute, whether node is in the shape of GeounitNode or a dict"""
    if isinstance(n, dict):
        return key in n
    else:
        return hasattr(n, key)


def getNodeAttr(n: Union[Dict[str, Any], GeounitNode], key) -> Any:
    """ Get node attribute, whether node is in the shape of GeounitNode or a dict"""
    if isinstance(n, dict):
        try:
            return n[key]
        except KeyError as err:
            raise NodeMissingAttributeError(str(err), key)
    else:
        try:
            return getattr(n, key)
        except AttributeError as err:
            raise NodeMissingAttributeError(str(err), key)
