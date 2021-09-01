import programs.queries.querybase as querybase
import das_utils as du
import numpy as np
import pandas
import itertools
import re
from typing import Union, Tuple, List, Iterable, Dict

import programs.schema.tabletools as tabletools

from constants import CC

MAX_SEED_PRINT = 100

###############################################################################
# Schema class
###############################################################################


class Schema:
    """
    Schema houses information related to a specific histogram
    
    Primary functions include:
        1. Creating querybase objects for computing query answers on the histograms
        2. Extracting, combining, and crossing names of the levels that refer to the structure of the query asked
    """

    def __init__(self, name: str, dimnames: List[str], shape: Tuple[int, ...], recodes=None, levels=None):
        """
        create a schema object
        
        Inputs:
            name: the name of the query
            dimnames: a list of strings; names of the dimensions in the histogram
            shape: tuple/list of integers indicating the number of levels in each dimension
            querytree: a class that contains query definitions specific to this schema
            levels: a dictionary of the form (key, value) = (dimname, list of level names)
        """
        self.name: str = name

        for dimname in dimnames:
            if dimnames.count(dimname) > 1:
                raise ValueError(f"Repeated variable name ({dimname}) in schema {name}")
        self.dimnames: List[str] = dimnames
        self.mangled_dimnames = [f"{x}_{self.name}" for x in self.dimnames]
        self.shape = shape
        self.levels = self._getLevels(levels) if levels is not None else getDefaultLevels(self.dimnames, self.shape)
        self.recodes = recodes if recodes is not None else {}
        self.base_queries = self._getBaseQueries()
        self.size = int(np.prod(self.shape))
        try:
            self.tabledict = tabletools.getTableDict(self.name)
        except AssertionError:
            self.tabledict = {}
    
    def __repr__(self):
        
        query_str = f"  3. There are {len(self.base_queries)} 'base' queries in this schema."

        if len(self.base_queries) > MAX_SEED_PRINT:
            seed_names = self.base_queries[0: MAX_SEED_PRINT]
            # too_many_seeds = True
            additional_query_str = f".. the first {len(seed_names)} are shown above."
            query_str = f"{query_str}{additional_query_str}"
        else:
            seed_names = self.base_queries
            # too_many_seeds = False
        
        seeds = [self.getQuerySeed(x) for x in seed_names]
        seed_dicts = []
        for seed in seeds:
            if seed.name == "detailed":
                dimensions = "{all dims}"
            else:
                dimensions = ", ".join(seed.keepdims)
            
            seed_dicts.append(
                {
                    'query':      seed.name,
                    'dimensions': dimensions,
                    'recode':     False if seed.name in self.dimnames + ['total', 'detailed'] else True,
                    'crossable':  False if seed.name in ['total', 'detailed'] else True,
                    'size':       seed.getShape(size=True),
                }
            )
            
        query_string = pandas.DataFrame(seed_dicts)[['query', 'dimensions', 'size', 'recode', 'crossable']].to_string()
        items = [
            f"Schema: {self.name}",
            f"Dimnames: {self.dimnames}",
            f"Shape: {self.shape}",
            f"Size: {self.size}",
            f"Tables: {list(self.tabledict.keys())}",
            f"Base Queries: \n{query_string}\n",
            f"NOTES:",
            f"  1. 'Base' queries that do not share a dimension can be crossed using asterisks (*) in-between queries.",
            f"  2. The 'total' and 'detailed' queries are always available; they cannot be crossed with any other queries.",
            query_str,
        ]
        
        return "\n".join(items)
    
    def _getLevels(self, custom_levels):
        """
        checks to make sure the custom levels are compatible with the definitions
        within this schema; if not, incompatible dimensions are replaced by
        default levels
        
        Inputs:
            custom_levels (dict): a dictionary of levels defined by the user
        
        Outputs:
            a dictionary that is compatible with this schema
        """
        default_levels = getDefaultLevels(self.dimnames, self.shape)
        schema_levels = {}
        for d, dim in enumerate(self.dimnames):
            if dim not in custom_levels:
                schema_levels[dim] = default_levels[dim]
            else:
                if len(custom_levels[dim]) != self.shape[d]:
                    schema_levels[dim] = default_levels[dim]
                else:
                    schema_levels[dim] = custom_levels[dim]
        
        return schema_levels
    
    def _getBaseLevels(self):
        """
        helper function that returns the levels associated with queries that
        can be combined to form more complex queries (including recodes) and
        the standard 'total' and 'detailed' query names
        
        Outputs:
            a dictionary of levels
        """
        levels = {}
        for dim in self.dimnames:
            levels[dim] = self.levels[dim]
        
        for dim in self.recodes:
            recode = self.recodes[dim]
            if 'levels' in recode:
                levels[dim] = recode['levels']
        
        return levels
    
    def _getBaseQueries(self):
        """
        helper function that returns the names associated with queries that
        can be combined to form more complex queries (including recodes) and
        the standard 'total' and 'detailed' query names
        
        Outputs:
            a list of query names
        """
        return ["detailed", "total"] + self.dimnames + list(self.recodes.keys())
    
    def _getCrossableQueries(self):
        return self.dimnames + list(self.recodes.keys())
    
    def saveAsJSON(self, path):
        """
        save this schema object in the JSON format
        """
        du.saveJSONFile(path, self.__dict__, indent=4)
    
    def removeRecode(self, name):
        """
        removes the recode (if it exists) from this schema
        
        Inputs:
            name (str): the name of the recoded query to remove
        """
        recodes = {}
        for k, v in self.recodes.items():
            if k != name:
                recodes[k] = v
        
        self.recodes = recodes
        self.base_queries = self._getBaseQueries()
    
    def addRecode(self, name, groupings, overwrite=False):
        assert name in self.recodes and overwrite or name not in self.recodes, f"Cannot add recode {name} because it already exists. Use overwrite=True to ignore this warning."
        recode_dict = {
            'keepdims': list(groupings.keys()),
            'groupings': {},
            'levels': {},
        }

        # unravel compact dictionary recode definition
        for dim in recode_dict['keepdims']:
            group = groupings[dim]
            if group is not {}:
                recode_dict['levels'][dim], recode_dict['groupings'][dim] = [list(x) for x in zip(*group.items())]
        
        self.recodes[name] = recode_dict
        self.base_queries = self._getBaseQueries()
    
    # def addRecode_old(self, name, groupings={}, keepdims=[], levels={}, overwrite=False):
    #     """
    #     adds a new variable / transformation of dimension(s) to the query building block list
    #
    #     Inputs:
    #         name: string; what the dim transformation is called
    #         recode_dict: a dictionary containing the definition for the recoded variable(s)
    #         overwrite: boolean; determines whether or not the definition of the recoded variable(s) can be overwritten/changed
    #
    #     Outputs:
    #         None
    #
    #     Notes:
    #         The recode_dict contains a subset of the following fields:
    #             'groupings': a dict of groupings based on the base queries (i.e. dimnames); can be used for subsetting / grouping dimensions
    #             'keepdims': a list of strings that reference the base queries
    #             'levels': a dict of levels based on the base queries; typically used to pass in customized levels for groupings
    #     """
    #     assert name in self.recodes and overwrite or name not in self.recodes, "Cannot add recode because it already exists. Use overwrite=True to ignore this warning."
    #     recode_dict = {}
    #     recode_dict['groupings'] = groupings
    #     recode_dict['keepdims'] = keepdims
    #     recode_dict['levels'] = levels
    #     self.recodes[name] = recode_dict
    #     self.base_queries = self._getBaseQueries()
    
    def getQueryNames(self, nway=None, ignore=None, include=None):
        """
        returns a list of valid query names
        
        Inputs:
            nway: an int or list of ints that refer to the marginal querynames desired
            ignore: a str or list of strs that refer to the "crossable" dimensions to remove
                    from the queryname list
            include: a str or list of strs that refer to queries that must be part of the list (unless
                    the queries are invalid)
        
        Outputs:
            a list of strings referring to the queries asked for
        
        Notes:
            Not often used in practice; used primarily for testing purposes
            
            'detailed' always refers to the query expressed by the crosses between all of the dimname variables
            Example:
                if dimnames = ['a', 'b', 'c'] and there are two recoded variables ['a1', 'c6'], then, even though
                'a1_b_c', 'a_b_c6', and 'a_b_c' are all valid queries, only 'a_b_c' matches the crosses of the
                original (dimnames) variables, so it is the only one that will be renamed as 'detailed'
        """
        if nway is None:
            valid_names = self._getAllQueryNames()
        
        else:
            valid_names = []
            for n in du.aslist(nway):
                if isinstance(n, int):
                    combos = list(itertools.combinations(self._getCrossableQueries(), n))
                    if n == 0:
                        combos = ['total']
                    elif n == 1:
                        combos = [list(x)[0] for x in combos]
                    else:
                        combos = [CC.SCHEMA_CROSS_JOIN_DELIM.join(list(x)) for x in combos]
                    valid_names += combos
        
        nonignored_names = set(valid_names)
        if ignore is not None:
            for name in du.aslist(ignore):
                nonignored_names = nonignored_names.intersection(set([x for x in valid_names if name not in re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, x)]))
            valid_names = list(nonignored_names)
        
        if include is not None:
            for x in du.aslist(include):
                if not np.any([True if isSameQuery(x, y) else False for y in valid_names]):
                    valid_names.append(x)
            
        valid_names = list(set(valid_names))
        
        for i, name in enumerate(valid_names):
            detailed_name = CC.SCHEMA_CROSS_JOIN_DELIM.join(self.dimnames)
            if isSameQuery(name, detailed_name):
                valid_names[i] = "detailed"
        
        valid_names = [x for x in valid_names if self._validQuerySeed(x)]
        valid_names.sort(key=lambda s: len(re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, s)))
            
        return valid_names
    
    def _getAllQueryNames(self):
        """
        helper function that returns a list of all valid queries (including
        all valid query crosses)
        """
        crossable_queries = self.dimnames + list(self.recodes.keys())
        all_names = ['total', 'detailed']
        for nway in range(1, len(crossable_queries)):
            combos = list(itertools.combinations(crossable_queries, nway))
            if nway == 1:
                combos = [list(x)[0] for x in combos]
            else:
                combos = [CC.SCHEMA_CROSS_JOIN_DELIM.join(list(x)) for x in combos]
            all_names += combos
        
        all_names = list(set(all_names))
        all_names = [x for x in all_names if self._validQuerySeed(x)]
        
        return all_names

    def _validQuerySeed(self, queryname):
        """
        helper function that checks to see if a particular query can be built
        
        Outputs:
            a bool: True if the query name is valid, False otherwise
        """
        try:
            self.getQuerySeed(queryname)
            valid = True
        except AssertionError as err:
            msg = f"'{queryname}' is not a valid query in this schema" + err.args[0]
            # print(msg)
            # logging.error(msg)
            # valid = False
            raise ValueError(msg)
        
        return valid

    def standardize_querynames(self, querynames: Union[Iterable[str], str]):
        return [self.marginal_name(x) for x in querynames]

    @staticmethod
    def marginal_name(string_or_iter: Union[str, Iterable]):
        if isinstance(string_or_iter, str):
            return string_or_iter
        # This will throw TypeError if argument is not iterable, so no need to handle it ourselves
        return CC.SCHEMA_CROSS_JOIN_DELIM.join(string_or_iter)
    
    def isValidQuery(self, queryname):
        """
        returns whether or not this query can be built
        """
        return self._validQuerySeed(queryname)

    def getQuery(self, queryname: Union[Iterable[str], str]) -> querybase.AbstractLinearQuery:
        """
        returns a querybase object
        
        Inputs:
            queryname: a string referring to the name of the query to build
        
        Notes:
            queryname must come from the QueryTree's queries dictionary
            does not work with keywords
        """
        queryname = self.marginal_name(queryname)
        # assert len(queryname) == 1, "getQuery can only handle a single query. If multiple are needed, use getQueries"
        # queryname = queryname.pop()
        seed: QuerySeed = self.getQuerySeed(queryname)
        query = self.buildQuery(seed)
        return query
    
    def getQueries(self, querynames):
        """
        returns a dictionary of querybase objects
        
        Inputs:
            querynames: a single string or list of strings referring to the queries to build
        """
        querynames = self.standardize_querynames(querynames)
        queries = {}
        querynames = du.aslist(querynames)
        querynames = self.getCustomQuerynames(querynames)
        for name in querynames:
            queries[name] = self.getQuery(name)
        
        return queries
    
    def getQueryLevels(self, querynames: Union[Iterable[str], str], order=None, flatten=True, cross_marker=" BY "):
        """
        returns a dictionary of arrays corresponding to the levels found in the querynames
        
        Inputs:
            querynames: a string or list of strings
            order: the order of the dimensions for creating crosses
                   the default is None, which sets the order to be the ordering of the dimnames attribute
            flatten: boolean. If True, return the levels as a flattened array
                              If False, return the levels as a multdimensional numpy array
        """
        querynames = self.standardize_querynames(querynames)
        qlevels = {}
        order = self.dimnames if order is None else order
        querynames = du.aslist(querynames)
        querynames = self.getCustomQuerynames(querynames)
        for name in querynames:
            if self.isValidQuery(name):
                seed = self.getQuerySeed(name)
                qlevels[name] = seed.getQueryLevels(order=order, flatten=flatten, cross_marker=cross_marker)
        
        return qlevels
    
    def getQueryLevel(self, queryname: Union[Iterable[str], str], order=None, flatten=True):
        """
        returns a flattened numpy array of levels found in the queryname
        
        Inputs:
            queryname: a single string
            order: the order of the dimensions for creating crosses
                   the default is None, which sets the order to be the ordering of the dimnames attribute
            flatten: boolean. If True, return the levels as a flattened array
                              If False, return the levels as a multdimensional array
        """
        queryname = self.marginal_name(queryname)
        # assert len(queryname) == 1, "getQueryLevel can only handle a single query. If multiple are needed, use getQueryLevels"
        # queryname = queryname.pop()
        assert isinstance(queryname, str), "Queryname must be a string (only one query's levels can be accessed at a time using this function)."
        return self.getQuerySeed(queryname).getQueryLevels(order=order, flatten=flatten)
    
    def getQueryShape(self, queryname, size=False):
        queryname = self.marginal_name(queryname)
        # assert len(queryname) == 1, "getQueryShape can only handle a single query at a time"
        # queryname = queryname.pop()
        shape = self.getQuerySeed(queryname).getShape(size=size)
        return shape

    def getQuerySeed(self, queryname):
        """
        returns a query seed object associated with queryname
        
        Inputs:
            queryname (str): the name of the query
        
        Outputs:
             a QuerySeed object
        """
        if isinstance(queryname, str):
            bases = re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, queryname)
        elif hasattr(queryname, "__iter__"):
            bases = queryname
        else:
            raise TypeError(f"Query name has to be string or iterable. Query name provided is {queryname} of type {type(queryname)}")
            # raise TypeError(f"Query name has to be a string. Query name provided is {queryname} of type {type(queryname)}")

        all_bases = self._getBaseQueries()
        for base in bases:
            assert base in all_bases, f"The query is invalid for this schema.\nThe base dimension '{base}' doesn't exist; add it to the recodes dictionary.\nIf '{base}' is the name of a table, expand it using getCustomQuerynames and build each query individually."
        
        if len(bases) == 1:
            base = bases[0]
            if base in self.dimnames:
                seed = self.buildDimQuerySeed(base)
            elif base == "detailed":
                seed = self.buildDetailedQuerySeed()
            elif base == "total":
                seed = self.buildTotalQuerySeed()
            else:
                seed = self.buildRecodeQuerySeed(base)
        else:
            seed = self.buildCrossedQuerySeed(bases)
        
        assert seed is not None, "Seed creation failed."
        return seed
    
    def buildQuery(self, seed):
        """
        returns a querybase object
        
        Inputs:
            seed: a QuerySeed object
        
        Notes:
            the qtype is a string that refers to the querybase object to build
            to change the type of querybase object, specify a different qtype in the query definition
        """
        qtype = seed.qtype
        if qtype == "tabularGroupQuery":
            array_dims = self.shape
            groupings = self.getGroupings(seed.groupings)
            add_over_margins = self.getAddOverMargins(seed.keepdims)
            name = seed.name
            query = makeTabularGroupQuery(array_dims, groupings, add_over_margins, name)
        else:
            query = None
        
        assert query is not None, "Could not build query"
        
        return query
    
    def getGroupings(self, groupings):
        """
        translates the human-readable grouping dictionary into one that querybase's QueryFactory can use to build query objects
        
        Inputs:
            groupings: a dictionary of groupings specified in a user-friendly, human-readable way
        
        Outputs:
            a dictionary of groupings specified in a way that querybase's QueryFactory will better understand
        
        Notes:
            Example:
            Human-readable grouping:
                groupings = {
                    "hhgq": [[0], [1,2,3,4], [5,6,7]],
                    "ethnicity": [[0]]
                }
            
            Translates to:
            
            querybase QueryFactory-readable grouping:
                groupings = {
                    0: [[0], [1,2,3,4], [5,6,7]],
                    2: [[0]]
                }
        """
        return getGroupings(self.dimnames, groupings)
    
    def getAddOverMargins(self, keepdims):
        """
        returns a tuple of dimensions to add over
        
        Inputs:
            keepdims: a list of strings indicating dimensions to keep
        
        Notes:
            It's more user-friendly to state which dimensions to keep rather than
            think about which ones to add over. The sets are complements of each other.
            This function determines the complement of keepdims and translates it
            into integers that are used by numpy arrays to sum over dimensions we don't
            want to keep.
            
            Example:
                dimnames = ["hhgq", "voting", "ethnicity", "cenrace"]
                keepdims = ["voting", "cenrace"]
                add_over_margins = (0, 2) <= we want to add up/marginalize the hhgq and ethnicity dimensions since
                                             we only want to keep the voting and cenrace dimensions
        """
        return getAddOverMargins(self.dimnames, keepdims)
    
    def buildCrossedQuerySeed(self, bases):
        """
        constructs a query seed with the information of (potentially) many
        QuerySeed objects that do not share a dimension
        
        Inputs:
            bases: a list of query names that will be crossed / combined
        
        Outputs:
            a QuerySeed object
        """
        seed = None
        for i, base in enumerate(bases):
            assert base in self.dimnames + list(self.recodes.keys()), f"No dimensions/variables can be crossed with 'detailed' or 'total'"
            if base in self.dimnames:
                base_seed = self.buildDimQuerySeed(base)
            else:
                base_seed = self.buildRecodeQuerySeed(base)
            
            if i == 0:
                seed = base_seed
            else:
                seed.mergeQuerySeed(base_seed)
        
        assert seed is not None, "Seed creation failed."
        return seed
    
    def buildDimQuerySeed(self, queryname):
        """
        builds a query seed with the information encoded in the
        dimnames and shape attributes of the schema
        
        Inputs:
            queryname (str): one of the elements in the dimnames list
        
        Outputs:
            a QuerySeed object
        """
        assert queryname in self.dimnames, "Queryname isn't a valid base query"
        return QuerySeed(name=queryname, schema_dimnames=self.dimnames, keepdims=[queryname], levels={f"{queryname}": self.levels[queryname]})
    
    def buildRecodeQuerySeed(self, queryname):
        """
        builds a query seed with the information encoded in the recodes
        dictionary of the schema
        
        Inputs:
            queryname (str): a name in the recodes dictionary
        
        Outputs:
            a QuerySeed object
        
        Notes:
            recodes are custom-built variables that are oftentimes some kind
            of grouping or marginal of one or more of the dimnames dimensions
        """
        recode = self.recodes[queryname]
        groupings = recode["groupings"] if "groupings" in recode else {}
        keepdims = recode["keepdims"] if "keepdims" in recode else []
        levels = recode["levels"] if "levels" in recode else {}
        levels = self.getGroupingLevels(levels, groupings, keepdims)
        
        return QuerySeed(name=queryname, schema_dimnames=self.dimnames, groupings=groupings, keepdims=keepdims, levels=levels)
    
    def getGroupingLevels(self, customlevels, groupings, keepdims):
        """
        returns a dictionary of levels for groupings
        this function is primarily used in the buildRecodeQuerySeed function
        
        Inputs:
            customlevels (dict): levels as defined by the user (and specified
                                 in the recode's levels attribute)
            groupings (dict): the groupings defined for the recoded variable
            keepdims (list): the dimensions in dimnames to keep (marginalize the others)
        
        Outputs:
            a dictionary of levels
        """
        levels = {}
        baselevels = self._getBaseLevels()
        for dim in keepdims:
            if dim in groupings:
                if dim in customlevels:
                    levels[dim] = customlevels[dim]
                else:
                    items = []
                    dimgroups = groupings[dim]
                    # in order to make automatic level-generation work for groupings, each group needs to be
                    # part of a list within the dim group.
                    # For example, it's simpler to write { 'dim0': [1,2,3,4,5] } than
                    #                                    { 'dim0': [[1],[2],[3],[4],[5]] }
                    # for a dimension dim0 that has 6 levels, but where we
                    # want to ignore the first on.
                    # As such, when the levels are automatically generated (i.e. no custom levels have been specified)
                    # then the first dictionary above is automatically translated into the second here
                    # This also works fine for other dimension groupings, such as { 'dim1': [[1],[2,3,4]] }
                    # since it is translated into the exact same thing
                    dimgroups = [du.aslist(x) for x in dimgroups]
                    
                    for j in range(len(dimgroups)):
                        items.append(".".join([x for i, x in enumerate(baselevels[dim]) if i in dimgroups[j]]))
                    levels[dim] = items
            else:
                levels[dim] = baselevels[dim]
        
        return levels
    
    def buildTotalQuerySeed(self):
        """
        construct the total query seed; keep none of the dimensions / marginalize all dimensions
        """
        return QuerySeed(name="total", schema_dimnames=self.dimnames, keepdims=[], levels=np.array(["total"]))
    
    def buildDetailedQuerySeed(self):
        """
        build the detailed query seed; keep all dimensions / marginalize none of the dimensions
        """
        return QuerySeed(name="detailed", schema_dimnames=self.dimnames, keepdims=self.dimnames, levels=self.levels)
    
    def getFSName(self, queryname):
        """
        changes the ui name (e.g. "votingage * cenrace") into a filesystem-safe name (e.g. "votingage.cenrace")
        """
        return self.getQuerySeed(queryname).getFSName()

    @staticmethod
    def getUIName(fsname):
        """
        changes the filesystem-safe name (e.g. "votingage.cenrace") into the ui name (e.g. "votingage * cenrace")
        """
        return CC.SCHEMA_CROSS_JOIN_DELIM.join(fsname.split("."))
        
    def getReplaceDict(self, queryname, labels=True):
        queryname = self.marginal_name(queryname)
        return self.getQuerySeed(queryname).getReplaceDict(labels=labels)

    def getCustomQuerynames(self, querynames):
        querynames = self.standardize_querynames(querynames)
        queries = []
        querynames = du.aslist(querynames)
        for name in querynames:
            if name in self.tabledict:
                queries += self.tabledict[name]
            else:
                if self.isValidQuery(name):
                    queries += [name]
                else:
                    print(f"Removing '{name}' from the list of queries.")
        
        queries = np.unique(queries).tolist()
        return queries
    
    def getTableTuples(self, querynames):
        tuples = []
        querynames = self.getCustomQuerynames(querynames)
        for query in querynames:
            tuples += [(query, level) for level in self.getQueryLevel(query)]
        return tuples
        
    def getTable(self, querynames):
        tuples = self.getTableTuples(querynames)
        queries, levels = [list(x) for x in zip(*tuples)]
        tabledict = {'query': queries, 'level': levels}
        return pandas.DataFrame(tabledict)        
        
    def getQuerySparkSQL(self, query, labels=False):
        """
        Returns a dictionary of Spark SQL expressions for recoding schema dimensions into
        the levels for answering this query.

        Parameters
        ==========
        query : str
            The name of the query

        labels : bool (default = False)
            True: Use the level's string rather than the level's index in the dimension
            False: Use the levels' index rather than the level's string in the dimension
        """
        return self.getQuerySeed(query).getSparkSQL(labels)


###############################################################################
# QuerySeed class
###############################################################################
class QuerySeed:
    """
    a query seed is an object that contains all the information needed to build a querybase object
    """
    def __init__(self, name: str, schema_dimnames: List[str], groupings=None, keepdims=None, levels=None, qtype="tabularGroupQuery"):
        """
        create a query seed
        
        Inputs:
            name: a string that will be the name of the query
            groupings: a dictionary with (key, value) = (dimname, list of lists of integers corresponding to the levels to keep)
            keepdims: a list of strings corresponding to the dimension to keep
            levels: a dictionary with (key, value) = (dimname, list of lists of strings associated with the levels being kept)
            qtype: a string that tells the Schema which querybase.QueryFactory function to use to make particular types of queries
                   the default is the "tabularGroupQuery" query
        """
        self.name = name
        self.schema_dimnames = schema_dimnames
        self.groupings = groupings if groupings is not None else {}
        self.keepdims: List[str] = keepdims if keepdims is not None else []
        self.levels: Dict[str, List[List[str]]] = levels if levels is not None else {}
        self.qtype: str = qtype
    
    def __repr__(self):
        items = [
            f"QuerySeed: {self.name}",
            f"groupings: {self.groupings}",
            f"keepdims: {self.keepdims}",
            f"levels: {du.pretty(self.levels)}",
            f"querybase query type: {self.qtype}"
        ]
        
        return "\n".join(items)
    
    def getShape(self, size=False):
        if self.name == "total":
            shape = np.array(["total"]).shape
        else:
            shape = tuple([len(x) for x in self.levels.values()])
            
        if size:
            shape = int(np.prod(shape))
        
        return shape
        
    def getFSName(self):
        """
        returns a "safe" filesystem version of the query's name
        
        Notes:
            The UI version of the query's name involves asterisks when crossing queries. The asterisk is
            not usually a valid character for writing to file, so the FS (filesystem) name replaces
            the asterisk with a period.
        
            Example:
                "age * race * hispanic" => "age.race.hispanic"
        """
        fsname = ".".join(re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, self.name))
        return fsname

    def canMerge(self, other):
        """
        determines whether or not this query seed can be merged with another query seed
        
        Inputs:
            seed (QuerySeed): the seed to merge with
        
        Outputs:
            a bool
        
        Notes:
            checks to see if keepdims overlaps; if so, then the seeds share
            at least one dimension and are therefore incompatible
        """
        return True if len(set(self.keepdims).intersection(set(other.keepdims))) == 0 else False
    
    def mergeQuerySeed(self, other):
        """
        merges the information from another query other with this one
        
        Inputs:
            other (QuerySeed): the other seed to merge with
        
        Outputs:
            self (allows for chaining, if desired)
        
        Notes:
            merging will fail if the seeds' keepdims overlap
            even though self is returned, this function modifies this object,
            so setting self to a new variable isn't necessary
            
            other = other.mergeQuerySeed(seed2) is the same as
            other.mergeQuerySeed(seed2)
        """
        assert self.canMerge(other), "Cannot merge query seeds that share dimensions"
        self.name = CC.SCHEMA_CROSS_JOIN_DELIM.join([self.name, other.name])
        # self.name = f"{self.name}_{other.name}"
        
        new_groupings = self.groupings.copy()
        new_groupings.update(other.groupings)
        self.groupings = new_groupings

        # The check for uniqueness throws error above, so is not really needed here, but in case that one is removed, this will assure correctness
        self.keepdims = list(set(self.keepdims).union(set(other.keepdims)))

        new_levels = self.levels.copy()
        new_levels.update(other.levels)
        self.levels = new_levels
        
        return self
    
    def getQueryLevels(self, order=None, flatten=True, cross_marker=" BY "):
        """
        returns a numpy array / list of query levels, crossed, if the query
        requires it
        
        Inputs:
            order (list; default=None): the order of the dimensions
            flatten (bool; default=True): if True, flattens the numpy array, otherwise it returns the array as is
        
        Outputs:
            a numpy array / list of query levels
        
        Notes:
            order is important if the crossed levels need to be in a different order;
            if None, the order is assumed to be the order of the elements in the schema's dimnames attribute
            Example:
                seed = schema.getQuerySeed("detailed")
                schema_dimnames = ['A', 'B', 'C']
                seed.getQueryLevels => ['A0 x B0 x C0', 'A0 x B0 x C1', 'A0 x B1 x C0', 'A0 x B1 x C1']
                
                new_order = ['B', 'C', 'A']
                seed.getQueryLevels(new_order) => ['B0 x C0 x A0', 'B0 x C1 x A0', 'B1 x C0 x A0', 'B1 x C1 x A0']
        """
        order = self.schema_dimnames if order is None else order
        if self.name == "total":
            levels = self.levels
        else:
            levels = crossLevels(self.levels, order, cross_marker=cross_marker)
        
        if flatten:
            levels = levels if isinstance(levels, list) else levels.flatten()
        
        return levels
        
    def getReplaceDict(self, labels=True):
        replace = {}
        for dim in self.keepdims:
            groupdict = {}
            levels = self.levels[dim]
            if not labels:
                levels = list(range(len(levels)))
            if dim in self.groupings:
                groups = self.groupings[dim]
            else:
                groups = [[i] for i in range(len(levels))]
            for g, group in enumerate(groups):
                for i in group:
                    groupdict[str(i)] = str(levels[g])
            replace[dim] = groupdict
        return replace

    def getSparkSQL(self, labels=False):
        """
        Returns a dictionary of Spark SQL expressions for recoding schema dimensions into
        the levels for answering this query.

        Parameters
        ==========
        labels : bool (default = False)
            True: Use the level's string rather than the level's index in the dimension
            False: Use the levels' index rather than the level's string in the dimension
        """
        sqldict = {}
        if self.name == 'total':
            sqldict = {}
        else:
            for dim in self.levels.keys():
                dimdict = getDimDict(self, dim, labels)
                sqldict[dim] = dimdict2sql(dim, dimdict)
        return sqldict


###############################################################################
# SQL-generating Helper Functions
###############################################################################
def getDimDict(seed, dim, labels=False):
    """
    make a column dictionary for one of the dimensions of a query seed. this dictionary is often used
    to recode a column in a Spark DataFrame either via SQL or a python user-defined function (udf)

    e.g. We may want to recode the DHCP_HHGQ schema's 'hhgq' dimension from:

    '0': 'Household',
    '1': 'Correctional facilities for adults',
    '2': 'Juvenile facilities',
    '3': 'Nursing facilities/Skilled-nursing facilities',
    '4': 'Other institutional facilities',
    '5': 'College/University student housing',
    '6': 'Military quarters',
    '7': 'Other noninstitutional facilities'

    to one where we only want 2 levels (Household and Group Quarters). To do this, we will use the query seed 'hhgq_2level':

    seed = "hhgq_2level"
    groupings: {'hhgq': [[0], [1, 2, 3, 4, 5, 6, 7]]}
    keepdims: ['hhgq']
    levels: {
        "hhgq": [
            "Household",
            "Group Quarters"
        ]
    }

    This function transforms the seed's information (for the 'hhgq' dimension) into the following dictionary:

    if labels == True:
    dim_dict = {
        '0': 'Household',
        '1': 'Group Quarters',
        '2': 'Group Quarters',
        '3': 'Group Quarters',
        '4': 'Group Quarters',
        '5': 'Group Quarters',
        '6': 'Group Quarters',
        '7': 'Group Quarters'
    }

    if labels == False:
    dim_dict = {
        '0': '0',
        '1': '1',
        '2': '1',
        '3': '1',
        '4': '1',
        '5': '1',
        '6': '1',
        '7': '1'
    }

    which allows us to easily make this recode using either SQL or a Python UDF

    Notes:
        - This function only generates a single dictionary at a time (one for each column), so if
          there are multiple dimensions in a query (i.e. using 'detailed' or some other crossed query), then
          a loop will need to be used.
    """
    dimdict = {}
    levels = seed.levels[dim]
    if not labels:
        levels = list(range(len(levels)))
    groups = seed.groupings.get(dim, [[i] for i in range(len(levels))])
    for g, group in enumerate(groups):
        for i in group:
            dimdict[str(i)] = levels[g]
    return dimdict


def dimdict2sql(dim, dimdict, join="\n"):
    """
    Takes a dimension dict and transforms it to a SQL statement for recoding levels
    """
    sql_when = ["case"]
    sql_when += [f"when {dim} = '{index}' then '{level}'" for index, level in dimdict.items()]
    sql_when += ["else '-1' end"]
    sql_when = join.join(sql_when)
    return sql_when


###############################################################################
# Module level functions
###############################################################################
def isSameQuery(name1, name2):
    return np.all(sorted(re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, name1)) == sorted(re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, name2)))


def sortMarginalNames(querynames):
    return sorted(CC.SCHEMA_CROSS_JOIN_DELIM.join(sorted(re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, q))) for q in querynames)


def getDefaultLevels(dimnames, shape):
    """
    returns a dictionary of default levels (key, value) = (dimname, list of level names of the form "dimname level"
    
    Parameters
    ========== 
    dimnames : a list of str
        Names of the dimensions
    
    shape: a tuple/list
        Number of levels in each dimension

    Example
    ==========
        dimnames = ["Apple", "Banana"]
        shape = (3, 2)

        what gets returned will be:
        levels = {
            "Apple": ["Apple_0", "Apple_1", "Apple_2"],
            "Banana": ["Banana_0", "Banana_1"]
        }
    """
    leveldict = {}
    for d, dim in enumerate(dimnames):
        leveldict[dim] = [f"{dim}_{level}" for level in range(shape[d])]
    
    return leveldict


def getGroupings(dimnames, groupings):
    """
    see Schema.getGroupings above
    """
    assert isinstance(groupings, dict), "groupings must be a dictionary"
    assert isinstance(dimnames, list), "dimnames must be a list"
    index_groupings = {}
    for dim, groups in groupings.items():
        index_groupings[dimnames.index(dim)] = groups
    
    return index_groupings


def getAddOverMargins(dimnames, keepdims):
    """
    see Schema.getAddOverMargins above
    """
    assert isinstance(keepdims, list), "keepdims must be a list"
    assert isinstance(dimnames, list), "dimnames must be a list"
    return tuple([dimnames.index(dim) for dim in dimnames if dim not in keepdims])


def crossLevels(levels, key_order, cross_marker=" BY "):
    """
    Crosses levels
    
    Parameters
    ==========
    levels : dict
        The (dimname, levels) to cross
    
    key_order : list of strings
        The order to cross the dimensions
    
    cross_marker : str (default = " BY ")
        The string that joins the crossed levels
    
    Returns
    =======
    a numpy array of crossed levels

    Notes
    =====
    - Most often the key_order will be the dimnames attribute of the schema
    """
    levels = [levels[key] for key in key_order if key in levels]
    shape = tuple([len(x) for x in levels])
    array = np.zeros(shape, dtype='object')
    
    for nd_index in np.ndindex(array.shape):
        cross = []
        for list_k, index_i in enumerate(nd_index):
            cross.append(str(levels[list_k][index_i]))
        
        cross_str = cross_marker.join(cross)
        array[nd_index] = cross_str
    
    return array


def unravelLevels(leveldict):
    levels = {}
    dims = list(leveldict.keys())
    for dim in dims:
        items = list(leveldict[dim].items())
        items.sort(key=lambda k: k[1][0])
        levels[dim] = [x[0] for x in items]
    
    return levels


def getSubsetTuples(dim_dict, subset_indices):
    """
    returns a list of tuples that represents a subset of dim_dict defined by subset_indices
    
    Inputs:
        dim_dict (dict): a dictionary of (key, value) = (label for the level, list of indices for the level)
                         Most often comes from one element in the dict returned by getLevels()
                         in the Schema_schemaname.py files
        subset_indices (list): a list of integers representing the levels to keep as part of the subset
    
    Outputs:
        a list of tuples (label, level)
    
    Notes:
        Easier and more robust than copying large lists of (label, level) pairs by hand
    """
    grouping = []
    items = list(dim_dict.items())
    
    for ind in subset_indices:
        for label, level in items:
            if level[0] == ind:
                grouping.append((label, level))
                break
    
    return grouping


def buildTestRowdicts(schema, geocode="0123456789abcdef", mangled_names=True, dimlist=None, add_flat_index=False):
    if dimlist is None:
        shape = schema.shape
    else:
        shape = []
        for i, dsize in enumerate(schema.shape):
            shape.append(dsize if i in dimlist else 1)
        shape = tuple(shape)
    rows = []
    for c, cell in enumerate(np.ndindex(shape)):
        rowdict = {'geocode': geocode}
        for i, level in enumerate(cell):
            if add_flat_index:
                rowdict['flat_index'] = str(c)
            if mangled_names:
                rowdict[f"{schema.dimnames[i]}_{schema.name}"] = str(level)
            else:
                rowdict[schema.dimnames[i]] = str(level)
        rows.append(rowdict)
    return rows


###############################################################################
# Query building functions
###############################################################################
def makeTabularGroupQuery(array_dims, groupings, add_over_margins, name):
    """
    returns a querybase TabularGroupQuery object
    
    Inputs:
        array_dims: the shape of the histogram
        groupings: a dictionary of (key, value) = (dim_index, list of lists corresponding to groupings of levels)
        add_over_margins: a tuple of dimension to add over/marginalize
        name: a string referring to the name of the query
    """
    return querybase.QueryFactory.makeTabularGroupQuery(array_dims, groupings, add_over_margins, name)


###############################################################################
# Loading Schema object from file
###############################################################################
def loadSchemaFromFile(path):
    """
    loads the schema from a json file
    """
    schema_dict = du.loadJSONFile(path)
    name = schema_dict['name']
    dimnames = schema_dict['dimnames']
    shape = schema_dict['shape']
    recodes = schema_dict['recodes']
    levels = schema_dict['levels']
    return Schema(name, dimnames, shape, recodes, levels)
