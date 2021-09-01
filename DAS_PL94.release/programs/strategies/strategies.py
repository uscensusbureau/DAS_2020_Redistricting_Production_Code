from constants import CC
from fractions import Fraction as Fr
from collections import defaultdict


class PL94Strategy:
    """ Parent class to define the schema attribute(s). It is only needed for unit testing impact gaps"""
    schema = CC.SCHEMA_PL94

class USLevelStrategy:
    """
    Generic parent class for strategies that can be used with US-level runs. It has the levels attribute, but at this point
    it is only used by the unit tests. In actual DAS runs, levels are supplied from reading the config file
    """
    levels = CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE, CC.GEOLEVEL_US

class StateLevelStrategy:
    """
    Parent class for strategies that are only meant to be used with state-sized runs (mostly, Puerto Rico strategies)
    It has the levels attribute, but at this point it is only used by the unit tests. In actual DAS runs, levels are supplied
    from reading the config file
    """
    levels = CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE

class TestStrategy(PL94Strategy, USLevelStrategy):

    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))  # So as to avoid having to specify empty dicts and defaultdicts
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                "total",
                "numraces",
                "numraces * votingage",
                "numraces * votingage * hispanic",
                "numraces * hhgq * hispanic * votingage",
                "detailed"),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 1024) for num in (204, 204, 204, 154, 154, 104))),
            # CC.DPQUERIES: {},
            # CC.QUERIESPROP: {},
            # CC.UNITDPQUERIES: {},
            # CC.UNITQUERIESPROP: {},
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
        test_strategy[CC.QUERIESPROP][CC.GEOLEVEL_COUNTY]      = tuple(Fr(num, 1024) for num in (104, 304, 204, 154, 154, 104))
        test_strategy[CC.QUERIESPROP][CC.GEOLEVEL_TRACT]       = tuple(Fr(num, 1024) for num in ( 54, 354, 204, 154, 154, 104))
        test_strategy[CC.QUERIESPROP][CC.GEOLEVEL_BLOCK_GROUP] = tuple(Fr(num, 1024) for num in ( 14, 394, 204, 154, 154, 104))
        test_strategy[CC.QUERIESPROP][CC.GEOLEVEL_BLOCK]       = tuple(Fr(num, 1024) for num in (394,  14, 204, 154, 154, 104))
        return test_strategy


class SingleStateTestStrategyRegularOrdering:
    @staticmethod
    def make(levels):
        # levels = (CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE)
        ordering = {

            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ("total", "numraces",),
                    1: ("numraces",),
                    2: ("numraces * votingage",),
                    3: ("numraces * votingage * hispanic",),
                },
                1: {
                    0: ("numraces * hhgq * hispanic * votingage",),
                    1: ("detailed",)
                }
            },

            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ("total",),
                    1: ("numraces",),
                    2: ("numraces * votingage",),
                    3: ("numraces * votingage * hispanic",),
                },
                1: {
                    0: ("numraces * hhgq * hispanic * votingage",),
                    1: ("detailed",)
                }

            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ("total", "numraces", "numraces * votingage", "numraces * votingage * hispanic",)
                },
                1: {
                    0: ("numraces * hhgq * hispanic * votingage", "detailed",)
                }
            }

        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }
        query_ordering[CC.GEOLEVEL_COUNTY][CC.L2_QUERY_ORDERING] = {
            0: {
                0: ("total",),
                1: ("numraces",),
                2: ("numraces * votingage",),
                3: ("numraces * votingage * hispanic",),
            },
            1: {
                0: ("numraces * hhgq * hispanic * votingage",),
                1: ("detailed",)
            }
        }
        return query_ordering


class Strategies1:
    @staticmethod
    def getDPQNames():
        return ("total",
                "cenrace",
                "hispanic",
                "votingage",
                "hhinstlevels",
                "hhgq",
                "hispanic * cenrace",
                "votingage * cenrace",
                "votingage * hispanic",
                "votingage * hispanic * cenrace",
                "detailed")


class Strategies2:
    @staticmethod
    def getDPQNames():
        return ("total",
                "hispanic * cenrace11cats",
                "votingage",
                "hhinstlevels",
                "hhgq",
                "hispanic * cenrace11cats * votingage",
                "detailed")


class StandardRedistrictingRounderOrdering:
    @staticmethod
    def get():
        return {
                    0: {
                        0: ('total',
                            'hhgq',
                            'hhgq * hispanic',
                            'hhgq * hispanic * cenrace',
                            'hhgq * votingage * hispanic * cenrace',
                            'detailed')
                    },
                }


class Strategy1a(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        strategy1a = defaultdict(lambda: defaultdict(dict))
        strategy1a.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": Strategies1.getDPQNames(),

            CC.QUERIESPROP + "default": tuple([Fr(1, 11)] * 11),
        })
        for level in strategy1a[CC.GEODICT_GEOLEVELS]:
            strategy1a[CC.DPQUERIES][level] = strategy1a[CC.DPQUERIES + "default"]
            strategy1a[CC.QUERIESPROP][level] = strategy1a[CC.QUERIESPROP + "default"]
        return strategy1a


class Strategy1b(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        strategy1b = defaultdict(lambda: defaultdict(dict))
        strategy1b.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": Strategies1.getDPQNames(),

            CC.QUERIESPROP + "default": tuple(Fr(num, 1024) for num in (1, 2, 1, 1, 1, 1, 5, 5, 1, 17, 989)),
        })
        for level in strategy1b[CC.GEODICT_GEOLEVELS]:
            strategy1b[CC.DPQUERIES][level] = strategy1b[CC.DPQUERIES + "default"]
            strategy1b[CC.QUERIESPROP][level] = strategy1b[CC.QUERIESPROP + "default"]
        return strategy1b


class Strategy2a(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        strategy2a = defaultdict(lambda: defaultdict(dict))
        strategy2a.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": Strategies2.getDPQNames(),

            CC.QUERIESPROP + "default": tuple([Fr(1, 7)] * 7),
        })
        for level in strategy2a[CC.GEODICT_GEOLEVELS]:
            strategy2a[CC.DPQUERIES][level] = strategy2a[CC.DPQUERIES + "default"]
            strategy2a[CC.QUERIESPROP][level] = strategy2a[CC.QUERIESPROP + "default"]
        return strategy2a


class Strategy1a_St_Cty_isoTot(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = tuple([Fr(1, 10)] * 10)
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(507,512),Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                            Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,11),Fr(1,11),Fr(1,11),Fr(1,11),Fr(1,11),
                                                            Fr(1,11),Fr(1,11),Fr(1,11),Fr(1,11),Fr(1,11),Fr(1,11))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = tuple([Fr(1, 11)] * 11)
        return geos_qs_props_dict


class Strategy1a_St_Cty_isoTot_Ordering(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic'),
                    1: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    2: ('votingage * hispanic * cenrace',),
                    3: ('detailed',),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels'),
                    1: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    2: ('votingage * hispanic * cenrace',),
                    3: ('detailed',),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic'),
                    2: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    3: ('votingage * hispanic * cenrace',),
                    4: ('detailed',),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels'),
                    2: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    3: ('votingage * hispanic * cenrace',),
                    4: ('detailed',),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        subCounty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic'),
                    1: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    2: ('votingage * hispanic * cenrace',),
                    3: ('detailed',),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels'),
                    1: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    2: ('votingage * hispanic * cenrace',),
                    3: ('detailed',),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County"):
                query_ordering[geolevel] = st_cty_ordering
            else:
                query_ordering[geolevel] = subCounty_ordering
        return query_ordering


class Strategy1b_St_Cty_isoTot(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(990,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(339,512),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                            Fr(1,1024),Fr(1,1024),Fr(1,512),Fr(1,512),
                                                            Fr(1,1024),Fr(3,512),Fr(165,512))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024), Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(989,1024))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024), Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(989,1024))
        return geos_qs_props_dict


class Strategy1b_St_Cty_isoTot_Ordering:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },

            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        subCounty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County"):
                query_ordering[geolevel] = st_cty_ordering
            else:
                query_ordering[geolevel] = subCounty_ordering
        return query_ordering


class Strategy1b_St_Cty_BG_optSpine_ppmfCandidate(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(990,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(678,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                            Fr(1,1024),Fr(1,1024),Fr(2,1024),Fr(2,1024),
                                                            Fr(1,1024),Fr(6,1024),Fr(330,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(3,1024), Fr(3,1024), Fr(1,1024), Fr(11,1024), Fr(659,1024))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(572,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(3,1024), Fr(3,1024), Fr(1,1024), Fr(8,1024), Fr(432,1024))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024), Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(989,1024))
        return geos_qs_props_dict

class ProductionCandidate20210517US(PL94Strategy, USLevelStrategy):
    """To improve Tract total, votingage PPMF performance, shifted rho from Block Detail to Tract Total"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(990,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(678,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                            Fr(1,1024),Fr(1,1024),Fr(2,1024),Fr(2,1024),
                                                            Fr(1,1024),Fr(6,1024),Fr(330,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(3,1024), Fr(3,1024), Fr(1,1024), Fr(11,1024), Fr(659,1024))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(20531, 72704), Fr(51, 36352), Fr(51, 72704), Fr(51, 72704), Fr(51, 72704),
                                                             Fr(51, 72704), Fr(255, 72704), Fr(255, 72704), Fr(51, 72704), Fr(867, 72704), Fr(50439, 72704),)
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(572,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(3,1024), Fr(3,1024), Fr(1,1024), Fr(8,1024), Fr(432,1024))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(519, 510976),
                                                             Fr(519, 255488),
                                                             Fr(519, 510976),
                                                             Fr(519, 510976),
                                                             Fr(519, 510976),
                                                             Fr(519, 510976),
                                                             Fr(2595, 510976),
                                                             Fr(2595, 510976),
                                                             Fr(519, 510976),
                                                             Fr(8823, 510976),
                                                             Fr(492811, 510976),)
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024), Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(989,1024))
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult05(PL94Strategy, USLevelStrategy):
    """Before June 3 DSEP; to provide range of increase/decrease on non-detail queries at super-BG level"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4104), Fr(1, 1368), Fr(1, 1368), Fr(1, 1368), Fr(1, 1368), Fr(11, 4104), Fr(11, 4104), Fr(1, 1368), Fr(35, 4104), Fr(4027, 4104))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1026, 2053), Fr(2, 2053), Fr(2, 2053), Fr(2, 2053), Fr(2, 2053), Fr(2, 2053), Fr(7, 4106), Fr(7, 4106), Fr(2, 2053), Fr(19, 4106), Fr(1997, 4106))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(119, 586), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(4, 2051), Fr(4, 2051), Fr(3, 4102), Fr(27, 4102), Fr(1604, 2051))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(683, 4100), Fr(1, 1025), Fr(1, 2050), Fr(1, 2050), Fr(1, 2050), Fr(1, 2050), Fr(9, 4100), Fr(9, 4100), Fr(1, 2050), Fr(29, 4100), Fr(839, 1025))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult1(PL94Strategy, USLevelStrategy):
    """To improve Tract total, votingage PPMF performance, shifted rho from Block Detail to Tract Total"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1, 456), Fr(5, 4104), Fr(5, 4104), Fr(5, 4104), Fr(5, 4104), Fr(7, 1368), Fr(7, 1368), Fr(5, 4104), Fr(17, 1026), Fr(55, 57))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2713, 4107), Fr(5, 4107), Fr(5, 4107), Fr(5, 4107), Fr(5, 4107), Fr(5, 4107), Fr(3, 1369), Fr(3, 1369), Fr(5, 4107), Fr(25, 4107), Fr(1321, 4107))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(171, 512), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(11, 1024), Fr(659, 1024))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1157, 4099), Fr(6, 4099), Fr(3, 4099), Fr(3, 4099), Fr(3, 4099), Fr(3, 4099), Fr(15, 4099), Fr(15, 4099), Fr(3, 4099), Fr(49, 4099), Fr(2842, 4099))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"US geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult2(PL94Strategy, USLevelStrategy):
    """Before June 3 DSEP; to provide range of increase/decrease on non-detail queries at super-BG level"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(16, 4099), Fr(8, 4099), Fr(8, 4099), Fr(8, 4099), Fr(8, 4099), Fr(39, 4099), Fr(39, 4099), Fr(8, 4099), Fr(132, 4099), Fr(3833, 4099))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3233, 4099), Fr(5, 4099), Fr(5, 4099), Fr(5, 4099), Fr(5, 4099), Fr(5, 4099), Fr(10, 4099), Fr(10, 4099), Fr(5, 4099), Fr(29, 4099), Fr(787, 4099))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2018, 4099), Fr(6, 4099), Fr(6, 4099), Fr(6, 4099), Fr(6, 4099), Fr(6, 4099), Fr(18, 4099), Fr(18, 4099), Fr(6, 4099), Fr(65, 4099), Fr(1944, 4099))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1771, 4100), Fr(9, 4100), Fr(1, 820), Fr(1, 820), Fr(1, 820), Fr(1, 820), Fr(11, 2050), Fr(11, 2050), Fr(1, 820), Fr(3, 164), Fr(544, 1025))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult4(PL94Strategy, USLevelStrategy):
    """Before June 3 DSEP; to provide range of increase/decrease on non-detail queries at super-BG level"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(10, 1367), Fr(5, 1367), Fr(5, 1367), Fr(5, 1367), Fr(5, 1367), Fr(73, 4101), Fr(73, 4101), Fr(5, 1367), Fr(248, 4101), Fr(3602, 4101))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3577, 4103), Fr(6, 4103), Fr(6, 4103), Fr(6, 4103), Fr(6, 4103), Fr(6, 4103), Fr(1, 373), Fr(1, 373), Fr(6, 4103), Fr(32, 4103), Fr(436, 4103))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2645, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 1367), Fr(8, 1367), Fr(8, 4101), Fr(86, 4101), Fr(1274, 4101))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(402, 683), Fr(2, 683), Fr(1, 683), Fr(1, 683), Fr(1, 683), Fr(1, 683), Fr(5, 683), Fr(5, 683), Fr(1, 683), Fr(17, 683), Fr(247, 683))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult8(PL94Strategy, USLevelStrategy):
    """Before June 3 DSEP; to provide range of increase/decrease on non-detail queries at super-BG level"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(26, 2049), Fr(13, 2049), Fr(13, 2049), Fr(13, 2049), Fr(13, 2049), Fr(65, 2049), Fr(65, 2049), Fr(13, 2049), Fr(221, 2049), Fr(1607, 2049))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1889, 2051), Fr(3, 2051), Fr(3, 2051), Fr(3, 2051), Fr(3, 2051), Fr(3, 2051), Fr(6, 2051), Fr(6, 2051), Fr(3, 2051), Fr(17, 2051), Fr(115, 2051))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(29, 38), Fr(5, 2052), Fr(5, 2052), Fr(5, 2052), Fr(5, 2052), Fr(5, 2052), Fr(7, 1026), Fr(7, 1026), Fr(5, 2052), Fr(101, 4104), Fr(755, 4104))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1472, 2051), Fr(15, 4102), Fr(4, 2051), Fr(4, 2051), Fr(4, 2051), Fr(4, 2051), Fr(37, 4102), Fr(37, 4102), Fr(4, 2051), Fr(125, 4102), Fr(452, 2051))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult8_add005_dsepJune3(PL94Strategy, USLevelStrategy):
    """After June 3 DSEP; increase hisp*cenrace rho by 0.05 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(52, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(130, 4097), Fr(130, 4097), Fr(26, 4097), Fr(26, 241), Fr(189, 241))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3773, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(12, 4097), Fr(12, 4097), Fr(6, 4097), Fr(2, 241), Fr(230, 4097))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1206, 2051), Fr(13, 4102), Fr(1, 586), Fr(1, 586), Fr(1, 586), Fr(1, 586), Fr(767, 4102), Fr(31, 4102), Fr(1, 586), Fr(103, 4102), Fr(741, 4102))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2108, 4101), Fr(4, 4101), Fr(4, 4101), Fr(4, 4101), Fr(4, 4101), Fr(4, 4101), Fr(335, 4101), Fr(4, 1367), Fr(4, 4101), Fr(10, 1367), Fr(1592, 4101))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult8_add01_dsepJune3(PL94Strategy, USLevelStrategy):
    """After June 3 DSEP; increase hisp*cenrace rho by 0.1 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(52, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(130, 4097), Fr(130, 4097), Fr(26, 4097), Fr(26, 241), Fr(189, 241))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3773, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(12, 4097), Fr(12, 4097), Fr(6, 4097), Fr(2, 241), Fr(230, 4097))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2045, 4101), Fr(11, 4101), Fr(2, 1367), Fr(2, 1367), Fr(2, 1367), Fr(2, 1367), Fr(1274, 4101), Fr(26, 4101), Fr(2, 1367), Fr(29, 1367), Fr(628, 4101))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1954, 4103), Fr(4, 4103), Fr(4, 4103), Fr(4, 4103), Fr(4, 4103), Fr(4, 4103), Fr(610, 4103), Fr(1, 373), Fr(4, 4103), Fr(28, 4103), Fr(1476, 4103))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult8_add02_dsepJune3(PL94Strategy, USLevelStrategy):
    """After June 3 DSEP; increase hisp*cenrace rho by 0.2 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(52, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(130, 4097), Fr(130, 4097), Fr(26, 4097), Fr(26, 241), Fr(189, 241))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3773, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(12, 4097), Fr(12, 4097), Fr(6, 4097), Fr(2, 241), Fr(230, 4097))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1567, 4102), Fr(4, 2051), Fr(5, 4102), Fr(5, 4102), Fr(5, 4102), Fr(5, 4102), Fr(1933, 4102), Fr(10, 2051), Fr(5, 4102), Fr(67, 4102), Fr(241, 2051))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1705, 4099), Fr(3, 4099), Fr(3, 4099), Fr(3, 4099), Fr(3, 4099), Fr(3, 4099), Fr(1055, 4099), Fr(9, 4099), Fr(3, 4099), Fr(24, 4099), Fr(1288, 4099))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult8_add03_dsepJune3(PL94Strategy, USLevelStrategy):
    """After June 3 DSEP; increase hisp*cenrace rho by 0.3 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(52, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(130, 4097), Fr(130, 4097), Fr(26, 4097), Fr(26, 241), Fr(189, 241))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3773, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(12, 4097), Fr(12, 4097), Fr(6, 4097), Fr(2, 241), Fr(230, 4097))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1271, 4102), Fr(1, 586), Fr(2, 2051), Fr(2, 2051), Fr(2, 2051), Fr(2, 2051), Fr(2343, 4102), Fr(8, 2051), Fr(2, 2051), Fr(27, 2051), Fr(391, 4102))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(504, 1367), Fr(1, 1367), Fr(1, 1367), Fr(1, 1367), Fr(1, 1367), Fr(1, 1367), Fr(1399, 4101), Fr(8, 4101), Fr(1, 1367), Fr(22, 4101), Fr(1142, 4101))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict


class ProductionCandidate20210527US_mult8_add04_dsepJune3(PL94Strategy, USLevelStrategy):
    """After June 3 DSEP; increase hisp*cenrace rho by 0.4 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(52, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(26, 4097), Fr(130, 4097), Fr(130, 4097), Fr(26, 4097), Fr(26, 241), Fr(189, 241))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3773, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(6, 4097), Fr(12, 4097), Fr(12, 4097), Fr(6, 4097), Fr(2, 241), Fr(230, 4097))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1069, 4100), Fr(3, 2050), Fr(3, 4100), Fr(3, 4100), Fr(3, 4100), Fr(3, 4100), Fr(1311, 2050), Fr(7, 2050), Fr(3, 4100), Fr(23, 2050), Fr(2, 25))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(97, 293), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(239, 586), Fr(4, 2051), Fr(3, 4102), Fr(19, 4102), Fr(513, 2051))
            elif level == "Block": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"US geolevel {level} not recognized.")
        return geos_qs_props_dict




class ProductionCandidate20210517PR(PL94Strategy, StateLevelStrategy):
    """To improve Tract total, votingage PPMF performance, shifted rho from Block Detail to Tract Total"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(678, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024),
                                                             Fr(1, 1024), Fr(1, 1024), Fr(2, 1024), Fr(2, 1024),
                                                             Fr(1, 1024), Fr(6, 1024), Fr(330, 1024))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024),
                                                             Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(11, 1024), Fr(659, 1024))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(6161, 23552),
                                                             Fr(17, 11776),
                                                             Fr(17, 23552),
                                                             Fr(17, 23552),
                                                             Fr(17, 23552),
                                                             Fr(17, 23552),
                                                             Fr(85, 23552),
                                                             Fr(85, 23552),
                                                             Fr(17, 23552),
                                                             Fr(289, 23552),
                                                             Fr(731, 1024),)

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(572, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024),
                                                             Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(8, 1024), Fr(432, 1024))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(173, 171008),
                                                             Fr(173, 85504),
                                                             Fr(173, 171008),
                                                             Fr(173, 171008),
                                                             Fr(173, 171008),
                                                             Fr(173, 171008),
                                                             Fr(865, 171008),
                                                             Fr(865, 171008),
                                                             Fr(173, 171008),
                                                             Fr(2941, 171008),
                                                             Fr(164953, 171008),)
            else:  # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1, 1024), Fr(1, 512), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024),
                                                             Fr(1, 1024), Fr(5, 1024), Fr(5, 1024), Fr(1, 1024), Fr(17, 1024), Fr(989, 1024))
        return geos_qs_props_dict


class ProductionCandidate20210521PR(PL94Strategy, StateLevelStrategy):
    """PR has no AIAN areas; re-distributed its 'total' rho to other queries equally"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(69, 1024), Fr(69, 1024), Fr(69, 1024),
                                                             Fr(69, 1024), Fr(69, 1024), Fr(70, 1024), Fr(70, 1024),
                                                             Fr(69, 1024), Fr(74, 1024), Fr(396, 1024))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024),
                                                             Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(11, 1024), Fr(659, 1024))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (
                                                                Fr(9767, 49664),
                                                                Fr(39, 24832),
                                                                Fr(39, 49664),
                                                                Fr(39, 49664),
                                                                Fr(39, 49664),
                                                                Fr(39, 49664),
                                                                Fr(195, 49664),
                                                                Fr(195, 49664),
                                                                Fr(39, 49664),
                                                                Fr(663, 49664),
                                                                Fr(38571, 49664),
                )

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(572, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024),
                                                             Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(8, 1024), Fr(432, 1024))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(519, 512000),
                                                             Fr(519, 256000),
                                                             Fr(519, 512000),
                                                             Fr(519, 512000),
                                                             Fr(519, 512000),
                                                             Fr(519, 512000),
                                                             Fr(519, 102400),
                                                             Fr(519, 102400),
                                                             Fr(519, 512000),
                                                             Fr(8823, 512000),
                                                             Fr(98767, 102400),)
            else:  # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1, 1024), Fr(1, 512), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024),
                                                             Fr(1, 1024), Fr(5, 1024), Fr(5, 1024), Fr(1, 1024), Fr(17, 1024), Fr(989, 1024))
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult05(PL94Strategy, StateLevelStrategy):
    """"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(200, 4103), Fr(200, 4103), Fr(200, 4103), Fr(200, 4103), Fr(200, 4103), Fr(202, 4103), Fr(202, 4103), Fr(200, 4103), Fr(214, 4103), Fr(2285, 4103))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(119, 586), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(3, 4102), Fr(4, 2051), Fr(4, 2051), Fr(3, 4102), Fr(27, 4102), Fr(1604, 2051))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(454, 4101), Fr(4, 4101), Fr(2, 4101), Fr(2, 4101), Fr(2, 4101), Fr(2, 4101), Fr(10, 4101), Fr(10, 4101), Fr(2, 4101), Fr(31, 4101), Fr(1194, 1367))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult1(PL94Strategy, StateLevelStrategy):
    """PR has no AIAN areas; re-distributed its 'total' rho to other queries equally"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(69, 1024), Fr(69, 1024), Fr(69, 1024), Fr(69, 1024), Fr(69, 1024), Fr(35, 512), Fr(35, 512), Fr(69, 1024), Fr(37, 512), Fr(99, 256))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(171, 512), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(11, 1024), Fr(659, 1024))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(403, 2052), Fr(7, 4104), Fr(1, 1026), Fr(1, 1026), Fr(1, 1026), Fr(1, 1026), Fr(17, 4104), Fr(17, 4104), Fr(1, 1026), Fr(55, 4104), Fr(1591, 2052))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult2(PL94Strategy, StateLevelStrategy):
    """"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(343, 4103), Fr(343, 4103), Fr(343, 4103), Fr(343, 4103), Fr(343, 4103), Fr(348, 4103), Fr(348, 4103), Fr(343, 4103), Fr(367, 4103), Fr(982, 4103))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2018, 4099), Fr(6, 4099), Fr(6, 4099), Fr(6, 4099), Fr(6, 4099), Fr(6, 4099), Fr(18, 4099), Fr(18, 4099), Fr(6, 4099), Fr(65, 4099), Fr(1944, 4099))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1317, 4103), Fr(1, 373), Fr(6, 4103), Fr(6, 4103), Fr(6, 4103), Fr(6, 4103), Fr(27, 4103), Fr(27, 4103), Fr(6, 4103), Fr(90, 4103), Fr(2601, 4103))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult4(PL94Strategy, StateLevelStrategy):
    """"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(389, 4099), Fr(389, 4099), Fr(389, 4099), Fr(389, 4099), Fr(389, 4099), Fr(395, 4099), Fr(395, 4099), Fr(389, 4099), Fr(417, 4099), Fr(558, 4099))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2645, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 4101), Fr(8, 1367), Fr(8, 1367), Fr(8, 4101), Fr(86, 4101), Fr(1274, 4101))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(193, 410), Fr(4, 1025), Fr(2, 1025), Fr(2, 1025), Fr(2, 1025), Fr(2, 1025), Fr(39, 4100), Fr(39, 4100), Fr(2, 1025), Fr(131, 4100), Fr(381, 820))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict



class ProductionCandidate20210527PR_mult8(PL94Strategy, StateLevelStrategy):
    """"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(53, 513), Fr(53, 513), Fr(11, 108), Fr(56, 513), Fr(25, 342))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(29, 38), Fr(5, 2052), Fr(5, 2052), Fr(5, 2052), Fr(5, 2052), Fr(5, 2052), Fr(7, 1026), Fr(7, 1026), Fr(5, 2052), Fr(101, 4104), Fr(755, 4104))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(419, 684), Fr(7, 1368), Fr(11, 4104), Fr(11, 4104), Fr(11, 4104), Fr(11, 4104), Fr(17, 1368), Fr(17, 1368), Fr(11, 4104), Fr(1, 24), Fr(1241, 4104))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] =  (Fr(143, 256), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(1, 1024), Fr(3, 1024), Fr(3, 1024), Fr(1, 1024), Fr(1, 128), Fr(27, 64))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4103), Fr(9, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(5, 4103), Fr(21, 4103), Fr(21, 4103), Fr(5, 4103), Fr(71, 4103), Fr(3951, 4103))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult8_add005_dsepJune3(PL94Strategy, StateLevelStrategy):
    """After June 3 DSEP; increment hispanic*cenrace rho by 0.05 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(53, 513), Fr(53, 513), Fr(11, 108), Fr(56, 513), Fr(25, 342))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2131, 4102), Fr(9, 2051), Fr(5, 2051), Fr(5, 2051), Fr(5, 2051), Fr(5, 2051), Fr(331, 2051), Fr(22, 2051), Fr(5, 2051), Fr(145, 4102), Fr(526, 2051))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(2169, 4099), Fr(4, 4099), Fr(4, 4099), Fr(4, 4099), Fr(4, 4099), Fr(4, 4099), Fr(225, 4099), Fr(12, 4099), Fr(4, 4099), Fr(31, 4099), Fr(1638, 4099))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult8_add01_dsepJune3(PL94Strategy, StateLevelStrategy):
    """After June 3 DSEP; increment hispanic*cenrace rho by 0.1 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(53, 513), Fr(53, 513), Fr(11, 108), Fr(56, 513), Fr(25, 342))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1852, 4103), Fr(16, 4103), Fr(9, 4103), Fr(9, 4103), Fr(9, 4103), Fr(9, 4103), Fr(1112, 4103), Fr(38, 4103), Fr(9, 4103), Fr(126, 4103), Fr(914, 4103))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1031, 2050), Fr(1, 1025), Fr(1, 1025), Fr(1, 1025), Fr(1, 1025), Fr(1, 1025), Fr(417, 4100), Fr(11, 4100), Fr(1, 1025), Fr(29, 4100), Fr(1557, 4100))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult8_add02_dsepJune3(PL94Strategy, StateLevelStrategy):
    """After June 3 DSEP; increment hispanic*cenrace rho by 0.2 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(53, 513), Fr(53, 513), Fr(11, 108), Fr(56, 513), Fr(25, 342))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1467, 4102), Fr(13, 4102), Fr(1, 586), Fr(1, 586), Fr(1, 586), Fr(1, 586), Fr(866, 2051), Fr(15, 2051), Fr(1, 586), Fr(50, 2051), Fr(725, 4102))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1876, 4103), Fr(4, 4103), Fr(4, 4103), Fr(4, 4103), Fr(4, 4103), Fr(4, 4103), Fr(749, 4103), Fr(10, 4103), Fr(4, 4103), Fr(27, 4103), Fr(1417, 4103))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult8_add03_dsepJune3(PL94Strategy, StateLevelStrategy):
    """After June 3 DSEP; increment hispanic*cenrace rho by 0.3 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(53, 513), Fr(53, 513), Fr(11, 108), Fr(56, 513), Fr(25, 342))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1215, 4102), Fr(11, 4102), Fr(3, 2051), Fr(3, 2051), Fr(3, 2051), Fr(3, 2051), Fr(1069, 2051), Fr(25, 4102), Fr(3, 2051), Fr(83, 4102), Fr(300, 2051))

            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1721, 4106), Fr(2, 2053), Fr(2, 2053), Fr(2, 2053), Fr(2, 2053), Fr(2, 2053), Fr(513, 2053), Fr(5, 2053), Fr(2, 2053), Fr(25, 4106), Fr(650, 2053))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict


class ProductionCandidate20210527PR_mult8_add04_dsepJune3(PL94Strategy, StateLevelStrategy):
    """After June 3 DSEP; increment hispanic*cenrace rho by 0.4 in Tract, BG"""
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(11, 108), Fr(53, 513), Fr(53, 513), Fr(11, 108), Fr(56, 513), Fr(25, 342))
            elif level == "County":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(3126, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(10, 4097), Fr(28, 4097), Fr(28, 4097), Fr(10, 4097), Fr(101, 4097), Fr(754, 4097))
            elif level == "Tract":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1037, 4102), Fr(9, 4102), Fr(5, 4102), Fr(5, 4102), Fr(5, 4102), Fr(5, 4102), Fr(1213, 2051), Fr(11, 2051), Fr(5, 4102), Fr(71, 4102), Fr(256, 2051))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(530, 1367), Fr(1, 1367), Fr(1, 1367), Fr(1, 1367), Fr(1, 1367), Fr(1, 1367), Fr(420, 1367), Fr(3, 1367), Fr(1, 1367), Fr(23, 4101), Fr(1201, 4101))
            elif level == "Block":  # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                           "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                           "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(5, 4097), Fr(9, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(5, 4097), Fr(21, 4097), Fr(21, 4097), Fr(5, 4097), Fr(71, 4097), Fr(3945, 4097))
            else:
                raise ValueError(f"PR geolevel {level} not recognized")
        return geos_qs_props_dict



class Strategy1b_St_Cty_B_optSpine_ppmfCandidate(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(990,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(678,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                            Fr(1,1024),Fr(1,1024),Fr(2,1024),Fr(2,1024),
                                                            Fr(1,1024),Fr(6,1024),Fr(330,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(3,1024), Fr(3,1024), Fr(1,1024), Fr(11,1024), Fr(659,1024))
            elif level == "Block":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(160,1024), Fr(2,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(15,1024), Fr(832,1024))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "cenrace", "hispanic", "votingage", "hhinstlevels",
                                                            "hhgq", "hispanic * cenrace", "votingage * cenrace",
                                                            "votingage * hispanic", "votingage * hispanic * cenrace", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024), Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024),
                                                        Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(989,1024))
        return geos_qs_props_dict


class Strategy1b_ST_CTY_B_isoTot_Ordering:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_b_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },

            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        default_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County", "Block"):
                query_ordering[geolevel] = st_cty_b_ordering
            else:
                query_ordering[geolevel] = default_ordering
        return query_ordering


class Strategy1b_ST_CTY_BG_isoTot_Ordering:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_bg_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },

            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        default_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County", "Block_Group"):
                query_ordering[geolevel] = st_cty_bg_ordering
            else:
                query_ordering[geolevel] = default_ordering
        return query_ordering


class Strategy1b_ST_CTY_TR_BG_isoTot_Ordering_dsepJune3:
    """Addresses additional DSEP June 3 POP/DEMO ask: isolate total query in TR to control diversity-totPop error correlation"""
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_tr_bg_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },

            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        default_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County", "Tract", "Block_Group"):
                query_ordering[geolevel] = st_cty_tr_bg_ordering
            else:
                query_ordering[geolevel] = default_ordering
        return query_ordering


class Strategy1b_CTY_BG_isoTot_Ordering_PR:
    """PR has no AIAN areas; re-distributed its 'total' rho to other queries equally, hence State now has no 'total' query"""
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_st_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        cty_bg_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },

            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        default_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State"):
                query_ordering[geolevel] = us_st_ordering
            elif geolevel in ("County", "Block_Group"):
                query_ordering[geolevel] = cty_bg_ordering
            else:
                query_ordering[geolevel] = default_ordering
        return query_ordering


class Strategy1b_CTY_TR_BG_isoTot_Ordering_PR_dsepJune3:
    """PR has no AIAN areas; re-distributed its 'total' rho to other queries equally, hence State now has no 'total' query
        Addresses additional DSEP June 3 POP/DEMO ask: isolate total query in TR to control diversity-totPop error correlation"""
    @staticmethod
    def make(levels):
        st_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hispanic * cenrace', 'votingage * cenrace',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        cty_tr_bg_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace',
                        'detailed'),
                },
            },

            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        default_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'votingage * hispanic',
                        'hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic',
                        'votingage * hispanic * cenrace', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("State"):
                query_ordering[geolevel] = st_ordering
            elif geolevel in ("County", "Tract", "Block_Group"):
                query_ordering[geolevel] = cty_tr_bg_ordering
            else:
                query_ordering[geolevel] = default_ordering
        return query_ordering



class Strategy2a_St_Cty_isoTot(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("hispanic * cenrace11cats", "votingage", "hhinstlevels", "hhgq",
                                                        "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = tuple([Fr(1, 6)] * 6)
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(509,512),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,7),Fr(1,7),Fr(1,7),Fr(1,7),
                                                        Fr(1,7),Fr(1,7),Fr(1,7))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,7),Fr(1,7),Fr(1,7),Fr(1,7),
                                                        Fr(1,7),Fr(1,7),Fr(1,7))
        return geos_qs_props_dict


class Strategy2a_St_Cty_isoTot_Ordering:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq'),
                    1: ('hhgq',),
                    2: ('hispanic * cenrace11cats * votingage',),
                    3: ('detailed',),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels'),
                    1: ('hhgq',),
                    2: ('hispanic * cenrace11cats * votingage',),
                    3: ('detailed',),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq'),
                    2: ('hhgq',),
                    3: ('hispanic * cenrace11cats * votingage',),
                    4: ('detailed',),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels'),
                    2: ('hhgq',),
                    3: ('hispanic * cenrace11cats * votingage',),
                    4: ('detailed',),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        subCounty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq'),
                    1: ('hhgq',),
                    2: ('hispanic * cenrace11cats * votingage',),
                    3: ('detailed',),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels'),
                    1: ('hhgq',),
                    2: ('hispanic * cenrace11cats * votingage',),
                    3: ('detailed',),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County"):
                query_ordering[geolevel] = st_cty_ordering
            else:
                query_ordering[geolevel] = subCounty_ordering
        return query_ordering


class Strategy2b_St_Cty_isoTot(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("hispanic * cenrace11cats", "votingage", "hhinstlevels", "hhgq",
                                                    "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1019,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(905,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(57,512))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1018,1024))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1018,1024))
        return geos_qs_props_dict

class Strategy2b_St_Cty_B_optSpine_ppmfCandidate(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("hispanic * cenrace11cats", "votingage", "hhinstlevels", "hhgq",
                                                    "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1019,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(815,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(204,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(677,1024))
            elif level == "Block":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(190,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(829,1024))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1018,1024))
        return geos_qs_props_dict


class Strategy2b_St_Cty_B_aianSpine_ppmfCandidate(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("hispanic * cenrace11cats", "votingage", "hhinstlevels", "hhgq",
                                                    "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1019,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(815,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(204,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(677,1024))
            elif level == "Block":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(208,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(811,1024))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1018,1024))
        return geos_qs_props_dict


class Strategy2b_St_Cty_BG_optSpine_ppmfCandidate(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        geos_qs_props_dict = defaultdict(lambda: defaultdict(dict))
        geos_qs_props_dict.update({
            CC.GEODICT_GEOLEVELS: levels,
        })
        for level in geos_qs_props_dict[CC.GEODICT_GEOLEVELS]:
            if level == "US": # No 'total' query
                geos_qs_props_dict[CC.DPQUERIES][level] = ("hispanic * cenrace11cats", "votingage", "hhinstlevels", "hhgq",
                                                    "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1019,1024))
            elif level == "State": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(815,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(204,1024))
            elif level == "County": # In Redistricting, separately tuned rho on 'total'
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(342,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(677,1024))
            elif level == "Block_Group":
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(530,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(489,1024))
            else: # Only per-geolevel rho actively tuned in other geolevels; query allocations left at default
                geos_qs_props_dict[CC.DPQUERIES][level] = ("total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                                        "hhgq", "hispanic * cenrace11cats * votingage", "detailed")
                geos_qs_props_dict[CC.QUERIESPROP][level] = (Fr(1,1024),Fr(1,1024),Fr(1,1024),Fr(1,1024),
                                                        Fr(1,1024),Fr(1,1024),Fr(1018,1024))
        return geos_qs_props_dict



class Strategy2b_ST_CTY_B_isoTot_Ordering:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                            'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING:{
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                            'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_b_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        default_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County", "Block"):
                query_ordering[geolevel] = st_cty_b_ordering
            else:
                query_ordering[geolevel] = default_ordering
        return query_ordering


class Strategy2b_ST_CTY_BG_isoTot_Ordering:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                            'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING:{
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                            'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_bg_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        default_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County", "Block_Group"):
                query_ordering[geolevel] = st_cty_bg_ordering
            else:
                query_ordering[geolevel] = default_ordering
        return query_ordering


class Strategy2b_St_Cty_isoTot_Ordering:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        us_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                            'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING:{
                0: {
                    0: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                            'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        st_cty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total',),
                    1: ('hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }
        subCounty_ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq',
                        'hispanic * cenrace11cats * votingage', 'detailed'),
                },
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hhgq', 'hhgq * hispanic', 'hhgq * hispanic * cenrace', 'hhgq * votingage * hispanic * cenrace',
                        'detailed'),
                },
            },
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel == "US":
                query_ordering[geolevel] = us_ordering
            elif geolevel in ("State", "County"):
                query_ordering[geolevel] = st_cty_ordering
            else:
                query_ordering[geolevel] = subCounty_ordering
        return query_ordering


class Strategy2b(PL94Strategy, USLevelStrategy):
    @staticmethod
    def make(levels):
        strategy2b = defaultdict(lambda: defaultdict(dict))
        strategy2b.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": Strategies2.getDPQNames(),

            CC.QUERIESPROP + "default": tuple(Fr(num, 1024) for num in (1,) * 6 + (1018,)),
        })

        for level in strategy2b[CC.GEODICT_GEOLEVELS]:
            strategy2b[CC.DPQUERIES][level] = strategy2b[CC.DPQUERIES + "default"]
            strategy2b[CC.QUERIESPROP][level] = strategy2b[CC.QUERIESPROP + "default"]

        return strategy2b


class RedistrictingStrategiesRegularOrdering1a:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total',
                         'cenrace',
                         'hispanic',
                         'votingage',
                         'hhinstlevels',
                         'hhgq',
                         'votingage * hispanic'),
                    1: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    2: ("votingage * hispanic * cenrace",),
                    3: ("detailed",),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels'),
                    1: ('hhgq', 'hispanic * cenrace', 'votingage * cenrace', 'votingage * hispanic'),
                    2: ("votingage * hispanic * cenrace",),
                    3: ("detailed",),
                },

            },
            CC.ROUNDER_QUERY_ORDERING: StandardRedistrictingRounderOrdering.get()
        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }
        return query_ordering


class RedistrictingStrategiesRegularOrdering1b:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: Strategies1.getDPQNames()
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: None,
            CC.ROUNDER_QUERY_ORDERING: StandardRedistrictingRounderOrdering.get()
        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }
        return query_ordering


class RedistrictingStrategiesRegularOrdering2a:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: Strategies2.getDPQNames()
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: None,
            CC.ROUNDER_QUERY_ORDERING: StandardRedistrictingRounderOrdering.get()
        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }
        return query_ordering


class RedistrictingStrategiesRegularOrdering2b:
    @staticmethod
    def make(levels):
        # levels = USGeolevelsNoTractGroup.getLevels()
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels', 'hhgq'),
                    1: ('hhgq', ),
                    2: ("hispanic * cenrace11cats * votingage",),
                    3: ("detailed",),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ('total', 'hispanic * cenrace11cats', 'votingage', 'hhinstlevels'),
                    1: ('hhgq',),
                    2: ("hispanic * cenrace11cats * votingage",),
                    3: ("detailed",),
                },

            },
            CC.ROUNDER_QUERY_ORDERING: StandardRedistrictingRounderOrdering.get()
        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }
        return query_ordering


class DetailedOnly(USLevelStrategy):

    schema = CC.SCHEMA_PL94  # Can be used with any schema. This attribute is only for unit tested impact gaps. Detailed query doesn't have impact gaps in any schema.

    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))  # So as to avoid having to specify empty dicts and defaultdicts
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                "detailed",),

            CC.QUERIESPROP + "default": (1,),
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]

        return test_strategy


class DetailedOnlyQueryOrderingOuterPass:
    @staticmethod
    def make(levels):
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ("detailed",),
                },
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ("detailed",),
                },
            },
            CC.ROUNDER_QUERY_ORDERING:  {
                0: {
                    0: ("detailed",),
                },
            }
        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }

        return query_ordering


class DetailedOnlyQueryOrdering:
    @staticmethod
    def make(levels):
        ordering = {
            CC.L2_QUERY_ORDERING: {0: ("detailed",)},

            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: None,
            CC.ROUNDER_QUERY_ORDERING:  {0: ("detailed",)},
        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }

        return query_ordering

class StrategySelector:
    strategies = {
        'strategy1a': Strategy1a,
        'strategy1b': Strategy1b,
        'strategy2a': Strategy2a,
        'strategy2b': Strategy2b,
        'test_strategy': TestStrategy,
        'DetailedOnly': DetailedOnly,
        'Strategy1a_St_Cty_isoTot'                              : Strategy1a_St_Cty_isoTot,
        'Strategy1b_St_Cty_isoTot'                              : Strategy1b_St_Cty_isoTot,
        'Strategy2a_St_Cty_isoTot'                              : Strategy2a_St_Cty_isoTot,
        'Strategy2b_St_Cty_isoTot'                              : Strategy2b_St_Cty_isoTot,
        'Strategy1b_St_Cty_BG_optSpine_ppmfCandidate'           : Strategy1b_St_Cty_BG_optSpine_ppmfCandidate,
        'ProductionCandidate20210517US'                         : ProductionCandidate20210517US,
        'ProductionCandidate20210527US_mult05'                  : ProductionCandidate20210527US_mult05,
        'ProductionCandidate20210527US_mult1'                   : ProductionCandidate20210527US_mult1,
        'ProductionCandidate20210527US_mult2'                   : ProductionCandidate20210527US_mult2,
        'ProductionCandidate20210527US_mult4'                   : ProductionCandidate20210527US_mult4,
        'ProductionCandidate20210527US_mult8'                   : ProductionCandidate20210527US_mult8,
        'ProductionCandidate20210527US_mult8_add005_dsepJune3'  : ProductionCandidate20210527US_mult8_add005_dsepJune3,
        'ProductionCandidate20210527US_mult8_add01_dsepJune3'   : ProductionCandidate20210527US_mult8_add01_dsepJune3,
        'ProductionCandidate20210527US_mult8_add02_dsepJune3'   : ProductionCandidate20210527US_mult8_add02_dsepJune3,
        'ProductionCandidate20210527US_mult8_add03_dsepJune3'   : ProductionCandidate20210527US_mult8_add03_dsepJune3,
        'ProductionCandidate20210527US_mult8_add04_dsepJune3'   : ProductionCandidate20210527US_mult8_add04_dsepJune3,
        'ProductionCandidate20210517PR'                         : ProductionCandidate20210517PR,
        'ProductionCandidate20210521PR'                         : ProductionCandidate20210521PR,
        'ProductionCandidate20210527PR_mult05'                  : ProductionCandidate20210527PR_mult05,
        'ProductionCandidate20210527PR_mult1'                   : ProductionCandidate20210527PR_mult1,
        'ProductionCandidate20210527PR_mult2'                   : ProductionCandidate20210527PR_mult2,
        'ProductionCandidate20210527PR_mult4'                   : ProductionCandidate20210527PR_mult4,
        'ProductionCandidate20210527PR_mult8'                   : ProductionCandidate20210527PR_mult8,
        'ProductionCandidate20210527PR_mult8_add005_dsepJune3'  : ProductionCandidate20210527PR_mult8_add005_dsepJune3,
        'ProductionCandidate20210527PR_mult8_add01_dsepJune3'   : ProductionCandidate20210527PR_mult8_add01_dsepJune3,
        'ProductionCandidate20210527PR_mult8_add02_dsepJune3'   : ProductionCandidate20210527PR_mult8_add02_dsepJune3,
        'ProductionCandidate20210527PR_mult8_add03_dsepJune3'   : ProductionCandidate20210527PR_mult8_add03_dsepJune3,
        'ProductionCandidate20210527PR_mult8_add04_dsepJune3'   : ProductionCandidate20210527PR_mult8_add04_dsepJune3,
        'Strategy1b_St_Cty_B_optSpine_ppmfCandidate'            : Strategy1b_St_Cty_B_optSpine_ppmfCandidate,
        'Strategy2b_St_Cty_B_optSpine_ppmfCandidate'            : Strategy2b_St_Cty_B_optSpine_ppmfCandidate,
        'Strategy2b_St_Cty_B_aianSpine_ppmfCandidate'           : Strategy2b_St_Cty_B_aianSpine_ppmfCandidate,
        'Strategy2b_St_Cty_BG_optSpine_ppmfCandidate'           : Strategy2b_St_Cty_BG_optSpine_ppmfCandidate,
    }

class QueryOrderingSelector:
    query_orderings = {
        'test_strategy_regular_ordering'                    : SingleStateTestStrategyRegularOrdering,
        'DetailedOnly_Ordering'                             : DetailedOnlyQueryOrdering,
        'DetailedOnly_OrderingOuterPass'                    : DetailedOnlyQueryOrderingOuterPass,
        'redistricting_regular_ordering_1a'                 : RedistrictingStrategiesRegularOrdering1a,
        'redistricting_regular_ordering_1b'                 : RedistrictingStrategiesRegularOrdering1b,
        'redistricting_regular_ordering_2a'                 : RedistrictingStrategiesRegularOrdering2a,
        'redistricting_regular_ordering_2b'                 : RedistrictingStrategiesRegularOrdering1b,
        'Strategy1a_St_Cty_isoTot_Ordering'                 : Strategy1a_St_Cty_isoTot_Ordering,
        'Strategy1b_St_Cty_isoTot_Ordering'                 : Strategy1b_St_Cty_isoTot_Ordering,
        'Strategy2a_St_Cty_isoTot_Ordering'                 : Strategy2a_St_Cty_isoTot_Ordering,
        'Strategy2b_St_Cty_isoTot_Ordering'                 : Strategy2b_St_Cty_isoTot_Ordering,
        'Strategy1b_ST_CTY_B_isoTot_Ordering'               : Strategy1b_ST_CTY_B_isoTot_Ordering,
        'Strategy1b_ST_CTY_BG_isoTot_Ordering'              : Strategy1b_ST_CTY_BG_isoTot_Ordering,
        'Strategy1b_CTY_BG_isoTot_Ordering_PR'              : Strategy1b_CTY_BG_isoTot_Ordering_PR,
        'Strategy2b_ST_CTY_B_isoTot_Ordering'               : Strategy2b_ST_CTY_B_isoTot_Ordering,
        'Strategy2b_ST_CTY_BG_isoTot_Ordering'              : Strategy2b_ST_CTY_BG_isoTot_Ordering,
        'Strategy1b_ST_CTY_TR_BG_isoTot_Ordering_dsepJune3' : Strategy1b_ST_CTY_TR_BG_isoTot_Ordering_dsepJune3,
        'Strategy1b_CTY_TR_BG_isoTot_Ordering_PR_dsepJune3' : Strategy1b_CTY_TR_BG_isoTot_Ordering_PR_dsepJune3,
    }
