import numpy as np
from copy import deepcopy
import constants as CC
from collections import OrderedDict
from typing import Tuple
from fractions import *


def recursion_through_tracts_entity_k(adjacency_dict: dict, levels_dict: dict, entity_k: list) -> Tuple[int, tuple]:
    """
    Performs the first two iterations of the algorithm proposed in the Alternative Geographic Spine document to find
    the off-spine entity distance (OSED), which is the number of geounits that must be added or subtracted from one
    another in order to derive the offspine entity k.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param entity_k: block-level (string) geoids that are in entity k

    :return bound_ck: dictionary providing the OSED of entity_k for each tract.
    :return cks: dictionaries providing the OSED of entity_k and the OSED of the complement of entity_k for each tract.
    """
    c_k_bg = dict()
    c_notk_bg = dict()
    # Initialize recursive formula described in the Alternative Geographic Spine Document at the block-group geolevel.
    for bg in levels_dict[CC.BLOCK_GROUP]:
        total_in_k = sum([block in entity_k for block in adjacency_dict[bg]])
        total_not_in_k = len(adjacency_dict[bg]) - total_in_k
        c_k_bg[bg] = min(total_in_k, total_not_in_k + 1)
        c_notk_bg[bg] = min(total_in_k + 1, total_not_in_k)

    # Perform one more iteration of the recursion to define c_k and c_notk at the tract geolevel.
    c_k = dict()
    c_notk = dict()

    bound_on_tg_c_k = 0
    bound_on_tg_c_notk = 0
    for unit in levels_dict[CC.TRACT]:
        total_in_k = sum([c_k_bg[child] for child in adjacency_dict[unit]])
        total_not_in_k = sum([c_notk_bg[child] for child in adjacency_dict[unit]])
        c_k[unit] = min(total_in_k, total_not_in_k + 1)
        c_notk[unit] = min(total_in_k + 1, total_not_in_k)
        bound_on_tg_c_k += c_k[unit]
        bound_on_tg_c_notk += c_notk[unit]

    # Compute final bound on c_k_county. This is given by OSED value for the case in which the tract-group geolevel
    # is removed entirely, and the parent geounit for all tracts is simply the county:
    bound_county_c_k = min(bound_on_tg_c_k, bound_on_tg_c_notk + 1)

    return bound_county_c_k, (c_k, c_notk)


def dist_of_entity_k(adjacency_dict: dict, levels_dict: dict, c_k: dict, c_notk: dict) -> int:
    """
    Performs the final iterations of the algorithm proposed in the Alternative Geographic Spine document to find
    the "off spine entity distance" (OSED), which is the number of geounits that must be added or subtracted from one
    another in order to derive the offspine entity k.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param c_k: dictionary providing the OSED of entity_k for each tract
    :param c_notk: dictionary providing the OSED of the complement of entity_k for each tract

    :return OSED: the OSED at the county geolevel
    """

    # Perform recursion through county geolevel (see Alternative Geographic Spine document):
    for level in [CC.TRACT_GROUP, CC.COUNTY]:
        for unit in levels_dict[level]:
            total_in_k = sum([c_k[child] for child in adjacency_dict[unit]])
            total_not_in_k = sum([c_notk[child] for child in adjacency_dict[unit]])
            c_k[unit] = min(total_in_k, total_not_in_k + 1)
            c_notk[unit] = min(total_in_k + 1, total_not_in_k)

    return c_k[levels_dict[CC.COUNTY][0]]


def entity_dist_obj_fxn(adjacency_dict: dict, levels_dict: dict, fun, cks: list):
    """
    Finds the off-spine entity distances (OSEDs) at the county geolevel for each off-spine entity (OSE) and then
    performs a reduce operation on this list
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param cks: each tuple element should be defined as the second output of recursion_through_tracts_entity_k()
    :param fun: a function that will be used in the reduce step of this function

    :return obj_fxn: the value of the function fun applied to the list of integer OSED values
    """

    cks = [dist_of_entity_k(adjacency_dict, levels_dict, c_k, c_notk) for c_k, c_notk in cks]
    return fun(cks)


def combine_geounits(units: list, adjacency_dict: dict, levels_dict: dict, parent: int,
                     level: str) -> Tuple[dict, dict]:
    """
    Combines the geounits in units.
    :param units: indices of geounits to be combined
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param parent: integer geoid of the parent of the geounits in units
    :param level: the geolevel of units

    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    """

    for sibling in deepcopy(units[1:]):
        # reassign the children of sibling to the parent units[0]:
        adjacency_dict[units[0]].extend(adjacency_dict[sibling])
        # The next three lines deletes the sibling of units[0]:
        levels_dict[level].remove(sibling)
        adjacency_dict[parent].remove(sibling)
        del adjacency_dict[sibling]

    return adjacency_dict, levels_dict


def lexicographic_gtoet(a: list, b: list) -> bool:
    """
    lexicographic "greater than or equal to" comparison. If the first order statistic for a and b that differ are such
    that this order statistic of a is greater than or equal to that of b, returns True, and otherwise returns False
    :param a: a list of integers
    :param b: a list of integers such that len(a) == len(b)

    :return is_larger_than: True if and only if a >= b in the lexicographic sense
    """

    assert len(a) == len(b)

    a = np.array(a)
    b = np.array(b)
    if np.all(a == b):
        return True
    idx = np.where((a > b) != (a < b))[0][0]
    if a[idx] > b[idx]:
        return True
    return False


def minimize_entity_distance(adjacency_dict: dict, levels_dict: dict, blocks_in_entities: list, entities_in_tract: dict,
                             fanout_cutoff: int, entity_threshold: int) -> Tuple[dict, dict]:
    """
    Approximates the tract-groups that minimize entity_dist_obj_fxn().
    We do not consider all combinations of adjacency relationships, so this is only an approximation.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param blocks_in_entities: blocks_in_entities[k] is a list of the blocks that are in entity k
    :param entities_in_tract: provides the list of entities in each tract
    :param fanout_cutoff: the fanouts of the block-groups and tract-groups will be no more than
    int(np.sqrt(number_of_tracts)) + fanout_cutoff at the end of the first optimization routine
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups

    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    """

    changed = True
    num_tracts = len(entities_in_tract)
    cutoff = int(np.sqrt(num_tracts) + fanout_cutoff)

    # Since, unlike the psuedocode in the Alternative Geographic Spine document, we do not iterate over block-groups, we
    # can simply fix the level iterator from the pseudocode at the tract-group:
    level = CC.TRACT_GROUP
    # All tract-groups have the same parent because we parallelized over counties.
    parent = levels_dict[CC.COUNTY][0]

    # Initialize tract-groups:
    while changed:
        changed = False
        tracts = list(entities_in_tract.keys())
        for i in range(len(tracts) - 1):
            # Uncommenting the next two lines may improve accuracy in some cases, but it will also take longer.
            # if len(entities_in_tract[tracts[i]]) > 1:
            #     continue
            q = [entities_in_tract[tracts[i]] == entities_in_tract[tracts[k]] for k in range(i + 1, len(tracts))]
            if any(q):
                # create lists of tract-groups to combine and then group them so that no group has more than threshold
                # geounits. Note that the parent of a tract with index tracts[i] is tracts[i] - num_tracts:
                combine_lists = [tracts[i] - num_tracts] + [tracts[k] - num_tracts for k in
                                                            range(i + 1, len(tracts)) if q[k - i - 1]]
                combine_lists = [combine_lists[k * cutoff:(k + 1) * cutoff] for k in
                                 range(int(np.ceil(len(combine_lists) / cutoff)))]
                for combine in combine_lists:
                    adjacency_dict, levels_dict = combine_geounits(combine, adjacency_dict, levels_dict, parent, level)
                for combine in combine_lists:
                    for tg in combine:
                        # Likewise, the child of a tract-group with index tg is tg + num_tracts:
                        del entities_in_tract[tg + num_tracts]
                changed = True
                break
            else:
                del entities_in_tract[tracts[i]]

    # Ignore entities that will automatically be close to the spine regardless of tract-groups:
    cks_tract = []
    for entity in blocks_in_entities:
        bound_county_c_k, c_k_and_c_notk_tract = recursion_through_tracts_entity_k(adjacency_dict, levels_dict, entity)
        if bound_county_c_k > entity_threshold:
            cks_tract.append(c_k_and_c_notk_tract)

    if len(cks_tract) == 0:
        # Comment out the following four lines to avoid combining TGs further in this case. This increases the number of TGs
        # bypassed (rather than counties) in move_to_pareto_frontier, but can also make fanouts at the county less favorable.
        combine_lists = deepcopy(levels_dict[CC.TRACT_GROUP])
        combine_lists = [combine_lists[k * cutoff:(k + 1) * cutoff] for k in range(int(np.ceil(len(combine_lists) / cutoff)))]
        for combine in combine_lists:
            adjacency_dict, levels_dict = combine_geounits(combine, adjacency_dict, levels_dict, parent, level)
        return adjacency_dict, levels_dict

    objective_init = entity_dist_obj_fxn(adjacency_dict, levels_dict, lambda x: x, cks_tract)
    objective_init.sort(reverse=True)
    finalized_units = []

    while True:
        combined_a_pair = False
        if len(levels_dict[level]) == 1:
            break

        # Find a pair (=[child, child's sibling]) such that the objective function is reduced when they are combined:
        siblings = [child for child in adjacency_dict[parent] if child not in finalized_units]
        for i, child in enumerate(siblings[:-1]):
            for sibling in siblings[i + 1:]:
                pair = [child, sibling]
                new_unit_fan_out = len(adjacency_dict[child]) + len(adjacency_dict[sibling])
                if new_unit_fan_out > cutoff:
                    continue
                # Test if combining the pair improves the objective function:
                adjacency_dict2, levels_dict2 = combine_geounits(pair, deepcopy(adjacency_dict),
                                                                 deepcopy(levels_dict), parent, level)
                objective_test = entity_dist_obj_fxn(adjacency_dict2, levels_dict2, lambda x: x, cks_tract)
                objective_test.sort(reverse=True)
                in_better_than_set = lexicographic_gtoet(objective_init, objective_test)
                if in_better_than_set:
                    objective_init = objective_test
                    combined_a_pair = True
                    adjacency_dict, levels_dict = adjacency_dict2, levels_dict2
                    break
            if combined_a_pair:
                break
            else:
                finalized_units.append(child)
        # If we cannot combine any pair of siblings without increasing the objective function:
        if not combined_a_pair:
            break

    return adjacency_dict, levels_dict


def make_plb_dicts(levels_dict: dict, user_plb_dict: dict) -> Tuple[dict, dict]:
    """
    Adds PLB allocation(s) to the values of the dictionary, adjacency_dict. For every geounit above the block-group
    geolevel, the format is {geoid:[children, plb_allocation]}, where plb_allocation is a Fraction. For block-groups,
    the format is, {geoid:[children, plb_allocation_children, plb_allocation]}, where plb_allocation_children is also
    a Fraction.
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param user_plb_dict: the user-specified PLB value for each geolevel

    :return plb_blocks: provides the PLB allocation of the child geounits for each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for each geounit above the block geolevel
    """

    geolevels = list(levels_dict.keys())
    plb_blocks = dict()
    plb_above_blocks = dict()

    for bg in levels_dict[CC.BLOCK_GROUP]:
        plb_blocks[bg] = user_plb_dict[CC.BLOCK]

    for level in geolevels:
        for unit in levels_dict[level]:
            plb_above_blocks[unit] = user_plb_dict[level]

    return plb_blocks, plb_above_blocks


def bypass_geounit(adjacency_dict: dict, levels_dict: dict, plb_blocks: dict, plb_above_blocks: dict, unit_level: str,
                   parent: int, unit: int, highest_geounit: int) -> Tuple[dict, dict, dict, dict, int]:
    """
    Bypasses the geounit, unit. This involves the following operations to preserve uniform depth length:
    Replaces geounit with geoid unit with a number of geounits given by its number of children. Each such geounit
    is the child of parent and the parent of one child of unit. The new PLB of these geounits are the sum of the plb
    allocated to its children and the geounit, unit. The PLB of the children is set to zero.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param plb_blocks: provides the PLB allocation of the child geounits for each block-group
    :param plb_above_blocks: provides the PLB allocation of the geounit for each geounit above the block geolevel
    :param unit: the geounit index of the geounit to be bypassed
    :param unit_level: the geolevel of unit
    :param parent: the parent geounit index
    :param highest_geounit: the largest geounit index

    :return highest_geounit: the largest geounit index
    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel
    """

    message = f"geounit {unit} is not in the {unit_level} geolevel of levels_dict."
    assert unit in levels_dict[unit_level], message

    for child in deepcopy(adjacency_dict[unit]):
        # Add new geounit to level of unit with one child:
        levels_dict[unit_level].append(highest_geounit + 1)
        adjacency_dict[highest_geounit + 1] = [child]
        # In each of the next four cases, we reallocate all PLB of child to this newly created geounit in two steps.
        # First, the PLB of parent is redefined, and then the PLB of the child is set to zero. Remark 2 in the
        # Alternative Geographic Spine Document describes how this ensures that the final sensitivity is correct.
        if unit_level == CC.BLOCK_GROUP:
            plb_above_blocks[highest_geounit + 1] = plb_above_blocks[unit] + plb_blocks[unit]
            plb_blocks[highest_geounit + 1] = Fraction(0, 1)
        else:
            plb_above_blocks[highest_geounit + 1] = plb_above_blocks[child] + plb_above_blocks[unit]
            plb_above_blocks[child] = Fraction(0, 1)
        # parent is the parent of the new geounit:
        adjacency_dict[parent].append(highest_geounit + 1)
        highest_geounit += 1

    # Delete old unit:
    if unit_level == CC.BLOCK_GROUP:
        del plb_blocks[unit]
    del adjacency_dict[unit]
    adjacency_dict[parent].remove(unit)
    levels_dict[unit_level].remove(unit)
    del plb_above_blocks[unit]

    return adjacency_dict, levels_dict, plb_blocks, plb_above_blocks, highest_geounit


def bypassing_improves_parent(parent_plb: Fraction, child_plbs: list, epsilon_delta: bool, bypass_geolevels: list,
                              unit_level: str, bypass_cutoff: int, num_siblings: int) -> bool:
    """
    Returns True if and only if bypassing the parent will improve the expected squared error of the OLS estimate for the
    parent. This in turn implies that bypassing will not decrease the expected squared error of the OLS estimate for any
    geounit, as described by Theorem 1 in the Alternative Geographic Spine document.
    :param parent_plb: PLB allocation of parent
    :param child_plbs: PLB allocations of each child
    :param epsilon_delta: True if and only if an approximate DP primitive will be used in the engine
    :param bypass_geolevels: the geolevels that should be bypassed
    :param unit_level: the geolevel of the parent geounit
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param num_siblings: the number of sibling geounits of the geounit for which bypassing is being considered

    :return bool: True if and only if bypassing parent will not increase the expected squared error of the OLS
    estimates for all geounits or unit_level is in bypass_geolevels
    """

    if unit_level in bypass_geolevels:
        return True
    if len(child_plbs) == 1:
        return True
    if epsilon_delta:
        return False
    if bypass_cutoff >= num_siblings + len(child_plbs) and min(child_plbs) * 2 >= (len(child_plbs) - 1) * parent_plb:
        return True

    return False


def move_to_pareto_frontier(adjacency_dict: dict, levels_dict: dict, plb_blocks: dict, plb_above_blocks: dict,
                            epsilon_delta: bool, bypass_geolevels: list, bypass_cutoff: int) -> Tuple[dict, dict, dict, dict]:
    """
    The algorithm bypasses over geounits when doing so would not increase the expected squared error of any query in
    any geolevel of the OLS estimator. (See Theorem 1 in the Alternative Geographic Spine document.)
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param plb_blocks: provides the PLB allocation of the child geounits for each block-group
    :param plb_above_blocks: provides the PLB allocation of the geounit for each geounit above the block geolevel
    :param epsilon_delta: True if and only if an approximate DP primitive will be used in the engine
    :param bypass_geolevels: the geolevels that should be bypassed
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children

    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel
    """

    geolevels = list(levels_dict.keys())
    highest_geounit = max(levels_dict[CC.BLOCK_GROUP])

    # Start from the block-group geolevel and move toward the root:
    for parent_level, unit_level in zip(geolevels[-2::-1], geolevels[-1:0:-1]):
        # Note that bypassing alters the adjacency relationships between three different geounits: the "unit", its
        # children, and its parent, so it is helpful to also have the parent ID available:
        for parent in deepcopy(levels_dict[parent_level]):
            for unit in deepcopy(adjacency_dict[parent]):
                if parent_level != CC.TRACT:
                    child_plbs = [plb_above_blocks[child] for child in adjacency_dict[unit]]
                else:
                    child_plbs = [plb_blocks[unit]] * len(adjacency_dict[unit])
                num_siblings = len(adjacency_dict[parent]) - 1
                if bypassing_improves_parent(plb_above_blocks[unit], child_plbs, epsilon_delta,
                                             bypass_geolevels, unit_level, bypass_cutoff, num_siblings):
                    adjacency_dict, levels_dict, plb_blocks, plb_above_blocks, highest_geounit =\
                        bypass_geounit(adjacency_dict, levels_dict, plb_blocks, plb_above_blocks, unit_level, parent,
                                       unit, highest_geounit)

    return adjacency_dict, levels_dict, plb_blocks, plb_above_blocks


def check_total_plb(adjacency_dict: dict, levels_dict: dict, plb_blocks: dict, plb_above_blocks: dict,
                    user_plb_dict: dict) -> bool:
    """
    Starts at the root geounit, sums PLBs down to the block geolevel, and then throws an error if sensitivity of the
    corresponding (row-weighted-)strategy matrix is not one
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel
    :param user_plb_dict: the user-specified PLB value for each geolevel

    :return bool: True if the sensitivity is the same as that of the user-specified PLB values
    """

    # This function is called just before reformatting the outputs and returning them to ensure the sensitivity of
    # the output is correct.

    # This function iteratively moves PLB (raised to either p=1 or p=2) from parents to their children. This ensures
    # that the weighted strategy matrix over all geolevels has the correct sensitivity by the logic outlined in
    # Remark 2 of the Alternative Geographic Spine Document:
    plb_above_blocks = deepcopy(plb_above_blocks)
    geolevels = list(levels_dict.keys())

    # Start by moving PLB down to block-group geolevel:
    for level in geolevels[:-1]:
        for parent in levels_dict[level]:
            parent_plb = plb_above_blocks[parent]
            for child in adjacency_dict[parent]:
                plb_above_blocks[child] = parent_plb + plb_above_blocks[child]

    target_plb = np.sum(np.array([plb_i for plb_i in list(user_plb_dict.values())]))

    for bg in levels_dict[CC.BLOCK_GROUP]:
        sensitivity = plb_above_blocks[bg] + plb_blocks[bg]
        message = f'Sensitivity for blocks in BG #{bg} are: {sensitivity} (!= {target_plb})'
        assert sensitivity == target_plb, message

    return True


def reformat_adjacency_dict(state_county: str, adjacency_dict: dict, levels_dict: dict, plb_blocks: dict,
                            plb_above_blocks: dict) -> Tuple[list, list, tuple]:
    """
    Encodes spine in its final format.
    :param state_county: a string with format [1 digit AIAN/non-AIAN][2 digit state][1 digit county/AIANNHCE]
    [4 digit county/AIANNHCE]
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel

    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geoid_mapping: elements are given by (block_geocode16_string, block_geocode_in_optimized_spine)
    :return widths: the fixed number of additional digits of DAS_geoid that is required to represent each geolevel at
    the county level and below
    """

    geolevels = list(levels_dict.keys()) + [CC.BLOCK]

    widths = []
    # Rename the artificial root geounit state_county:
    adjacency_dict[state_county] = adjacency_dict.pop(levels_dict["auxiliary_root_geounit"][0])
    plb_above_blocks[state_county] = plb_above_blocks.pop(levels_dict["auxiliary_root_geounit"][0])
    levels_dict["auxiliary_root_geounit"].append(state_county)
    levels_dict["auxiliary_root_geounit"].remove(levels_dict["auxiliary_root_geounit"][0])

    for level_id, level in enumerate(geolevels[:-2]):
        fan_outs = [len(adjacency_dict[parent]) for parent in levels_dict[level]]
        id_lengths = len(str(max(fan_outs)))
        widths.append(id_lengths)
        for parent_num, parent in enumerate(deepcopy(levels_dict[level])):
            # Define geocodes of children as list(range(1, 1 + fan_outs[parent_num])), after left padding with parent
            # geocode as well as the number of zeros required to ensure a fixed width:
            ids = [str(k) for k in range(1, fan_outs[parent_num] + 1)]
            ids = [parent + '0' * (id_lengths - len(idk)) + idk for idk in ids]
            for child, new_id in zip(deepcopy(adjacency_dict[parent]), ids):
                adjacency_dict[new_id] = adjacency_dict.pop(child)
                plb_above_blocks[new_id] = plb_above_blocks.pop(child)
                levels_dict[geolevels[level_id + 1]].append(new_id)
                levels_dict[geolevels[level_id + 1]].remove(child)
                # Note that level is geolevel of parent, and not the geolevel of child:
                if level == CC.TRACT:
                    plb_blocks[new_id] = plb_blocks.pop(child)
                assert type(plb_above_blocks[new_id]) is Fraction

    # widths[0] will correspond to width of county(/or AIANNHCE). Since we allow county to be bypassed, we will actually
    # view the county geocodes as the last five digits of state_county, joined with the required number of digits to
    # express the new number of geounits in this geolevel:
    widths[0] += 5

    geoid_mapping = []
    # we always represent the block id digits as the block id in [geocode16] format to simplify mapping
    # back to geocode16 format after top-down is run:
    widths.append(16)
    # Add PLB allocations of block geounits to plb_above_blocks to avoid making a new dictionary:
    for parent in levels_dict[CC.BLOCK_GROUP]:
        for child in adjacency_dict[parent]:
            geoid_mapping.append([child, parent + child])
            plb_above_blocks[parent + child] = plb_blocks[parent]
            assert type(plb_blocks[parent]) is Fraction

    # Delete auxiliary geounit at geolevel 0 from plb_above_blocks and redefine dictionary as a list of tuples:
    del plb_above_blocks[state_county]
    plb_above_blocks = [(k, v) for k, v in plb_above_blocks.items()]

    widths = tuple(widths)
    return plb_above_blocks, geoid_mapping, widths


def optimize_spine(state_county: str, adjacency_dict: dict, levels_dict: dict, blocks_in_entities: list,
                   entities_in_tract: dict, user_plb_dict: dict, fanout_cutoff: int, epsilon_delta: bool,
                   check_final_plb: bool, entity_threshold: int, bypass_cutoff: int, includes_tg: bool) -> Tuple[list, list, tuple]:
    """
    Provides an optimized geographic subspine with a county defined as the root geounit.
    :param state_county: a string with format [1 digit AIAN/non-AIAN][2 digit state][1 digit county/AIANNHCE]
    [4 digit county/AIANNHCE]
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param blocks_in_entities: blocks_in_entities[k] is a list of the blocks that are in entity k
    :param entities_in_tract: provides the list of entities in each tract
    :param user_plb_dict: the user-specified PLB value for each geolevel
    :param fanout_cutoff: the fanouts of the block-groups and tract-groups will be no more than
    int(np.sqrt(number_of_tracts)) + fanout_cutoff at the end of the first optimization routine
    geounits. Higher values often also result in more efficient use of the PLB over geolevels, as generally more
    geounits are bypassed in this case
    :param epsilon_delta: True if and only if an approximate DP primitive will be used in the engine
    :param check_final_plb: indicates whether to check the PLB allocations of the final spine
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param includes_tg: indicates if the user included tract-groups in the initial spine

    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geoid_mapping: elements are given by (block_geocode16_string, block_geocode_in_optimized_spine)
    :return widths: the fixed number of additional digits of DAS_geoid that is required to represent each geolevel at
    the county level and below
    """

    if includes_tg:
        # In this case, we do not want to include tract-groups in the initial spine, so we do not redefine them by
        # aggregating them together. These tract-group geounits will be bypassed move_to_pareto_frontier().
        adjacency_dict, levels_dict = minimize_entity_distance(adjacency_dict, levels_dict, blocks_in_entities,
                                                               entities_in_tract, fanout_cutoff, entity_threshold)
    # to free up some memory:
    del blocks_in_entities, entities_in_tract

    plb_blocks, plb_above_blocks = make_plb_dicts(levels_dict, user_plb_dict)

    bypass_geolevels = [] if includes_tg else [CC.TRACT_GROUP]

    adjacency_dict, levels_dict, plb_blocks, plb_above_blocks = move_to_pareto_frontier(adjacency_dict, levels_dict,
                                                                                        plb_blocks, plb_above_blocks,
                                                                                        epsilon_delta, bypass_geolevels,
                                                                                        bypass_cutoff)

    if check_final_plb:
        assert check_total_plb(adjacency_dict, levels_dict, plb_blocks,
                               plb_above_blocks, user_plb_dict), 'PLB check failed.'
    plb_mapping, geoid_mapping, widths = reformat_adjacency_dict(state_county, adjacency_dict, levels_dict,
                                                                 plb_blocks, plb_above_blocks)

    return plb_mapping, geoid_mapping, widths


def initial_geounit_index_ranges(num_tracts: int) -> dict:
    return OrderedDict([("auxiliary_root_geounit", (0, 1)), (CC.COUNTY, (1, 2)), (CC.TRACT_GROUP, (2, num_tracts + 2)),
                        (CC.TRACT, (num_tracts + 2, 2 * num_tracts + 2))])


def call_optimize_spine(state_county: str, row, user_plb_dict: dict, fanout_cutoff: int, epsilon_delta: bool,
                        check_final_plb: bool, entity_threshold: int, bypass_cutoff: int, includes_tg: bool) -> Tuple[list, list, tuple]:
    """
    Calls spine optimization routines for an individual county. Note that block-groups in the input spine may
    conform with the standard Census definition; however, this is not the case for the spine that is output from
    this function. Instead, these optimization routines redefine these geounits by their optimized counterparts,
    or geounits in the geolevel block-group-custom. (The standard Census definition is defined by:
    [2 digit state][3 digit county][6 digit tract][first digit of block ID].)
    :param state_county: a string with format [1 digit AIAN/non-AIAN][2 digit state][1 digit county/AIANNHCE]
    [4 digit county/AIANNHCE]
    :param row: tuple with length given by the number of blocks in the county and format of element i, (tract_i,
    block_i, Place_i/MCD_i, gq_OSE_i)
    :param user_plb_dict: the user-specified PLB value for each geolevel.
    :param fanout_cutoff: the fanouts of the block-groups and tract-groups will be no more than
    int(np.sqrt(number_of_tracts)) + fanout_cutoff at the end of the first optimization routine
    :param epsilon_delta: True if and only if the L2 sensitivity of strategy matrix of spine should be held fixed
    :param check_final_plb: indicates whether to check the PLB allocations of the final spine
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param includes_tg: indicates if the user included tract-groups in the initial spine

    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geoid_mapping: elements are given by (block_geocode16_string, block_geocode_in_optimized_spine)
    :return widths: the fixed number of additional digits of DAS_geoid that is required to represent each geolevel at
    the county level and below.
    """
    # Recall each element of row is formattd as (tract_i, block_i, OSEs_i, gq_OSE_i), where OSEs_i is a tuple containing the geographic codes of
    # off-spine entities to target in the spine optimization routines.

    # Sort by block geocode16 geoid:
    row = tuple(sorted(row, key=lambda d: d[1]))

    adjacency_dict = dict()
    intersect_entities = [''.join(row_k[2] + (row_k[3],)) for row_k in row]

    tracts = np.unique([row_k[0] for row_k in row])
    num_tracts = len(tracts)

    # Initialize "auxiliary_root_geounit" geolevel s.t. it contains geounit index 0, county geolevel s.t. it contains
    # geounit index 1, and the tract/tract-group geolevel s.t. each tract-group has one tract as a child:
    init_ranges = initial_geounit_index_ranges(num_tracts)
    levels_dict = OrderedDict((k, list(range(v[0], v[1]))) for k, v in init_ranges.items())
    adjacency_dict[init_ranges["auxiliary_root_geounit"][0]] = [init_ranges[CC.COUNTY][0]]
    adjacency_dict[init_ranges[CC.COUNTY][0]] = list(range(*init_ranges[CC.TRACT_GROUP]))
    values = [[k] for k in deepcopy(levels_dict[CC.TRACT])]
    adjacency_dict = {**adjacency_dict, **dict(zip(deepcopy(levels_dict[CC.TRACT_GROUP]), values))}

    # Initialize block-groups so that they group together blocks in a single off-spine entity that are within a tract.
    cur_bg_id = init_ranges[CC.TRACT][1]
    levels_dict[CC.BLOCK_GROUP] = []
    # Define block-groups and continue to assign them block children from the same off-spine entity as long as there are
    # no more than cutoff children assigned:
    for k, tract in enumerate(tracts):
        unique_intersect_entities_in_tract = np.unique([intersect_entities[n] for n, row_k in enumerate(row) if row_k[0] == tract])
        tract_id = init_ranges[CC.TRACT][0] + k
        adjacency_dict[tract_id] = []
        for i, entity in enumerate(unique_intersect_entities_in_tract):
            num_blocks_in_tract = sum([row_k[0] == tract for row_k in row])
            cutoff = int(np.sqrt(num_blocks_in_tract) + fanout_cutoff)
            blocks = [row_k[1] for n, row_k in enumerate(row) if intersect_entities[n] == entity and row_k[0] == tract]
            num_blocks_in_bg = 0
            adjacency_dict[cur_bg_id] = []
            adjacency_dict[tract_id].append(cur_bg_id)
            levels_dict[CC.BLOCK_GROUP].append(cur_bg_id)
            for h, block in enumerate(deepcopy(blocks)):
                if num_blocks_in_bg > cutoff:
                    cur_bg_id += 1
                    adjacency_dict[cur_bg_id] = []
                    adjacency_dict[tract_id].append(cur_bg_id)
                    levels_dict[CC.BLOCK_GROUP].append(cur_bg_id)
                    num_blocks_in_bg = 0
                adjacency_dict[cur_bg_id].append(block)
                num_blocks_in_bg += 1
            cur_bg_id += 1

    # Create blocks_in_entities input for optimize_spine(.):
    blocks_in_entities = []
    n_ose_types = len(row[0][2])
    for ose_type_index in range(n_ose_types):
        unique_oses = np.unique([row_k[2][ose_type_index] for row_k in row if not np.all([xi == "9" for xi in row_k[2][ose_type_index]])])
        for ose in unique_oses:
            blocks_in_entities.append({row_k[1] for row_k in row if row_k[2][ose_type_index] == ose})

    # Create entities_in_tract input for optimize_spine(.):
    entities_in_tract = dict()
    for k, tract in enumerate(tracts):
        tract_id = init_ranges[CC.TRACT][0] + k
        ose_tuples_in_tract = [row_k[2] for row_k in row if row_k[0] == tract]
        unique_entities_in_tract = np.unique([ose_tuple[k] for ose_tuple in ose_tuples_in_tract for k in range(n_ose_types)])
        entities_in_tract[tract_id] = unique_entities_in_tract.tolist()

    plb_mapping, geoid_mapping, widths = optimize_spine(state_county, adjacency_dict, levels_dict,
                                                        blocks_in_entities, entities_in_tract, user_plb_dict,
                                                        fanout_cutoff, epsilon_delta, check_final_plb,
                                                        entity_threshold, bypass_cutoff, includes_tg)

    return plb_mapping, geoid_mapping, widths
