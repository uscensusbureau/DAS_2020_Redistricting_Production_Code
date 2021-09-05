import das_utils as du


def getDenseDF_mapper(node, schema):
    """
    Spark map/flatMap function for creating pyspark.sql.Row objects from the geounit dict
    elemetns found in the RDD.

    Parameters
    ----------
    node : dict
        a dictionary containing information about a geounit node
        this mapper requires/assumes that the following items exist:
            'geocode' : str
            'raw' : programs.sparse.multiSparse object
            'syn' : programs.sparse.multiSparse object

    schema : programs.schema.schema.Schema objects


    Returns
    -------
    list
        a list of dict objects


    Notes
    -----
    - This mapper creates a Row for every cell in the histogram
    - This mapper uses the schema object to determine the columns that should be
      present in the Rows
    - To avoid dependencies on pyspark, these functions return dicts that can then
      be turned into pyspark.sql.Row objects in a subsequent map call.
    """
    import numpy as np
    if isinstance(node, dict):
        orig = node['raw'].toDense()
        # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        priv = node['syn'].toDense()
        geocode = node['geocode']
    else: # assume GeounitNode
        orig = node.raw.toDense()
        priv = node.syn.toDense()
        geocode = node.geocode

    rows = []
    for c, cell in enumerate(np.ndindex(schema.shape)):
        rowdict = {}
        rowdict['geocode'] = str(geocode)
        rowdict['orig'] = int(orig[cell])
        # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        rowdict['priv'] = int(priv[cell])
        for dim, level in enumerate(cell):
            rowdict[schema.dimnames[dim]] = str(level)
        row = rowdict
        rows.append(row)
    return rows




def getSparseDF_mapper(node, schema):
    """
    Spark map/flatMap function for creating pyspark.sql.Row objects from the geounit dict
    elements found in the RDD.

    Parameters
    ----------
    node : dict
        a dictionary containing information about a geounit node
        this mapper requires/assumes the following items exist:
            'geocode' : str
            'raw' : programs.sparse.multiSparse object
            'syn' : programs.sparse.multiSparse object

    schema : programs.schema.schema.Schema object


    Returns
    -------
    list
        a list of dict objects


    Notes
    -----
    - This mapper creates a Row for any cell in the histogram in which the
      'raw' count or 'syn' count is nonzero.
    - This mapper uses the schema object to determine the columns that should
      be present in the Rows.
    - To avoid dependencies on pyspark, these functions return dicts that can then
      be turned into pyspark.sql.Row objects in a subsequent map call.
    """
    import numpy as np
    data_types = ['raw', 'syn']
    data = {}

    for data_type in data_types:
        d = node.get(data_type) if isinstance(node, dict) else getattr(node, data_type)
        if d is not None:
            data[data_type] = d.sparse_array

    geocode = node.get('geocode') if isinstance(node, dict) else getattr(node, 'geocode')

    rows = []

    nz_indices_list = []
    for d in data.values():
        nz_indices_list += d.indices.tolist()
    nz_indices = np.unique(nz_indices_list).tolist()
    if len(nz_indices) == 0:
        nz_indices = [0]

    # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
    key_dict = {'raw': 'orig', 'syn': 'priv'}

    for ind in nz_indices:
        rowdict = {}
        # geocode column
        rowdict['geocode'] = str(geocode)

        # [orig, priv] column(s) (depending on what exists in the mpde)
        for key in data.keys():
            rowdict[key_dict[key]] = int(data[key][0,ind])

        # columns corresponding to schema dimensions
        cell = np.unravel_index(ind, schema.shape)
        for dim, level in enumerate(cell):
            rowdict[schema.dimnames[dim]] = str(level)

        row = rowdict
        rows.append(row)
    return rows




# privatized means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
def getMicrodataDF_mapper(node, schema, privatized=True, mangled_names=True, recoders=None):
    # TODO: provide support for node = GeounitNode, in addition to node = dict
    GEOCODE = "geocode"
    ORIG = "raw"
    PRIV = "syn"
    DATATYPE = "data_type"
    import numpy as np
    if privatized:
        datakey = PRIV
    else:
        datakey = ORIG
    for item in [datakey, GEOCODE]:
        assert item in node, f"Cannot create microdata; '{item}' not found in the node."
    data = node[datakey].sparse_array
    all_nonzero_indices = data.indices.tolist()
    rows = []
    for ind in all_nonzero_indices:
        rowdict = {}
        rowdict[GEOCODE] = node[GEOCODE]
        rowdict[DATATYPE] = str(datakey)
        num_records = int(data[0,ind])
        cell = np.unravel_index(ind, schema.shape)
        for dim, level in enumerate(cell):
            if mangled_names:
                dimname = f"{schema.mangled_dimnames[dim]}"
            else:
                dimname = f"{schema.dimnames[dim]}"
            rowdict[dimname] = str(level)
        if recoders is not None:
            for recode in du.aslist(recoders):
                rowdict = recode(rowdict)
        row = rowdict
        rows += [row]*num_records
    return rows
