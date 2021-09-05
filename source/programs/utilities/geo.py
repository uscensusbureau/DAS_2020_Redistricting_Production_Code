class GeoHierarchy:
    def __init__(self, geocode_length, geonames, geoprefix_sizes):
        self.geocode_length = geocode_length # how long a data geocode is
        prefix_sizes = [int(x) for x in geoprefix_sizes]
        assert len(geonames) == len(prefix_sizes), "Geolevel names and geolevel lengths differ in size"
        assert self.geocode_length in prefix_sizes, f"There is no geolevel of length {self.geocode_length}"
        assert max(prefix_sizes) == self.geocode_length, f"Maximum geocode prefix must be at most {self.geocode_length}"
        ordered_info = sorted(zip(prefix_sizes, geonames))
        self.level_to_name = tuple(x[1] for x in ordered_info)
        self.level_to_size = tuple(x[0] for x in ordered_info)
        self.name_to_level = {n : i for (i,n) in enumerate(self.level_to_name)}
        self.size_to_level = {s : i for (i,s) in enumerate(self.level_to_size)}

    def getLevel(self, geocode):
        """ Returns the numeric level of a geocode"""
        size = len(geocode)
        assert size in self.size_to_level, f"Geocode {geocode} is not a defined level"
        return self.size_to_level[size]

    def getParentName(self, level_name):
        """ Gets parent level name from child lavel name"""
        level = self.getLevelOfName(level_name)
        return None if level == 0 else self.getNameOfLevel(level-1)

    def getParentGeocode(self, childgeo):
        """ returns the parent geocode of child geocode """
        lev = self.getLevel(childgeo)
        pargeo = None
        if lev > 0:
            par_size = self.level_to_size[lev-1]
            pargeo = childgeo[:par_size]
        return pargeo

    def isBelow(self, first: str, second: str):
        """ returns true if first is the name of a level below second """
        return self.getLevelOfName(first) < self.getLevelOfName(second)

    @property
    def levelNames(self):
        """ returns a tuple of the names of levels from top to bottom"""
        return self.level_to_name

    def getLevelOfName(self, name):
        """ returns the numeric level give level name"""
        return self.name_to_level[name]

    def getNameOfLevel(self, level):
        """ returns level name given numeric level """
        return self.level_to_name[level]

    @property
    def numLevels(self):
        """ returns the number of levels """
        return len(self.level_to_name)

    @property
    def leafName(self):
        """ returns the name of the leaf level """
        return self.level_to_name[-1]

    @property
    def rootname(self):
        """ returns the name of the root level """
        return self.level_to_name[0]

    def __str__(self):
        return "\n".join([
            "GeoHierarchy Details",
            f"Geocode length: {self.geocode_length}",
            f"Height: {self.numLevels}",
            f"Levels: {self.level_to_name}",
            f"Prefixes: {self.level_to_size}",
            f"Name to Level: {self.name_to_level}",
            f"Size to Level: {self.size_to_level}",
        ])