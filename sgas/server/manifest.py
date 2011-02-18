"""
Service manifest, i.e., various information about state of the server and
what has happened.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


class NoSuchPropertyError(KeyError):
    """
    Raised when trying to access a property that does not exist.
    """


class PropertyTypeError(TypeError):
    """
    Raised when trying to handle a property value in a way that does not
    match with its type.
    """


class Manifest:

    def __init__(self):

        self.properties = {}


    def setProperty(self, property_, value):

        self.properties[property_] = value


    def hasProperty(self, property_):

        return property_ in self.properties


    def getProperty(self, property_):

        try:
            return self.properties[property_]
        except KeyError, e:
            raise NoSuchPropertyError(e)


    def incrementProperty(self, property_, count):

        assert type(count) is int, 'Count parameter must be an integer'

        if not property_ in self.properties:
            self.properties[property_] = count
        else:
            self.properties[property_] += count


    def getAllProperties(self):

        return self.properties.copy()

