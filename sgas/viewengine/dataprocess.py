"""
Data processing functionaliy.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2011)
"""


import json

from sgas.viewengine import viewcore



def uniqueEntries(rows, idx):

    entries = {}
    for r in rows:
        entries[r[idx]] = True

    return entries.keys()


def buildDictList(rows, keys):

    entries = []

    for r in rows:
        assert len(r) == len(keys), 'Row length does not match key length'
        entries.append( dict( zip(keys, r)) )
    return entries


def createJavascriptData(dict_list):

    jd = json.dumps(dict_list, indent=4)
    # make the javascript more compact, but still look "nice"
    jd = jd.replace('{\n        ', '{ ')
    jd = jd.replace(', \n       ', ',')
    jd = jd.replace('\n    }, \n', '},\n')
    jd = jd.replace('\n    }'    , '}')
    return jd


def createMatrix(dict_list):

    matrix = {}

    for d in dict_list:
        batch = d.get(viewcore.BATCH)
        group = d.get(viewcore.GROUP, 0)
        value = d.get(viewcore.VALUE)

        matrix[batch,group] = value

    return matrix

