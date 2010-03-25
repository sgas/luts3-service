"""
Chunk processing (for generating stock views).

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import time

from sgas.viewengine import dataprocess



def removeChunkAttributes(chunk, attributes):
    cc = chunk.copy()
    for attr in attributes:
        cc.pop(attr, None)
    return cc


def chunkDate(chunk):
    date = None
    try:
        date  = chunk['year']
        date += ' ' + chunk['month']
        date += ' ' + chunk['day']
    except KeyError:
        pass
    return date


def filterResults(chunks, filter):
    #print "CHUNKS PRE-FILTER", len(chunks)
    filtered_chunks = []
    for chunk in chunks:
        for attr, value in filter.items():
            if chunk.get(attr) != value:
                break
            cc = removeChunkAttributes(chunk, filter.keys())
            filtered_chunks.append(cc)
    #print "CHUNKS POST-FILTER", len(filtered_chunks)
    return filtered_chunks


def changeResolution(chunks, resolution):
    # allows different scope in vo and time
    # this is really just deleting the "right" information and resumming (done later)
    #print "BEFORE RES", chunks
    del_attrs = []
    vo_res    = resolution.get('vo', 2)
    date_res  = resolution.get('date', 2)
    if vo_res   <= 2: del_attrs.append('vo_role')
    if vo_res   <= 1: del_attrs.append('vo_group')
    if vo_res   <= 0: del_attrs.append('vo')
    if date_res <= 2: del_attrs.append('day')
    if date_res <= 1: del_attrs.append('month')
    if date_res <= 0: del_attrs.append('year')

    new_chunks = [ removeChunkAttributes(chunk, del_attrs) for chunk in chunks ]
    #print "AFTER RES", new_chunks
    return new_chunks


def groupResults(chunks, group):
    #print "GROUPING", chunks
    grouped_results = {}
    for chunk in chunks:
        group_attr = chunk.get(group)
        group_date = chunkDate(chunk)
        key = (group_attr, group_date)
        cc = removeChunkAttributes(chunk, [group] + ['year', 'month', 'day'])
        grouped_results.setdefault(key, []).append(cc)
    #print "GROUPED", grouped_results
    return grouped_results.items()


def sumChunks(chunks, sum_attrs):
    def scaleValue(value, attr):
        if attr in ('cputime', 'walltime'):
            return dataprocess.SECONDS_TO_HOURS(value)
        else:
            return value

    #print "SUMMING CHUNKS", chunks
    ccw_values = [ [ chunk.pop(attr) for attr in sum_attrs ] for chunk in chunks ]
    summed_chunks = [ sum(values) for values in zip(*ccw_values) ]
    scaled_summed_chunks = [ scaleValue(value, attr) for value, attr in zip(summed_chunks, sum_attrs) ]
    #print "SUMMED_CHUNKS", summed_chunks
    return scaled_summed_chunks


def sumGroups(grouped_results, sum_attrs):
    summed_grouped_results = [ (group, sumChunks(chunks, sum_attrs)) for group, chunks in grouped_results ]
    return summed_grouped_results


def chunkQuery(chunks, group, cluster=None, filter=None, resolution=None, sum_attributes=None):
    # group      : attr
    # cluster    : attr
    # filter     : { attr : value }
    # resolution : { attr : level }
    # values     : [ attr1, attr2 ]

    if sum_attributes is None:
        sum_attributes = ['count', 'cputime', 'walltime']

    t_start = time.time()

    if filter:
        chunks = filterResults(chunks, filter)
    if resolution:
        chunks = changeResolution(chunks, resolution)
    grouped_chunks = groupResults(chunks, group)
    summed_grouped_chunks = sumGroups(grouped_chunks, sum_attributes)

    t_end = time.time()
    t_delta = t_end - t_start
    print "Chunk processing took %s seconds" % round(t_delta,2)
    return summed_grouped_chunks

