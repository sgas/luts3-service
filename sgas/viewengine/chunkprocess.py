"""
Chunk processing (for generating stock views).

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""



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


def sumChunks(chunks, values):
    #print "SUMMING CHUNKS", chunks
    ccw_values = [ [ chunk.pop(value) for value in values ] for chunk in chunks ]
    summed_chunks = [ sum(values) for values in zip(*ccw_values) ]
    #print "SUMMED_CHUNKS", summed_chunks
    return summed_chunks


def sumGroups(grouped_results, values):
    summed_grouped_results = [ (group, sumChunks(chunks, values)) for group, chunks in grouped_results ]
    return summed_grouped_results


def chunkQuery(chunks, group, cluster=None, filter=None, resolution=None, values=None):
    # group      : attr
    # cluster    : attr
    # filter     : { attr : value }
    # resolution : { attr : level }
    # values     : [ attr1, attr2 ]

    if values is None:
        values = ['count', 'cputime', 'walltime']

    if filter:
        chunks = filterResults(chunks, filter)
    if resolution:
        chunks = changeResolution(chunks, resolution)
    grouped_chunks = groupResults(chunks, group)
    summed_grouped_chunks = sumGroups(grouped_chunks, values)
    return summed_grouped_chunks

