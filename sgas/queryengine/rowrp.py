"""
Row result parser

Builds JSON objects from the resulting rows of a query.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""


def buildDictRecords(rows, query_args):

    records = []

    mapping = {}

    i = 0

    mapping[i] = 'machine_name'   ; i += 1
    mapping[i] = 'user_identity'  ; i += 1
    if 'vo_name' in query_args:
        mapping[i] = 'vo_name'    ; i += 1
    mapping[i] = 'start_date'     ; i += 1
    mapping[i] = 'end_date'       ; i += 1
    mapping[i] = 'n_jobs'         ; i += 1
    mapping[i] = 'cpu_time'       ; i += 1
    mapping[i] = 'wall_time'      ; i += 1

    for row in rows:
        assert len(row) == i, 'Rows structure assertion failed (%i/%i)' % (len(row), i)

        d = {}
        for j in range(0,i):
            d[mapping[j]] = row[j]
        records.append(d)

    return records

