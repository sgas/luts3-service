"""
OpenMetrics text exposition format rendering for the SGAS custom query engine.

Author: Erik Edelmann <erik.edelmann@csc.fi>
Copyright: NeIC (2026)
"""

MIME_TYPE    = 'application/openmetrics-text'
CONTENT_TYPE = 'application/openmetrics-text; version=1.0.0; charset=utf-8'


class OpenMetricsError(Exception):
    """
    Thrown if a query result cannot be rendered as OpenMetrics.
    """


def render(query_def, rows):
    """
    Render the result of a custom query (a list of dicts, as returned by
    db.dictquery) as OpenMetrics text exposition format, using the
    metric_name/metric_type/metric_help/metric_value/metric_labels
    configuration on the given QueryDefinition.

    Returns the rendered payload as utf-8 encoded bytes.
    """
    if not query_def.metric_value:
        raise OpenMetricsError('Query "%s" is not configured for openmetrics output (missing metric_value).' \
                                % query_def.query_name)

    name = query_def.metric_name

    lines = []
    if query_def.metric_help:
        lines.append('# HELP %s %s' % (name, _escapeHelp(query_def.metric_help)))
    lines.append('# TYPE %s %s' % (name, query_def.metric_type))

    for row in rows:
        lines.append(_renderSample(query_def, row))

    lines.append('# EOF')

    return ('\n'.join(lines) + '\n').encode('utf-8')


def _renderSample(query_def, row):

    metric_name_column = query_def.metric_name_column

    value_column = query_def.metric_value
    if value_column not in row:
        raise OpenMetricsError('Value column "%s" not present in query result.' % value_column)

    value = _coerceNumeric(row[value_column], value_column)

    label_columns = query_def.metric_labels
    if label_columns is None:
        label_columns = [ column for column in row.keys() if column not in (value_column, metric_name_column) ]

    labels = []
    for column in label_columns:
        if column not in row:
            raise OpenMetricsError('Label column "%s" not present in query result.' % column)
        labels.append('%s="%s"' % (column, _escapeLabelValue(row[column])))

    if metric_name_column:
        if metric_name_column not in row:
            raise OpenMetricsError('Metric name column "%s" not present in query result.' % metric_name_column)

        name = row[metric_name_column]
    else:
        name = metric_name

    if labels:
        return '%s{%s} %s' % (name, ','.join(labels), value)
    else:
        return '%s %s' % (name, value)


def _coerceNumeric(value, column_name):

    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return value
    if value is None:
        raise OpenMetricsError('Value column "%s" is NULL, cannot render as openmetrics.' % column_name)
    try:
        return float(value)
    except (TypeError, ValueError):
        raise OpenMetricsError('Value column "%s" has non-numeric value "%s", cannot render as openmetrics.' \
                                % (column_name, value))


def _escapeHelp(text):
    return text.replace('\\', '\\\\').replace('\n', '\\n')


def _escapeLabelValue(value):
    text = 'None' if value is None else str(value)
    text = text.replace('\\', '\\\\')
    text = text.replace('"', '\\"')
    text = text.replace('\n', '\\n')
    return text
