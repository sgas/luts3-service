"""
HTML templating for viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010-2011)
"""


HTML_VIEWBASE_HEADER = """<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>%(title)s</title>
        <link rel="stylesheet" type="text/css" href="/static/css/view.css" />
    </head>
    <body>
"""

HTML_VIEWBASE_FOOTER = """   </body>
</html>
"""

HTML_VIEWGRAPH_HEADER = """<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>%(title)s</title>
        <link rel="stylesheet" type="text/css" href="/static/css/view.css" />
        <script type="text/javascript" src="/static/js/protovis-r3.2.js"></script>
        <script type="text/javascript" src="/static/js/protovis-helper.js"></script>
    </head>
    <body>
"""

HTML_VIEWGRAPH_FOOTER  = HTML_VIEWBASE_FOOTER



_INDENT = ' ' * 4

P = _INDENT + '<p>\n'
SECTION_BREAK = _INDENT + '<p> &nbsp; \n' + P + '\n'


def createTitle(title):

    return _INDENT + '<h3>' + title + '</h3>' + '\n' + P


def createSectionTitle(title):

    return _INDENT + '<h5>' + title + '</h5>' + '\n' + P


def createParagraph(text):

    return _INDENT + text + ' <p>\n'


