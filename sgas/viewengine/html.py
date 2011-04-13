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

NBSP = '&nbsp;'

P = _INDENT + '<p>\n'
SECTION_BREAK = _INDENT + '<p> &nbsp; \n' + P + '\n'


def createTitle(title):

    return _INDENT + '<h3>' + title + '</h3>' + '\n' + P


def createSectionTitle(title):

    return _INDENT + '<h5>' + title + '</h5>' + '\n' + P


def createParagraph(text):

    return _INDENT + text + ' <p>\n'


def createLink(url, description):

    return '<a href=%s>%s</a>\n' % (url, description)


def createRadioButtons(name, values, checked_value=None):

    RADIO_TEMPLATE = '''<input type="radio" name="%s" value="%s" %s />%s<br>'''

    inputs = []
    for value, description in values:
        checked = ''
        if checked_value == value:
            checked = 'checked'
        inputs.append( RADIO_TEMPLATE % (name, value, checked, description) )

    return '\n'.join(inputs)


def createSelector(title, name, options, current_option=None):

    selector = _INDENT + title + '\n' + _INDENT + ' <select name=%s>' % name

    if not current_option in options:
        selector += _INDENT * 2 + '<option selected>%s</option>\n' % current_option

    for o in options:
        assert type(o) is str, 'Selector option must be a string'
        if current_option == o:
            selector += _INDENT * 2 + '<option selected>%s</option>\n' % o
        else:
            selector += _INDENT * 2 + '<option>%s</option>\n' % o
    selector += _INDENT + '</select>\n'
    return selector


def createSelectorForm(action, selectors, buttons=None):

    form = _INDENT + '<form name="input" action="%s" method="get">\n' % action

    for sel in selectors:
        form += sel
        form += '    &nbsp; &nbsp;\n'

    if buttons is not None:
        form += '<br><br>'
        form += buttons
        form += '<br>'

    form += '\n' + _INDENT + '<input type="submit" value="Submit" />\n' + \
            '</form>\n'

    return form


