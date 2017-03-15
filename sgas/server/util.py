

def has_headers(req, header):
    try:
       r = header in req.received_headers
    except AttributeError:
        r = req.requestHeaders.hasHeader(header)

    return r


def get_headers(req, header):
    try:
        h = req.received_headers.get(header)
    except AttributeError:
        h = req.requestHeaders.getRawHeaders(header)[0]

    return h

