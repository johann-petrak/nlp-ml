#!/usr/bin/env python
'''
Functions to clean strings by replacing HTML entities, escapes and the like with 
their equivalent or spaces
'''

import html
import codecs
import re


RE_FIND_UESCAPES = re.compile(r'''\\U[0-9a-fA-F]{8,8}|\\u[0-9a-fA-F]{4,4}|\\x[0-9a-fA-F]{2,2}\\[0-7]{1,3}\\[\\'"abfnrtv]''', re.UNICODE)


def clean_string(thestring, replace_invalid=None):
    """
    Return a copy of the string where html character entities, hex-escapes, unicode escapes and python string escapes
    are replaced with the actual utf-8 replacement. Anything that cannot be converted to utf is replaced with
    by the unicode replacement character by default or something specified as "replace_invalid"
    """
    # step 1: replace HTML character entities with their unicode equivalent
    thestring = html.unescape(thestring)
    # step 2: remove hex escapes, and allow hex escapes sequences to get interpreted as UTF-8
    # This does not handle unicode or python escapes but it can deal with strings which already
    # contain other unicode characters and it should be able to properly decode successive hex escapes
    # This will replace all locations which something could not be decoded with the \ufffd character.
    thestring = codecs.decode(codecs.escape_decode(thestring)[0], encoding="utf-8", errors='replace')
    # step 3: if requested, use a different replacement
    if replace_invalid is not None:
        thestring = thestring.replace("\ufffd", replace_invalid)
    # step 4: replace various kinds of unicode-escapes by searching and replacing just the specific
    # occurrences of the escapes. This specifically only replaces the matching patterns since the
    # unicode-escape decoding will not work if the string already contains non-latin1 (unicode) chars.
    thestring = RE_FIND_UESCAPES.sub(lambda m: codecs.decode(m.group(0), 'unicode-escape'), thestring)
    return str
