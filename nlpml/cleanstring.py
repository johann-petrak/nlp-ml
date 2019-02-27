#!/usr/bin/env python
'''
Functions to clean strings by replacing HTML entities, escapes and the like with 
their equivalent or spaces
'''

import html
import codecs
import re

# Notes: 
# replace hex codes which represent unicode, but replace invalid stuff with spaces:
# codecs.decode(codecs.escape_decode(r"äà\xc3\xa4")[0], encoding="utf-8", errors='replace') 
#   followed by replace all � with space
# However this does NOT decode stuff like \n or \t
# Also does not decode unicode escapes of the form \u00e4 
# To decode unicode escapes from a string, we need to use codecs.decode(thestring, encoding="unicode-escape")
# This also handles python string escapes like tab etc.
# but this does not handle anything that is already unicode! So we can only apply it to the pattern itself!

# Once we have a unicode string we should normalise to NFC or NFD using unicodedata.normalize("NFC", ustring)
# We probably want "NFC"or "NFKC" here: both decompose then compose again but NFKC uses compatibility mode
# so e.g. a ligature fi will get composed into separate fi characters which we probably want usually.

# Possible strategy to do this:
# 1) replace HTML character entities use html.unescape 
# 2) use above strategy to replace hex escapes, this should leave unicode escapes and python string escapes intact
# 3) then iterate over the unicode escapes and the python string escapes and replace each with the unicode-escape method

RE_FIND_UESCAPES = re.compile(r'''\\U[0-9a-fA-F]{8,8}|\\u[0-9a-fA-F]{4,4}|\\x[0-9a-fA-F]{2,2}\\[0-7]{1,3}\\[\\'"abfnrtv]''', re.UNICODE)


def clean_string(thestring, replace_invalid=None):
    """
    Return a copy of the string where html character entities, hex-escapes, unicode escapes and python string escapes
    are replaced with the actual utf-8 replacement. Anything that cannot be converted to utf is replaced with
    by the unicode replacement character by default or something specified as "replace_invalid"
    """
    # step 1: remove HTML character entities
    str = html.unescape(thestring) 
    # step 2: remove hex escapes, and allow hex escapes sequences to get interpreted as UTF-8
    str = codecs.decode(codecs.escape_decode(str)[0], encoding="utf-8", errors='replace')    
    # step 3: if requested, use a different replacement
    if replace_invalid is not None:
        str = str.replace("\ufffd", replace_invalid)
    # step 4: replace various kinds of unicode-escapes by searching and replacing just the specific occurrences of the escapes
    str = RE_FIND_UESCAPES.sub(lambda m: codecs.decode(m.group(0), 'unicode-escape'), str)
    return str


if __name__ == "__main__":
    
