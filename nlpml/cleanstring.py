#!/usr/bin/env python
'''
Functions to clean strings by replacing HTML entities, escapes and the like with 
their equivalent or spaces
'''

import html
import codecs
import re
import htmlparser
import sys

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


def clean_string(thestring, replace_invalid=None, process_html=True, parse_html=True):
    """
    Return a copy of the string where html character entities, hex-escapes, unicode escapes and python string escapes
    are replaced with the actual utf-8 replacement. Anything that cannot be converted to utf is replaced with
    by the unicode replacement character by default or something specified as "replace_invalid".
    process_html controls if HTML stuff is processed at all, if yes, parse_html controls if the HTML uis
    getting fully parsed or if not, only HTML character entities are replaced (but not HTML code like links, paragraphs)
    """
    # step 1: parse HTML or remove HTML character entities
    if process_html:
        if parse_html:
            parser = htmlparser.MyHTMLParser()
            parser.reset()
            parser.feed(thestring)
            parser.close()
            thestring = parser.text()
        else:
            thestring = html.unescape(thestring)
    # step 2: remove hex escapes, and allow hex escapes sequences to get interpreted as UTF-8
    oldstring = thestring
    try:
        thestring = codecs.decode(codecs.escape_decode(thestring)[0], encoding="utf-8", errors='replace')
    except:
        # we get an error if the original string contains some weird backslash which could be
        # just a normal literal backslash or the result from going through the clean_string code twice,
        # for now we ignore this
        # print("Problem {} converting string: >{}<".format(sys.exc_info(), oldstring), file=sys.stderr)
        pass
    # step 3: if requested, use a different replacement
    if replace_invalid is not None:
        thestring = thestring.replace("\ufffd", replace_invalid)
    # step 4: replace various kinds of unicode-escapes by searching and replacing just the specific occurrences of the escapes
    thestring = RE_FIND_UESCAPES.sub(lambda m: codecs.decode(m.group(0), 'unicode-escape'), thestring)
    return thestring

