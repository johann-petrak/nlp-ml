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
import unicodedata

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
            thestring = html.unescape(thestring)
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
    # step 5: normalize unicode to composed compatibility mode
    thestring = unicodedata.normalize("NFKC", thestring)
    return thestring

if __name__ == "__main__":
    teststring = r"""Ligature ﬁ followed by html pound &pound; and HTML 969 for omega &#969; and ampersand &amp; <p>
    Ligature again as HTML 64257: &#64257; and UTF8 hex codes \xef\xac\x81 and unicode 16 \ufb01<p>
    a line with a <a href="asasa">link</a>
    And here is a <div>div</div>
    """
    print("Testing cleanstring with: \n", teststring)
    print(clean_string(clean_string(teststring)))
