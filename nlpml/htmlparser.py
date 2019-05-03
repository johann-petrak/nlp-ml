"""
How to parse the (X)HTML we get into a sequence of paragraphs and gather
a bit of additional data for this.
TODO: maybe consider https://github.com/kennethreitz/requests-html 
"""

from html.parser import HTMLParser
# import preprocessing

class MyHTMLParser(HTMLParser):

    def __init__(self):
        kwargs = {}
        HTMLParser.__init__(self, **kwargs)
        self.ignore = False
        self.data = []
        self.p = []
        self.TAGS = ['p', 'br', 'tr', 'div', 'caption']


    def finishp(self):
        if len(self.p) > 0:
            self.data.append(self.p)
            self.p = []

    def handle_starttag(self, tag, attrs):
        # print("Encountered a start tag:", tag)
        if tag in ['script', 'style']:
            self.ignore = True
        elif tag in self.TAGS:
            self.finishp()
        # any tags that need to get replaced by space?
        # elif tag in ['???']:
        #     self.p.append(" ")

    def handle_endtag(self, tag):
        # print("Encountered an end tag :", tag)
        if tag in ['script', 'style']:
            self.ignore = False
        elif tag in self.TAGS:
            self.finishp()
        # any tags that need to get replaced by space?
        # elif tag in ['???']:
        #     self.p.append(" ")

    def handle_startendtag(self, tag, attrs):
        # print("Encountered a startend tag:", tag)
        if tag in self.TAGS:
            self.finishp()

    def handle_data(self, data):
        # print("Encountered some data  :", data)
        if not self.ignore:
            self.p.append(data)

    def close(self):
        HTMLParser.close(self)
        self.finishp()

    def reset(self):
        HTMLParser.reset(self)
        self.data = []
        self.p = []

    def text(self):
        """
        Convert back to text.
        """
        pars = []
        for par in self.data:
            if len(par) > 0:
                text = ''.join(par).strip()
                if text:
                    pars.append(text)
        return "\n".join(pars)


if __name__ == '__main__':

    html = """
    <article><script>SCRIPT</script><style>STYLE</style>
    some <em>text</em><p>more <br> text 1</p>text2 with a <a href="ccc">link</a> and another
     <a href="asas">link</a><p/>text3 ampersand: &amp; and umlaut-a &auml; and copyright &#169; and less &lt; <br/>text4<p>some dangling
      text, no closing article.<br> 
    Text after line break, contains a <div>html div</div>.
    <p>
    Also, a table with two rows:<br>
    <table>
    <tr><td>first row</td></tr>
    <tr><td>second row</td></tr>
    </table>
    """
    print("Original HTML: ", html)
    parser = MyHTMLParser()
    parser.reset()
    parser.feed(html)
    parser.close()
    print("parsed data: ", parser.data)
    print("text: ", parser.text())

