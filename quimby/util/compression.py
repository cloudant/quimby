
import gzip as gziplib
import StringIO


def gzip(string):
    out = StringIO.StringIO()
    f = gziplib.GzipFile(fileobj=out, mode='wb')
    f.write(string)
    f.close()
    return out.getvalue()
