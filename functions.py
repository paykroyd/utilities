# -*- coding: latin-1 -*-

from StringIO import StringIO
import collections
import functools
from datetime import timedelta, datetime, date
import time
import sys

class memoize(object):
    """
    Memoization decorator.

    Based on: https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        """
        Check to see if the arguments are already in cache. If not call the fn.
        """
        # if the arguments are no hashable then just call on the function
        if not isinstance(args, collections.Hashable):
            return self.func(*args)
        if args not in self.cache:
            self.cache[args] = self.func(*args)
        return self.cache[args]

    def __repr__(self):
        """
        Use the memoized function's docstring.
        """
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """
        Instance method calls.
        """
        return functools.partial(self.__call__, obj)


def imap_login(host, username, password):
    """
    Logs in and returns an imaplib.IMAP4_SSL object.

    :param host: imap hosts (e.g., imap.gmail.com)
    :param username:
    :param password:
    """
    import imaplib
    server = imaplib.IMAP4_SSL(host)
    server.login(username, password)
    return server


def imap_get_message_ids(imap):
    """
    Retrieves all the message ids in the current mailbox.

    :param imap: a valid imaplib.IMAP4_SSL object logged in.
    """
    if imap.state == 'AUTH':
        raise ValueError('A mailbox must be selected: (e.g., imap.select("Inbox"))')
    result, data = imap.search(None, 'ALL')
    assert result == 'OK'
    msg_ids = data[0].split()
    return msg_ids


def imap_delete_messages(imap, message_ids, verbose=False):
    """
    Deletes the messages and expunges them.

    This will automatically batch every 1,000 messages processed.

    :param imap:  a valid imaplib.IMAP4_SSL object logged in.
    :param message_ids: array of ids
    :param verbose: if True then logging to stdout happens
    """
    # TODO: do the batches as CSV rather than individual ids
    count = 0
    for batch in seq_batch(message_ids, 1000):
        imap.store(','.join(batch), '+FLAGS', '\\Deleted')
        count += len(batch)
        if verbose:
            print '%d deleted' % count
    imap.expunge()
    if verbose:
        print 'finished, %d deleted' % count


def seq_batch(seq, batchsize):
    """
    Create batches of batchsize elements from seq.
    """
    batch = []
    for x in seq:
        batch.append(x)
        if len(batch) == batchsize:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


def get_text_from_url(url):
    """
    Gets the text from this url. Raises an exception if the status is bad.
    """
    import requests
    resp = requests.get(url)
    resp.raise_for_status()
    if 'charset=utf-8' in resp.headers.get('content-type', ''):
        return resp.text
    else:
        return resp.content.decode('utf-8')


def parse_xml(text):
    """
    Returns an LXML document.
    """
    from lxml import etree
    try:
        return etree.XML(text)
    except ValueError:
        # stupid thing where if the xml declares encoding to include
        # the correct encoding, this barfs. For example, this would fail:
        # <?xml version="1.0" encoding="UTF-8"?>
        # so we'll try and remove it
        if text.startswith('<?'):
            return etree.XML(text[text.index('?>') + 2:])
        raise


def parse_html(html):
    """
    Returns an LXML document from the source html.

    :param html: html document string
    """
    from lxml import etree
    return etree.HTML(html)

def lxml_pretty_print(el_or_els):
    """
    Pretty prints the lxml element or elements.

    :param el_or_els: an Element or a sequence of them.
    """
    from lxml import etree
    try:
        print etree.tostring(el_or_els, pretty_print=True)
    except TypeError:
        if not el_or_els:
            print '[]'
        else:
            print '['
            print ',\n'.join([etree.tostring(el, pretty_print=True).strip() for el in el_or_els])
            print ']'


def lxml_get_child(node, selector, ns):
    """
    Returns child node found with the xpath selector.

    If there are none, it returns none. If there are multiple, it raises a
    ValueError.
    """
    if not selector:
        raise ValueError('xpath selector must be specified')

    children = node.xpath('./%s' % selector, namespaces=ns)
    if not children:
        return None
    expect(1, len(children), 'should only be one child with tag name: "%s"' % selector)
    return children[0]


def lxml_get_child_value(node, selector, ns=None):
    """
    Returns the text value of the child of the node with the given name.

    This assumes that there is only one child with that name and errors otherwise.
    """
    child = lxml_get_child(node, selector, ns)
    return child.text if child is not None else None


def lxml_get_attribute(node, selector, attribute, ns=None):
    """
    Selects a node based on the xpath selector and return its attribute value.
    """
    child = lxml_get_child(node, selector, ns)
    return child.attrib.get(attribute) if child is not None else None


def db_insert(conn, table, value):
    """
    Inserts a dict-based record into the table. Commit is not called.

    The function assumes that the fields of the dict map those of the table.

    :param conn: sqlite3 connection
    :param table: string name of table
    :param value: dict representing the record to insert
    """
    fields = []
    placeholders = []
    values = []
    for name, val in value.items():
        fields.append(name)
        values.append(val)
        placeholders.append('?')
    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table, ','.join(fields), ','.join(placeholders))
    conn.execute(sql, values)


def db_select(conn, sql):
    """
    Executes a select statement and returns the values as dicts.

    TODO: just a temporary ghetto parsing of select statements.
    Doesn't support * yet and 'from' in a select statement.

    :param conn:
    :param sql:
    """
    expect_that('select' in sql.lower())
    expect_that('from' in sql.lower())

    def extract_fields():
        s = sql.lower()
        start = s.index('select ') + 7
        end = s.index(' from')
        return [v.strip() for v in sql[start:end].split(',')]

    cur = conn.execute(sql)
    fields = extract_fields()
    return [dict(zip(fields, result)) for result in conn.execute(sql).fetchall()]


def expect(expected, actual, error_message=None):
    """
    A function can expect things about input, which if not met a ValueError is raised.
    """
    if expected != actual:
        raise ValueError('%s: "%s" expected but was "%s"' % (error_message or '', expected, actual))


def str_ellipsize(string, length):
    """
    Cuts the string off the length specified and adds '...'
    """
    if len(string) >= length:
        return string[:length - 3] + '...'
    else:
        return string


def str_quote(string):
    """
    Puts quotes around the value.
    """
    return '"' + str(string) + '"'


def random_string():
    """
    Returns a random string. (A UUID).
    """
    from uuid import uuid4
    return str(uuid4())


def random_file_name(extension, path=''):
    """
    Generates a random file name with the provided extension.

    :param extension: e.g., 'txt', 'tmp', 'py'
    :param path: optionally a path to prepend to the filename
    """
    filename = '%s.%s' % (random_string(), extension)
    return '%s/%s' % (path, filename) if path else filename


def view(string):
    """
    Opens a string in a system text editor.
    """
    from subprocess import call
    filename = random_file_name('txt', '/tmp')
    with open(filename, 'w') as f:
        f.write(string.encode('utf-8'))
    call(['atom', filename])


def dump(data, filename):
    """
    Assumes and 2D array and dumps it out as CSV.
    """
    with open(filename, 'w') as f:
      for row in data:
        f.write(','.join([str(val) for val in row]))
        f.write('\n')


def datetime_from_now(days):
    """
    Returns a datetime n days from now.
    """
    return datetime.now() + timedelta(days=days)


def yesterday(formatted=False):
    """
    Returns the date yesterday.

    :param formatted: if formatted is True, then it's returned in YYYY-mm-dd format.
    """
    d = datetime_from_now(-1)
    return d if not formatted else to_bigquery_date(d)


def iterate_dates(start, end):
    """
    Iterates every date from start to but not including end (by day)

    :param start: start date
    :param end: end date
    :returns: iterator of dates
    """
    d = start
    while d < end:
        yield d
        d += timedelta(days=1)


def daterange(start, end, days=1, months=0):
    """
    Returns an array of datetimes from start to end incremented by day or month.

    days and months cannot be used together.
    Incrementing by months with a start date whose day is > 28 could cause problems.

    :param start: a start date
    :param end: non-inclusive end date for range
    :param days: number of days to increment by
    :params months: number of months to increment by
    """

    values = []
    d = start
    while d < end:
        values.append(d)
        if days != 0:
          d += timedelta(days=days)
        else:
          year = d.year + (d.month - 1 + months) / 12
          month = ((d.month - 1 + months) % 12) + 1
          d = todate('%d-%02d-%02d' % (year, month, d.day))
    return values


def todate(datestr):
    """
    Handles the format YYYY-mm-dd and returns a date.
    """
    return datetime.strptime(datestr, '%Y-%m-%d')


def fromdate(date):
    """
    Returns a string from a date object in YYYY-mm-dd format.
    """
    return date.strftime('%Y-%m-%d')


def parsedate(datestr):
    """
    A more general date parsing method than todate.
    """
    import dateutil.parser
    def from_epoch(val):
      t = time.gmtime(val / 1000)
      dt = datetime(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour,
                    t.tm_min, t.tm_sec, 1000 * (val - (int((val / 1000)) * 1000)))
      return dt
    try:
      return from_epoch(datestr)
    except TypeError:
      return dateutil.parser.parse(datestr)


def first_day_of_week(d):
    """
    Returns the first day of the week that d is in.
    """
    return d - timedelta(days=d.weekday())


def db_insert(conn, table, value):
    """
    Inserts a dict-based record into the table. Commit is not called.

    The function assumes that the fields of the dict map those of the table.

    :param conn: sqlite3 connection or cursor
    :param table: string name of table
    :param value: dict representing the record to insert
    """
    fields = []
    placeholders = []
    values = []
    for name, val in value.items():
        fields.append(name)
        values.append(val)
        placeholders.append('?')
    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table, ','.join(fields), ','.join(placeholders))
    conn.execute(sql, values)


def domain_from_url(url):
    """
    Returns the domain from a URL string.

    :param url:
    :return:
    """
    from urlparse import urlparse
    r = urlparse(url)
    return r.netloc


def histogramize_counter(counter, *maxes):
    """
    Creates a histogram from the collections.Counter using the maxes passed in.
    """
    values = [0] * (len(maxes) + 1)
    for val in counter.keys():
      found = False
      for n in range(len(maxes)):
        if val < maxes[n]:
          values[n] += counter[val]
          found = True
          break
      if found == False:
        values[-1] += 1
    return values


def histogram_print(hist, *maxes):
  """
  Prints out a histogram to stdout.
  """
  start = 0
  for n in range(len(maxes)):
    print '%d - %d, %d' % (start, maxes[n] - 1, hist[n])
    start = maxes[n]
  print '%d+, %d' % (start, hist[-1])


def print_dict(d):
    """
    Pretty print a dict.
    """
    for item, val in d.items():
        print '%s: %s' % (str(item), str(val))


def diff_counters(c1, c2):
    """
    Returns a new Counter with semantics similar to c1 - c2 but with missing elements.
    """
    c3 = collections.Counter(c1)
    keys = set(c1.keys())
    keys.update(c2.keys())
    for key in keys:
        c3[key] = c1[key] - c2[key]
    return c3


def do_each(seq, fn, report_every=100):
    """
    Runs a fn on each item in the sequence and reports to console every n items.

    I'm sick of writing the same code over and over again to keep count in
    a for-each loop and printing to the console.

    :param seq: the items to iterate over
    :param fn: function to run
    :param report_every: update console after this many have been processed
    """
    count = 0
    for item in seq:
        fn(item)
        count += 1
        if count % report_every == 0:
            sys.stdout.write('\r%d items processed' % count)
            sys.stdout.flush()
    sys.stdout.write('Finished. %d items processed\n' % count)


def read_csv(path):
    """
    Reads a quote delimited CSV file.

    :param path: path to the csv file
    """
    values = []
    with open(path) as f:
        for line in f:
            values.append([field.strip('"') for field in line.strip().split(',')])
    return values
