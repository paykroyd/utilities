import re
from datetime import timedelta
from amazonproduct.api import API
import functions as fns

response_group = 'Images,ItemAttributes,Offers,EditorialReview'

aws_key = '1QTKKA77QPEB2MGMBYR2'
aws_secret = '643VohcwGOqct4C2AeLOHbmN4uR6Is2Gby8+4cX4'

result_ns = {'xmlns': 'http://webservices.amazon.com/AWSECommerceService/2011-08-01'}

def response_parser(fp):
    dom = fns.parse_xml(fp.read())
    return dom

client = API(aws_key,
             aws_secret,
             'us',
             'springpartner-20',
             processor=response_parser)


def get_items(response):
    return response.xpath('//xmlns:Item', namespaces=result_ns)


def get_price(item):
    """
    Returns the amazon price for an item.
    """
    result = item.xpath('//xmlns:LowestNewPrice/xmlns:FormattedPrice', namespaces=result_ns)
    if len(result) > 0:
        return result[0].text
    else:
        return None


def amazon_prime_eligible(item):
    """
    Returns True if this qualifies for Amazon Prime shipping.
    """
    results = item.xpath('//xmlns:IsEligibleForSuperSaverShipping', namespaces=result_ns)
    return any([True if e.text == '1' else False for e in results])


def asin_for_url(url):
    """
    Parses an ASIN from an amazon URL.
    """
    asin_patterns = [re.compile(".*amazon\\.co.*\\/(product|dp|lm|ASIN)\\/([^\\/\\?&]*).*"),
                     re.compile(".*amazon\\.co.*\\/(detail)\\/\\-\\/(.*?)[\\/\\?$].*"),
                     re.compile(".*amazon\\.co.*\\/.*(\\?|\\&)?a=([0-9a-zA-Z]*).*"),
                     re.compile(".*amazon\\.co.*?\\/d\\/(.*?)\\/.*")]
    isbnPatterns = [".*amazon\\.co.*\\/gp/aw/d/([0-9]*).*"]
    for pat in asin_patterns:
        m = pat.match(url)
        if m:
            return m.group(2)
    return None

def amazon_by_id(asin):
    results = None
    if True:
        return lookup_by_asin(asin, ResponseGroup=response_group)

    # TODO: support getting things by upc, ean, and isbn
    if not results and params.get('/product/upc'):
        results = lookup_by_upc(params.get('/product/upc'), SearchIndex='All', ResponseGroup=response_group)

    if not results and params.get('/product/ean'):
        results = lookup_by_ean(params.get('/product/ean'), SearchIndex='All', ResponseGroup=response_group)

    if not results and params.get('/book/isbn'):
        results = lookup_by_isbn(params.get('/book/isbn'), ResponseGroup=response_group)

    if results:
        return results[0]

    return []

def item_search(index, **kwargs):
    kwargs['ContentType'] = 'application/json'
    if 'ResponseGroup' not in kwargs:
        kwargs['ResponseGroup'] = 'Images,ItemAttributes,Offers,Reviews,EditorialReview'
    return client.item_search(index, **kwargs)


def lookup_item(term, **kwargs):
    kwargs['ContentType'] = 'application/json'
    if 'ResponseGroup' not in kwargs:
        kwargs['ResponseGroup'] = 'Images,ItemAttributes,Offers,Reviews,EditorialReview'
    return client.item_lookup(term, **kwargs)


def lookup_by_asin(asin, **kwargs):
    return lookup_item(asin, **kwargs)


def lookup_by_upc(upc, **kwargs):
    kwargs['IdType'] = 'UPC'
    if 'SearchIndex' not in kwargs:
        kwargs['SearchIndex'] = 'All'
    return lookup_item(upc, **kwargs)


def lookup_by_ean(upc, **kwargs):
    kwargs['IdType'] = 'EAN'
    if 'SearchIndex' not in kwargs:
        kwargs['SearchIndex'] = 'All'
    return lookup_item(upc, **kwargs)


def lookup_by_isbn(upc, **kwargs):
    kwargs['IdType'] = 'ISBN'
    if 'SearchIndex' not in kwargs:
        kwargs['SearchIndex'] = 'Books'
    return lookup_item(upc, **kwargs)
