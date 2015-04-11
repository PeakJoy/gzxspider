# -*- coding: utf-8 -*-
import gzip
import chardet
import datetime
import time
import logging
from urllib import request
from io import BytesIO
from urllib.error import HTTPError
from urllib.error import URLError

'''HTTP访问封装模块'''

'''消息头'''
HEAD = {'Accept' : '*/*',
        'Accept-Language' : 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
        'Connection' : 'keep-alive',
        'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; rv:25.0) Gecko/20100101 Firefox/25.0'}

def getRequest(url, data = None, headers = HEAD, origin_req_host = None, unverifiable = False, method = 'GET'):
    '''获取request对象'''
    return request.Request(url, data = data, headers = headers, origin_req_host = origin_req_host, unverifiable = unverifiable, method=method)

def getResponse(url, data = None, headers = HEAD, origin_req_host = None, unverifiable = False, method = 'GET'):
    '''获取response对象'''
    return request.urlopen(getRequest(url, data, headers, origin_req_host, unverifiable, method))

def getResponseData(url, headers = HEAD):
    '''获取http返回数据，可以处理gzip压缩数据，并将各种数据编码转换为utf8格式'''
    logger = logging.getLogger('GzxSpider')
    try:
        resp = getResponse(url, headers = headers)
        contentEncoding = resp.headers.get('Content-Encoding')
        respData = resp.read()
    except HTTPError as httperror:
        logger.error('%s. The url is %s' % (repr(httperror), url))
        return b''
    except URLError as urlerror:
        logger.error('%s. The url is %s' % (repr(urlerror), url))
        return b''
    except Exception as e:
        logger.error('%s. The url is %s' % (repr(e), url))
        return b''

    # 如果为gzip文件，解压为字节流
    if contentEncoding == 'gzip':
        f = open('page.gzip', 'wb')
        f.write(respData)
        f.close()
        respData = gzip.GzipFile(fileobj = BytesIO(respData)).read()

    # 判断当前字节流编码，最终转码为utf8
    stringEncode = chardet.detect(respData[0:2056])['encoding'].lower()
    if stringEncode.startswith('gb'):
        stringEncode = 'gb18030'
    try:
        data = respData.decode(stringEncode).encode('utf-8')
    except UnicodeDecodeError as decodeerror:
        logger.warning('%s. The url is %s' % (repr(decodeerror), url))
    except Exception as e:
        logger.error('%s. The url is %s' % (repr(e), url))
        data = b''
    finally:
        return data
