
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
import ujson
from tornado.ioloop import IOLoop
from tornado import gen

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

class Riak(object):
    def __init__(self,
                 riak_url=None,
                 io_loop=None):
        self._io_loop = io_loop or IOLoop.current()
        self._client = AsyncHTTPClient(self._io_loop)
        if not riak_url:
            self._riak_url = "http://127.0.0.1:8098/"
        else:
            self._riak_url = riak_url

    @gen.coroutine
    def _return_response(self, data):
        data = ujson.loads(data.body)
        if "buckets" in data:
            data = data["buckets"]
        elif "props" in data:
            data = data["props"]
        print data
        raise gen.Return(data)

    @gen.coroutine
    def get_bucket_properties(self, bucket_name="_riak_client_test"):
        print "in get_bucket_properties"
        r = yield self._http_get(url="buckets/{0}/props".format(bucket_name))
        raise gen.Return(r)


    @gen.coroutine
    def _http_get(self, url):
        request = HTTPRequest(url=self._riak_url + url,
                              method="GET")
        try:
            response = yield self._client.fetch(request)
        except Exception, e:
            pass
        raise gen.Return(self._return_response(response))

    @gen.coroutine
    def list_buckets(self):
        r = yield self._http_get(url="buckets?buckets=true")
        raise gen.Return(r)

if __name__ == '__main__':
    R = Riak(riak_url="http://172.16.0.105:8098/")
    IOLoop.instance().run_sync(R.list_buckets)
    IOLoop.instance().run_sync(R.get_bucket_properties)
