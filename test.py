
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
import ujson
from tornado.ioloop import IOLoop
from tornado import gen

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
class BucketProperties(object):
    def __init__(self,
                 **kwargs):
        #for k, v in kwargs:
        #    setattr(self, k, v)
        self.__dict__.update(kwargs)

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
    def _return_response(self, data, key_type=None):
        data = ujson.loads(data.body)
        if "buckets" or "keys" in key_type:
            data = data[key_type]
        elif "props" in key_type:
            data = BucketProperties(**data[key_type])
        elif not key_type:
            print data
        raise gen.Return(data)

    @gen.coroutine
    def get_bucket_properties(self, bucket_name="_riak_client_test"):
        r = yield self._http_get(
            url="buckets/{0}/props".format(bucket_name),
            key_type="props")
        raise gen.Return(r)


    @gen.coroutine
    def _http_get(self, url, key_type):
        request = HTTPRequest(
            url=self._riak_url + url,
            method="GET")
        try:
            response = yield self._client.fetch(request)
        except Exception, e:
            pass
        raise gen.Return(self._return_response(response, key_type))

    @gen.coroutine
    def __http_put(self, bucket, key, data):
        data = ujson.dumps(data)
        request = HTTPRequest(
            url="buckets/{0}/keys/{1}/".format(
                bucket,
                key),
            method="PUT",
            headers= {"Content-Type": "application/json"},
            body=data)
        try:
            response = yield self._client.fetch(request)
        except Exception, e:
            pass
        raise gen.Return(self._return_response(response))

    @gen.coroutine
    def list_keys(self, bucket_name="riak_client_test"):
        r = yield self._http_get(
            url="buckets/{0}/keys?keys=true".format(bucket_name),
            key_type="keys")
        raise gen.Return(r)

    @gen.coroutine
    def list_buckets(self):
        r = yield self._http_get(
            url="buckets?buckets=true",
            key_type="buckets")
        raise gen.Return(r)


if __name__ == '__main__':
    from __main__ import Riak
    import timeit
    from __main__ import IOLoop
    R = Riak(riak_url="http://172.16.0.105:8098/")
    IOLoop.instance().run_sync(R.get_bucket_properties)
    IOLoop.instance().run_sync(R.list_keys)
    IOLoop.instance().run_sync(R.list_keys)
