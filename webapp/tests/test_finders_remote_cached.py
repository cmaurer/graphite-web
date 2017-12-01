"""
    use docker for local testing
    docker pull memcached
    docker run -p 127.0.0.1:11211:11211  --name my-memcache -d memcached

    basic test
    echo 'stats' | nc  -v  127.0.0.1 11211

"""
import logging
import types
import memcache
import sys
import traceback
import mock
from urllib3.response import HTTPResponse
from django.test import override_settings
from mock import patch
from graphite.finders.remote import RemoteFinder
from graphite.finders.remote_cached import RemoteCachedFinder
from graphite.finders.utils import FindQuery
from graphite.node import BranchNode, LeafNode
from graphite.util import json, pickle, StringIO, msgpack
from .base import TestCase

# Silence logging during tests
LOGGER = logging.getLogger()

# logging.NullHandler is a python 2.7ism
if hasattr(logging, "NullHandler"):
    LOGGER.addHandler(logging.NullHandler())

# Set test cluster servers


@override_settings(CLUSTER_SERVERS=['127.0.0.1'])
@override_settings(MEMCACHE_HOSTS=['127.0.0.1:11211'])
@override_settings(STORAGE_FINDERS=(
    'graphite.finders.remote.RemoteCachedFinder'
))
class RemoteCachedFinderTest(TestCase):

    def setUp(self):
        self.MC = memcache.Client(['127.0.0.1:11211'])

    def tearDown(self):
        self.MC.flush_all()
        self.MC.disconnect_all()

    def test_memcache_hosts_not_set(self):
        with self.settings(MEMCACHE_HOSTS=None):
            try:
                RemoteCachedFinder.factory()
            except Exception as e:
                pass
            else:
                self.fail(
                    'expected exception to be raised when MEMCACHE_HOSTS=None')

    def test_memcache_hosts_set(self):
        with self.settings(MEMCACHE_HOSTS=['127.0.0.1:11211']):
            try:
                remoteCacheFinder = RemoteCachedFinder.factory()
            except Exception as e:
                self.fail(
                    'Factory method should not throw exception when MEMCACHE_HOSTS is set.')
            else:
                pass

    def test_refresh_cluster_servers(self):
        with self.settings(MEMCACHE_HOSTS=['127.0.0.1:11211']):
            try:
                remoteCachedFinder = RemoteCachedFinder.factory()
                servers = remoteCachedFinder.refresh_cluster_servers()
                if len(servers) <= 0:
                    self.fail(
                        'The number of cluster servers returned was 0.  Not good.')
                else:
                    pass
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                self.fail('test_refresh_cluster_servers: unexpected exception')
            else:
                pass

    @patch('urllib3.PoolManager.request')
    @override_settings(INTRACLUSTER_HTTPS=False)
    @override_settings(REMOTE_STORE_USE_POST=True)
    @override_settings(REMOTE_FIND_TIMEOUT=10)
    def test_find_nodes(self, http_request):
        """
            find nodes against single cluster server.
            Baseline Test; should be the same as 'Remote'
        """
        finder = RemoteCachedFinder.factory()

        startTime = 1496262000
        endTime = 1496262060

        data = [{
            'path': 'a.b.c',
            'is_leaf': False,
        },
            {
            'path': 'a.b.c.d',
            'is_leaf': True,
        }]
        responseObject = HTTPResponse(body=StringIO(
            pickle.dumps(data)), status=200, preload_content=False)
        http_request.return_value = responseObject

        query = FindQuery('a.b.c', startTime, endTime)
        result = finder.find_nodes(query)

        self.assertIsInstance(result, types.GeneratorType)

        nodes = list(result)
        self.assertEqual(len(nodes), 2)
        self.assertIsInstance(nodes[0], BranchNode)
        self.assertEqual(nodes[0].path, 'a.b.c')
        self.assertIsInstance(nodes[1], LeafNode)
        self.assertEqual(nodes[1].path, 'a.b.c.d')

        self.assertEqual(http_request.call_args[0], (
            'POST',
            'http://127.0.0.1/metrics/find/',
        ))
        self.assertEqual(http_request.call_args[1], {
            'fields': [
                ('local', '1'),
                ('format', 'pickle'),
                ('query', 'a.b.c'),
                ('from', startTime),
                ('until', endTime),
            ],
            'headers': None,
            'preload_content': False,
            'timeout': 10,
        })

        self.assertEqual(len(nodes), 2)

        self.assertIsInstance(nodes[0], BranchNode)
        self.assertEqual(nodes[0].path, 'a.b.c')

        self.assertIsInstance(nodes[1], LeafNode)
        self.assertEqual(nodes[1].path, 'a.b.c.d')

        finder = RemoteFinder('https://127.0.0.1?format=msgpack')

        data = [
            {
                'path': 'a.b.c',
                'is_leaf': False,
            },
            {
                'path': 'a.b.c.d',
                'is_leaf': True,
            },
        ]
        responseObject = HTTPResponse(
            body=StringIO(msgpack.dumps(data)),
            status=200,
            preload_content=False,
            headers={'Content-Type': 'application/x-msgpack'}
        )
        http_request.return_value = responseObject

        query = FindQuery('a.b.c', None, None)
        result = finder.find_nodes(query)

        self.assertIsInstance(result, types.GeneratorType)

        nodes = list(result)

        self.assertEqual(http_request.call_args[0], (
            'POST',
            'https://127.0.0.1/metrics/find/',
        ))
        self.assertEqual(http_request.call_args[1], {
            'fields': [
                ('local', '1'),
                ('format', 'msgpack'),
                ('query', 'a.b.c'),
            ],
            'headers': None,
            'preload_content': False,
            'timeout': 10,
        })

        self.assertEqual(len(nodes), 2)

        self.assertIsInstance(nodes[0], BranchNode)
        self.assertEqual(nodes[0].path, 'a.b.c')

        self.assertIsInstance(nodes[1], LeafNode)
        self.assertEqual(nodes[1].path, 'a.b.c.d')

        # non-pickle response
        responseObject = HTTPResponse(body=StringIO(
            'error'), status=200, preload_content=False)
        http_request.return_value = responseObject

        result = finder.find_nodes(query)

        with self.assertRaisesRegexp(Exception, 'Error decoding find response from https://[^ ]+: .+'):
            list(result)

    def find_nodes_side_effect(*args, **kwargs):
        startTime = 1496262000
        endTime = 1496262060
        if args[1] == 'http://127.0.0.1/metrics/find/':
            return HTTPResponse(body=StringIO(pickle.dumps([{
                'path': 'a.b.c',
                'is_leaf': False,
            },
                {
                'path': 'a.b.c.d',
                'is_leaf': True,
            }])), status=200, preload_content=False)
        elif args[1] == 'http://1.1.1.1/metrics/find/':
            return HTTPResponse(body=StringIO(pickle.dumps([{
                'path': 'a.b.c.d.e',
                'is_leaf': False,
            }])), status=200, preload_content=False)

    @override_settings(CLUSTER_SERVERS=['127.0.0.1', '1.1.1.1'])
    @patch('urllib3.PoolManager.request', side_effect=find_nodes_side_effect)
    def test_find_nodes_multiple_servers(self, http_request):
        """
            Test find across multiple cluster servers whose addresses were retrieved from memcache
        """
        finder = RemoteCachedFinder.factory()

        cluster_servers = finder.refresh_cluster_servers()
        self.assertTrue(cluster_servers is not None)

        query = FindQuery('a.b.c', None, None)
        result = finder.find_nodes(query)
        self.assertIsInstance(result, types.GeneratorType)

        nodes = list(result)
        self.assertEqual(len(nodes), 3)

        self.assertIsInstance(nodes[0], BranchNode)
        self.assertEqual(nodes[0].path, 'a.b.c')

        self.assertIsInstance(nodes[1], LeafNode)
        self.assertEqual(nodes[1].path, 'a.b.c.d')

        self.assertIsInstance(nodes[2], BranchNode)
        self.assertEqual(nodes[2].path, 'a.b.c.d.e')

    @patch('urllib3.PoolManager.request')
    @override_settings(CLUSTER_SERVERS=['127.0.0.1'])
    @override_settings(INTRACLUSTER_HTTPS=True)
    @override_settings(REMOTE_STORE_USE_POST=True)
    @override_settings(REMOTE_FETCH_TIMEOUT=10)
    def test_RemoteFinder_fetch(self, http_request):
      finder = RemoteCachedFinder.factory()
      startTime = 1496262000
      endTime   = 1496262060
      data = [
        {
          'start': startTime,
          'step': 60,
          'end': endTime,
          'values': [1.0, 0.0, 1.0, 0.0, 1.0],
          'name': 'a.b.c.d',
        },
      ]
      responseObject = HTTPResponse(body=StringIO(pickle.dumps(data)), status=200, preload_content=False)
      http_request.return_value = responseObject

      result = finder.fetch(['a.b.c.d'], startTime, endTime)
      expected_response = [
        {
          'pathExpression': 'a.b.c.d',
          'name': 'a.b.c.d',
          'time_info': (1496262000, 1496262060, 60),
          'values': [1.0, 0.0, 1.0, 0.0, 1.0],
        },
      ]
      self.assertEqual(result, expected_response)
      self.assertEqual(http_request.call_args[0], (
        'POST',
        'https://127.0.0.1/render/',
      ))
      self.assertEqual(http_request.call_args[1], {
        'fields': [
          ('format', 'pickle'),
          ('local', '1'),
          ('noCache', '1'),
          ('from', startTime),
          ('until', endTime),
          ('target', 'a.b.c.d'),
        ],
        'headers': None,
        'preload_content': False,
        'timeout': 10,
      })

    def remote_finder_side_effect(*args, **kwargs):
        # print("\n>>remote_finder_side_effect:{} {}".format(args, kwargs))
        startTime = 1496262000
        endTime = 1496262060
        if args[1] == 'https://127.0.0.1/render/':
            return HTTPResponse(body=StringIO(pickle.dumps([{
              'start': startTime,
              'step': 60,
              'end': endTime,
              'values': [1.0, 0.0, 1.0, 0.0, 1.0],
              'name': 'a.b.c.d',
            }])), status=200, preload_content=False)
        elif args[1] == 'https://1.1.1.1/render/':
            return HTTPResponse(body=StringIO(pickle.dumps([])), status=200, preload_content=False)


    @patch('urllib3.PoolManager.request', side_effect=remote_finder_side_effect)
    @override_settings(CLUSTER_SERVERS=['127.0.0.1', '1.1.1.1'])
    @override_settings(INTRACLUSTER_HTTPS=True)
    @override_settings(REMOTE_STORE_USE_POST=True)
    @override_settings(REMOTE_FETCH_TIMEOUT=10)
    def test_RemoteFinder_fetch_multiple_servers(self, http_request):
      finder = RemoteCachedFinder.factory()

      startTime = 1496262000
      endTime   = 1496262060
      result = finder.fetch(['a.b.c.d'], startTime, endTime)
      expected_response = [
        {
          'pathExpression': 'a.b.c.d',
          'name': 'a.b.c.d',
          'time_info': (1496262000, 1496262060, 60),
          'values': [1.0, 0.0, 1.0, 0.0, 1.0],
        },
      ]
      self.assertEqual(result, expected_response)

    @patch('urllib3.PoolManager.request')
    @override_settings(CLUSTER_SERVERS=['127.0.0.1'])
    @override_settings(INTRACLUSTER_HTTPS=False)
    @override_settings(REMOTE_STORE_USE_POST=True)
    @override_settings(REMOTE_FIND_TIMEOUT=10)
    def test_get_index(self, http_request):
      finder = RemoteCachedFinder.factory()
      data = [
        'a.b.c',
        'a.b.c.d',
      ]
      responseObject = HTTPResponse(body=StringIO(json.dumps(data)), status=200, preload_content=False)
      http_request.return_value = responseObject
      result = finder.get_index({})
      self.assertIsInstance(result, list)

      self.assertEqual(http_request.call_args[0], (
        'POST',
        'http://127.0.0.1/metrics/index.json',
      ))
      self.assertEqual(http_request.call_args[1], {
        'fields': [
          ('local', '1'),
        ],
        'headers': None,
        'preload_content': False,
        'timeout': 10,
      })

      self.assertEqual(len(result), 2)

      self.assertEqual(result[0], 'a.b.c')
      self.assertEqual(result[1], 'a.b.c.d')

      # non-json response
      responseObject = HTTPResponse(body=StringIO('error'), status=200, preload_content=False)
      http_request.return_value = responseObject

      with self.assertRaisesRegexp(Exception, 'Error decoding index response from http://[^ ]+: .+'):
        result = finder.get_index({})

    def remote_get_index_side_effect(*args, **kwargs):
        if args[1] == 'http://127.0.0.1/metrics/index.json':
            return HTTPResponse(body=StringIO(json.dumps([
                'a.b.c',
                'a.b.c.d',
            ])), status=200, preload_content=False)
        elif args[1] == 'http://1.1.1.1/metrics/index.json':
            return HTTPResponse(body=StringIO(json.dumps([])), status=200, preload_content=False)

    @patch('urllib3.PoolManager.request', side_effect=remote_get_index_side_effect)
    @override_settings(CLUSTER_SERVERS=['127.0.0.1', '1.1.1.1'])
    @override_settings(INTRACLUSTER_HTTPS=False)
    @override_settings(REMOTE_STORE_USE_POST=True)
    @override_settings(REMOTE_FIND_TIMEOUT=10)
    def test_get_index_multiple_servers(self, http_request):
      finder = RemoteCachedFinder.factory()
      result = finder.get_index({})
      self.assertIsInstance(result, list)
