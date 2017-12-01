
import sys
import memcache

# from graphite import settings
from django.conf import settings
from graphite.finders.remote import RemoteFinder
from graphite.readers.remote import RemoteReader
from graphite.logger import log
from graphite.util import logtime

major,minor = sys.version_info[:2]
py_version = sys.version.split()[0]

# Store CLUSTER_SERVERS addresses in a Cache
class RemoteCachedFinder(RemoteFinder):

    @classmethod
    def factory(cls):
        #test to see if MEMCACHE_HOSTS is set or not.  If not, it does not make sense to move any further.
        if settings.MEMCACHE_HOSTS is None:
            raise Exception("MEMCACHE_HOSTS is not set.  Please add this to local_settings.py")
        return cls()

    def __init__(self):
        """Initialize the finder."""

    def refresh_cluster_servers(self):
        '''
            should return only one Finder that is a facade to memcache -vs- one finder per CLUSTER_SERVER

            RemoteFinder -> memcache -> cluster_servers

            possible states:
                startup: no existing cache:
                    - should create cache entries

                startup: existing cache:
                    - should not update cache

                running: cache updated:
                    new cluster_server added to cache:
                        - should get updated cluster_server addresses

                    cluster_server removed from cache:
                        - should get updated cluster_server addresses

        '''
        # does settings.MEMCACHE_HOSTS exist?
        #  if no; throw Exception
        #  if yes; get CLUSTER_SERVERS from memcache
        #
        # does key exist in memcache?
        #  if not; use CLUSTER_SERVERS
        #  if yes; get from memcache
        try:
            MC = memcache.Client(settings.MEMCACHE_HOSTS)
            cluster_servers = MC.get("cluster_servers")
            if cluster_servers is None:
                cluster_servers = settings.CLUSTER_SERVERS
            else:
                cluster_servers = cluster_servers.replace('[', '').replace(']','').split(',')
            log.debug('retrieved cluster_servers from cache: {cluster_servers}'.format(cluster_servers=cluster_servers))
            return cluster_servers
        except Exception as err:
            log.exception(
                "RemoteCachedFinder: Error retrieving cluster_servers from cache: %s" %
                (err))
            raise Exception("Error retrieving cluster_servers from cache: %s" % (err))
        finally:
            #close connection to memcache
            MC.disconnect_all()

    @logtime
    def find_nodes(self, query, timer=None):
        cluster_servers = self.refresh_cluster_servers()
        for server in cluster_servers:
            log.debug("RemoteCachedFinder: find_nodes: using server: {}".format(server))
            remote = RemoteFinder(server)
            nodes = remote.find_nodes(query)
            for node in nodes:
                yield node

    def fetch(self, patterns, start_time, end_time, now=None, requestContext=None):
        cluster_servers = self.refresh_cluster_servers()
        fetch_results = []
        for server in cluster_servers:
            log.debug("RemoteCachedFinder: fetch: using server: {}".format(server))
            reader = RemoteReader(RemoteFinder(server), {}, patterns)
            fetch_results = fetch_results + reader.fetch_multi(start_time, end_time, now, requestContext)
        return fetch_results

    def get_index(self, requestContext):
        cluster_servers = self.refresh_cluster_servers()
        index_results = []
        for server in cluster_servers:
            log.debug("RemoteCachedFinder: get_index: using server: {}".format(server))
            finder = RemoteFinder(server)
            result = finder.get_index(requestContext)
            index_results = index_results + result
        return index_results
