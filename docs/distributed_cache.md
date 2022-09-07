## Distribuert cache

RobotsEvaluator, DNS-resolver og Proxy trenger en distribuert cache for å kunne skalere horisontalt.

Alternativer:

- [Hazelcast](https://hazelcast.com/blog/hazelcast-sidecar-container-pattern/)
- [Olric](https://github.com/buraksezer/olric), [Olric cloud plugin](https://github.com/buraksezer/olric-cloud-plugin)
- [Groupcache](https://www.mailgun.com/blog/it-and-engineering/golangs-superior-cache-solution-memcached-redis/)
