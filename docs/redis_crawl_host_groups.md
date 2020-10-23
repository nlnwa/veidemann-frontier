# Crawl Host Group

The Crawl Host Group (CHG) is a concept for applying politeness to crawling. A CHG maps to one or more hosts
represented by hostname or IP address. A CHG my only be engaged in crawling on page at a time, and the
politeness settings determines how long time must elapse until the same CHG can be used for crawling
another page.

A CHG has an ID, but in the Redis data structure this is combined with the Politeness Config ID.

> **NOTE:** As soon as politeness config is moved to the crawl host group config, the ID will only consist of
          the CHG ID.

### Data structure

The CHG is represented i Redis by the following data structure
<pre>
CHG:<em>chg id</em> = {
  n = <em>nextFetchTime</em>
  c = <em>uri count</em>
}
</pre>

Given the following:

* _uri1_ has earliestFetchTimeStamp in the future _t<sub>1</sub>_ and a CHG ID _chg1_
* _uri2_ has earliestFetchTimeStamp in the past _t<sub>2</sub>_ and a CHG ID _chg1_
* _uri3_ has earliestFetchTimeStamp in the future _t<sub>3</sub>_ and a CHG ID _chg2_
* _uri4_ has earliestFetchTimeStamp in the past _t<sub>4</sub>_ and a CHG ID _chg3_
* _uri5_ has earliestFetchTimeStamp in the future _t<sub>5</sub>_ where _t<sub>5</sub>_ > _t<sub>3</sub>_ and a CHG ID _chg4_

On insertion the _uri2_ and _uri4_'s earliestFetchTimeStamp is set to the current time _t<sub>now</sub>_

The initial datastructure then becomes:
<pre>
CHG:chg1 = {n = <em>t<sub>now</sub></em>, c = 2]
CHG:chg2 = {n = <em>t<sub>3</sub></em>, c = 1]
CHG:chg3 = {n = <em>t<sub>now</sub></em>, c = 1]
CHG:chg4 = {n = <em>t<sub>5</sub></em>, c = 1]
</pre>

Then there is three Redis keys representing the state of the CHG:
  1. The _ready queue_ which is a Redis [list](https://redis.io/commands#list) containing all CHG IDs
     which are ready to be engaged in crawling.
  2. The _wait queue_ which is a Redis [sorted set](https://redis.io/commands#sorted_set) with key _chg_wait_,
     containing all CHG IDs which are not engaged in crawling and must wait some time to do so due to
     politeness settings. The CHG IDs score is the _earliestFetchTimeStamp_ of the corresponding CHG.
  3. The _busy queue_ which is a Redis [sorted set](https://redis.io/commands#sorted_set) with key _chg_busy_,
     containing all CHG IDs which are currently engaged in crawling. The CHG IDs score is a _timeout_ which
     ensures that busy CHGs will eventually be put back in the _ready queue_ in case of a Frontier crash while crawling.

Given the above data structure and let _uri2_ be active in crawling, the three queues will be like this:
<pre>
chg_wait = [<em>t<sub>3</sub></em> <em>chg2</em>, <em>t<sub>5</sub></em> <em>chg4</em>]
chg_busy = [<em>timeout<sub>1</sub></em> <em>chg1</em>]
chg_ready = [<em>chg3</em>]
</pre>

### Operations

#### Adding a uri
Adding a uri to the queue involves several steps. This section covers only the CHG related step. 

1. First we set the _readyTime_ for the uris CHG by calling [HSETNX](https://redis.io/commands/hset)
   with "CHG:*chg_id*" as key and setting the field "_n_" to _nextFetchTimeStamp_. The result is used to determine if
   the CHG needs to be added to the _wait queue_. If the return value is `1`, then the CHG had no previous uris in
   queue and should be added to the _wait queue_. The _readyTime_ is only set if it wasn't already set.
2. Then the "_c_" field is incremented by one with [HINCRBY](https://redis.io/commands/hincrby). This is the number of
   uris in the queue.
3. For statistics a couple of counters are incremented to keep track of the number of uris in queue.
4. If result from 1. was `> 0` then [ZADD](https://redis.io/commands/zadd) with key "chg_wait" and score _readyTime_
   and value *chg_id*.

The _lua_ script looks like this:

```
---
--- KEYS[1]: chgKey
--- KEYS[2]: waitKey
--- KEYS[3]: crawlExecutionIdCountKey
--- KEYS[4]: queueCountTotalKey
--- ARGV[1]: nextFetchTimeFieldName
--- ARGV[2]: nextReadyTime
--- ARGV[3]: uriCountFieldName
--- ARGV[4]: crawlExecutionId
---

local changes = redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])

--- Increment chg queue count
redis.call('HINCRBY', KEYS[1], ARGV[3], 1)

--- Increment crawl execution queue count
redis.call('HINCRBY', KEYS[3], ARGV[4], 1)

--- Increment total queue count
redis.call('INCR', KEYS[4])

--- If new chg was created, queue it.
if changes > 0 then
    redis.call('ZADD', KEYS[2], ARGV[2], ARGV[4])
end
```

#### Fetching next ready
>*Work in progress*
```
List<String> res = jedis.brpop(5, CHG_READY_KEY);
if (res == null) {
  return null;
}
String chgp = res.get(1);
String chgpKey = CHG_PREFIX + chgp;
List<byte[]> keys = ImmutableList.of(CHG_BUSY_KEY.getBytes(), chgpKey.getBytes());
List<byte[]> args = ImmutableList.of(chgp.getBytes(), String.valueOf(System.currentTimeMillis() + busyTimeout).getBytes());
List<byte[]> result = (List<byte[]>) chgNextScript.runBytes(jedis, keys, args);
return deserializeCrawlHostGroup(chgp, result);
```

```
---
--- KEYS[1]: busyKey
--- KEYS[2]: chgpKey
--- ARGV[1]: chgp
--- ARGV[2]: busyExpireTime
---

redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return redis.call('HGETALL', KEYS[2])
```

#### Release CHG
>*Work in progress*
```
CrawlHostGroup chg = crawlHostGroup.toBuilder().setBusy(false).build();
long readyTime = Timestamps.toMillis(chg.getNextFetchTime());
long sysTime = System.currentTimeMillis();
if (readyTime <= sysTime) {
  readyTime = 0;
}

String chgp = CrawlQueueManager.createChgPolitenessKey(chg);
String chgpKey = CrawlQueueManager.CHG_PREFIX + chgp;

if (!jedis.exists(chgpKey)) {
  return null;
}
jedis.zrem(CHG_BUSY_KEY, chgp);
jedis.hset(chgpKey.getBytes(), CrawlQueueManager.serializeCrawlHostGroup(chg));
if (readyTime > System.currentTimeMillis()) {
  jedis.zadd(CHG_WAIT_KEY, readyTime, chgp);
} else {
  jedis.rpush(CHG_READY_KEY, chgp);
}
return null;
```

#### Crawl Host Group queue worker
Since Redis doesn't have a delayed queue, there is a background job checking for CHGs which have a score less or equal
to the current time stamp in the _wait queue_ and _busy queue_ and moving such items to the _ready queue_.

The background job runs at regular intervals. For each run the following _lua_ script is executed once for the 
_wait queue_ and once for the _busy queue_.

Input parameters to the script are:
  * KEYS[1] = "chg_wait" for the _wait queue_ and "chg_busy" for the _busy queue_
  * KEYS[2] = "chg_ready"
  * ARGV[1] = _current unix time_

```
local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
for _, key in ipairs(res) do
    redis.call('ZREM', KEYS[1], key)
    redis.call('RPUSH', KEYS[2], key)
end
```

First [ZRANGEBYSCORE](https://redis.io/commands/zrangebyscore) gets all CHGs with `score < current unix time`. Then
those CHGs are removed from the queue with [ZREM](https://redis.io/commands/zrem) and added to the ready queue with
[RPUSH](https://redis.io/commands/rpush) such that the newest
addition to the ready queue is added at the end.
 