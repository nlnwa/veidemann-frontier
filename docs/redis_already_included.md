# Already Included In Queue
The already included datastructure is a Redis [SET](https://redis.io/commands#set) keyed by the string `AINC:`
followed by the JobExecutionId. The members of the set are hashes of normalized Uri's.

It is used to determine if a uri is already queued or harvested and need not to be added to the queue.

### Data structure
Given the following:

`jobExecutionId1` harvests `uri1` and `uri2`\
`jobExecutionId2` harvests `uri1` and `uri3`

The initial datastructure then becomes:
```
AINC:jobExecutionId1 = [uriHash1, uriHash2]
AINC:jobExecutionId2 = [uriHash1, uriHash3]
```

### Operations
#### Check if uri is not in queue
To atomically check if a uri is already included in queue for a JobExecution and add the uri if it's not,
try inserting it with [SADD](https://redis.io/commands/sadd) and examine the result.
If the return value is '0', the uri was already in the queue and was not added.
If the return value is '1' the uri was added, indicating that it was not in the already included queue.

Example:
```
redis> SADD AINC:jobExecutionId1 "uriHash1"
(integer) 0
redis> SADD AINC:jobExecutionId1 "uriHash3"
(integer) 1
redis> SADD AINC:jobExecutionId1 "uriHash3"
(integer) 0
```

#### Reset Already Included Queue for a job execution
Resetting the already included for a job execution is simply a matter of removing the corresponding
key with [DEL](https://redis.io/commands/del)

Example:
```
redis> DEL AINC:jobExecutionId1
(integer) 1
```
