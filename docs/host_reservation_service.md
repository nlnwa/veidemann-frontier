# Host reservation service

### Host table
- **Key:** Busy TS HOST
- **Value:** NIL

### External service
``` go
func ReserveNextHost() HOST
    GET first HOST from Hosttable WHERE Busy == false
    SET Busy = true
    SET TS to current time + timeout
    return HOST
    
func ReleaseHost(HOST)
    GET first URL from URL queue for given HOST
    SET HOSTs TS to URLs TS
    SET Busy = false
```

### Internal service
Background service to ensure that no Host stays in busy state forever
``` go
REPEAT EVERY x seconds:
    GET first HOST from Hosttable WHERE Busy == true AND TS > Current time
    SET Busy = false
    SET TS = Current time
```