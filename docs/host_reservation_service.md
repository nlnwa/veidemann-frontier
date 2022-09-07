# Host reservation service

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
