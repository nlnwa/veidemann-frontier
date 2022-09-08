# URL queue service

### URL queue table

- **Key:** HOST TS URL
- **Val:** LastFetch ErrorCount NotFoundCount


### Cookie jar

- **Key:** HOST
- **Val:** Cookies

## External service
``` go
func GetNextUrlForHost(HOST) URL, ERROR
    GET first URL from URL queue table WHERE HOST == given HOST
    IF no URL found THEN
        return ERROR
    ELSE
        SET TS to timeout
        return URL
    
func Update(URL)
    IF blacklist CONTAINS URL THEN return
    IF queue CONTAINS URL THEN
        SET TS to the the lowest value of the existing or given URL
    ELSE
        add new URL to table
    
func Delete(URL)
    TODO
    
func Blacklist(URL)
    TODO
```

## Internal service
Background service to ensure that no URL stays in harvesting state forever
``` go
REPEAT EVERY x seconds:
    TODO
    SET TS = Current time
```
