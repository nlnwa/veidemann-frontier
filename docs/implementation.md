# Implementation

## TiKV

### Clients

1. https://tikv.org/docs/6.1/develop/clients/introduction/

### Constraints

- Max key size: 4kB
- Max value size: 8MB

UTF-8 uses 1-4 bytes per character.

1. https://github.com/tikv/tikv/issues/7272

### Transactions

#### RawKV API
The RawKV API support compare-and-swap as an atomic operation.

```javascript
    // swap value of key with newValue value if and only if
	// oldValue is equal to current value
    client.compareAndSwap(key, oldValue, newValue)
```

1. https://tikv.org/docs/6.1/develop/rawkv/cas/
2. https://tikv.org/docs/5.1/concepts/explore-tikv-features/cas/

#### Transactional API
The transactional API support transactions.

