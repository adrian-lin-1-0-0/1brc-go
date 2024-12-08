# 1brc-go

I implemented a lock-free version of a map structure and operated on it using multiple goroutines.
In the flame graph, I noticed that atomic operations consumed more time than I expected.

I recall hearing this statement at CppCon 2017:
> Atomic operations do not guarantee good performance.

I need some time to revisit and understand this concept.






## Quick Start

Build go binary
```sh
make build
```

Run go binary
```sh
make run TARGET=basic
```

## TODO
- [x] Concurrency
- [ ] mmap
- [ ] Other data structures
