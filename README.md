# Global Ordering Demo

A simple demo that provides an example of how global ordering across partitions
can be implemented using a sliding window.

## Requirements

* Kafka running locally
* (Ideally) a topic named `messages` with multiple partitions

## Build

```
go get -u ./...
cd cmd/consumer && go build .
cd ../generator && go build .
```

## Run

The demo has 2 components, the generator (`producer`) and the consumer.

The generator will produce messages with timestamps that are up to 6 seconds in
the past.

The consumer can be run in two modes, the default mode will log messages as they
are received and will highlight when messages are received out of order. An "in order" 
mode can be used by setting the environment variable `IN_ORDER=true`.

### Run Message Generator

```
./generator
```

### Run Consumer

Without explicit order enforcement:
```
./consumer
```

With global ordering enforced:
```
IN_ORDER=true ./consumer
```

