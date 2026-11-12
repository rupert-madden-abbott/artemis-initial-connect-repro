# Artemis core client doesn't try the next broker when a connection is accepted then closed

Given a URL listing several brokers, such as `(tcp://a:61616,tcp://b:61616)`, the core client tries the next broker
only when the TCP connect fails. If an endpoint accepts the connection, then closes it or never replies,
`ServerLocator.createSessionFactory()` throws instead, though the next broker is up.

## Run

Needs Java 17+ and Maven.

```
mvn test
```

The test starts an embedded broker and connects with `(tcp://127.0.0.1:<unusable>,tcp://127.0.0.1:<live>)`, without
and with credentials, for three kinds of unusable endpoint. The default load-balancing policy starts on a random
connector, so each case repeats until the unusable endpoint has been tried first 5 times. `callTimeout` is 3 s, to keep
the run short.

## Expected

Every attempt connects to the live broker.

## Actual

Attempts that try the unusable endpoint first fail, except when it refuses the connection:

| Unusable endpoint     | Without credentials             | With credentials                |
|-----------------------|---------------------------------|---------------------------------|
| Refuses connection    | Connects to the live broker     | Connects to the live broker     |
| Accepts, then closes  | `AMQ219013` after `callTimeout` | `AMQ219016` at once             |
| Accepts, then silent  | `AMQ219013` after `callTimeout` | `AMQ219014` after `callTimeout` |

## Versions

Artemis 2.57.0 (broker and client), Java 17+.
