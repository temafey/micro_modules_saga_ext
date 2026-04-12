# micro-module/saga

Micro-module saga extension library. Provides abstract saga base class, DBAL saga repository, and command dispatching infrastructure following the Dependency Inversion Principle.

## Installation

```bash
composer require micro-module/saga
```

## Dispatcher Interfaces

### SagaCommandDispatcherInterface

Abstraction for dispatching commands from within sagas. Two implementations are provided:

- `SyncSagaCommandDispatcher` — dispatches synchronously via Tactician `CommandBus`
- `OutboxSagaCommandDispatcher` — enqueues asynchronously via `SagaCommandQueueInterface`

### SagaCommandQueueInterface

Abstract queue contract. The saga package does NOT depend on `outbox-bundle` or any concrete queue implementation. The outbox package (or any other package) implements this interface and is wired via DI.

## Dependency Inversion Diagram

```
                    saga package
                 ┌───────────────────────────────────────────────┐
                 │                                               │
                 │  SagaCommandDispatcherInterface               │
                 │          ▲              ▲                     │
                 │          │              │                     │
                 │  OutboxSaga-    SyncSaga-                     │
                 │  CommandDispatcher   CommandDispatcher        │
                 │          │              │                     │
                 │  SagaCommandQueueInterface   CommandBus       │
                 │          │              │                     │
                 └──────────┼──────────────┼─────────────────────┘
                            │              │
                 ┌──────────┼──────────────┼─────────────────────┐
                 │          │              │  outbox-bundle /     │
                 │  OutboxAwareTaskProducer    tactician-bundle   │
                 │  implements SagaCommandQueueInterface          │
                 └───────────────────────────────────────────────┘
```

**Key rule:** The arrow from `OutboxAwareTaskProducer` points TO the saga package interface — the saga package has zero dependency on the outbox package. This is the Dependency Inversion Principle in action.

## Usage

### Sync dispatch (Tactician)

```php
// DI wiring
$dispatcher = new SyncSagaCommandDispatcher($commandBus);
$dispatcher->dispatch(new CreateNewsCommand($id));
```

### Async dispatch via outbox (DIP)

```php
// The queue implementation (OutboxAwareTaskProducer) lives in outbox-bundle
// and implements SagaCommandQueueInterface.
// Wire via DI alias — never hard-code the concrete class in your saga.

$dispatcher = new OutboxSagaCommandDispatcher($queue); // $queue is SagaCommandQueueInterface
$dispatcher->dispatch(new PublishNewsCommand($id));    // command must implement Serializable
```

## Requirements

- PHP 8.0+
- `league/tactician` ^1.0
- `micro-module/broadway` ^2.6 (provides `Broadway\Serializer\Serializable`)
