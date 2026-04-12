# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — VP-7: SagaCommandQueueInterface + Dispatcher impls (DIP)

- `src/Application/SagaCommandQueueInterface` — abstract queue contract; saga package does NOT require `micro-module/outbox-bundle`. The outbox package implements this interface (Dependency Inversion Principle).
- `src/Application/SagaCommandDispatcherInterface` — single `dispatch(object $command): void` abstraction for command dispatching from sagas.
- `src/Infrastructure/OutboxSagaCommandDispatcher` — async dispatcher; depends only on `SagaCommandQueueInterface`, never on `OutboxAwareTaskProducer`. Commands must implement `Broadway\Serializer\Serializable`.
- `src/Infrastructure/SyncSagaCommandDispatcher` — synchronous dispatcher via Tactician `CommandBus`.
- Unit tests for both dispatchers; mock uses `SagaCommandQueueInterface` (not a concrete class) proving DIP.
- `README.md` with dependency inversion architecture diagram.
- `autoload-dev` PSR-4 mapping for test namespace `MicroModule\Saga\Tests\\`.
