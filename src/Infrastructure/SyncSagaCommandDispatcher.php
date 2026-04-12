<?php

declare(strict_types=1);

namespace MicroModule\Saga\Infrastructure;

use League\Tactician\CommandBus;
use MicroModule\Saga\Application\SagaCommandDispatcherInterface;

final readonly class SyncSagaCommandDispatcher implements SagaCommandDispatcherInterface
{
    public function __construct(
        private CommandBus $commandBus,
    ) {
    }

    public function dispatch(object $command): void
    {
        $this->commandBus->handle($command);
    }
}
