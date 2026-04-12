<?php

declare(strict_types=1);

namespace MicroModule\Saga\Infrastructure;

use Broadway\Serializer\Serializable;
use MicroModule\Saga\Application\SagaCommandDispatcherInterface;
use MicroModule\Saga\Application\SagaCommandQueueInterface;

final readonly class OutboxSagaCommandDispatcher implements SagaCommandDispatcherInterface
{
    public function __construct(
        private SagaCommandQueueInterface $queue,
    ) {
    }

    public function dispatch(object $command): void
    {
        if (!$command instanceof Serializable) {
            throw new \InvalidArgumentException(sprintf(
                'Command %s must implement Broadway\\Serializer\\Serializable to be dispatched via async queue.',
                $command::class
            ));
        }
        $this->queue->enqueue($command::class, $command->serialize());
    }
}
