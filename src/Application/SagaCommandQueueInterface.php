<?php

declare(strict_types=1);

namespace MicroModule\Saga\Application;

interface SagaCommandQueueInterface
{
    /**
     * @param class-string $commandClass
     * @param array        $payload Broadway Serializable::serialize() format
     */
    public function enqueue(string $commandClass, array $payload): void;
}
