<?php

declare(strict_types=1);

namespace MicroModule\Saga\Application;

interface SagaCommandDispatcherInterface
{
    public function dispatch(object $command): void;
}
