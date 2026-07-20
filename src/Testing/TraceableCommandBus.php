<?php

declare(strict_types=1);

namespace MicroModule\Saga\Testing;

use League\Tactician\CommandBus;

/**
 * Command bus that is able to record all dispatched commands.
 *
 * @category Tests\Unit
 */
final class TraceableCommandBus extends CommandBus
{
    /**
     * Command store.
     *
     * @var object[]
     */
    private array $commands = [];

    /**
     * Start set commands to command store.
     */
    private bool $record = false;

    public function __construct()
    {
        // handle() is fully overridden below and never invokes the parent
        // middleware chain, so an empty middleware list is all the parent
        // constructor needs. This avoids pulling Mockery (a require-dev
        // package) into a class that ships in src/.
        parent::__construct([]);
    }

    /**
     * {@inheritdoc}
     */
    public function handle($command): void
    {
        if (!$this->record) {
            return;
        }

        $this->commands[] = $command;
    }

    /**
     * Return array of command objects, that should be traced.
     *
     * @return object[]
     */
    public function getRecordedCommands(): array
    {
        return $this->commands;
    }

    /**
     * Start.
     *
     * @return bool
     */
    public function record(): bool
    {
        return $this->record = true;
    }
}
