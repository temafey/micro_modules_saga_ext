<?php

declare(strict_types=1);

namespace MicroModule\Saga\Testing;

use League\Tactician\CommandBus;
use League\Tactician\Middleware;
use Mockery;

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
    private $commands = [];

    /**
     * Start set commands to command store.
     *
     * @var bool
     */
    private $record = false;

    /**
     * TraceableCommandBus constructor.
     *
     * @psalm-suppress PossiblyUndefinedMethod
     * @psalm-suppress InvalidArgument
     */
    public function __construct()
    {
        $middleware = Mockery::mock(Middleware::class);
        $middleware
            ->shouldReceive('execute')
            ->andReturnUsing(
                static function ($command, $next) {
                    return $next($command);
                }
            );

        parent::__construct([$middleware]);
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
