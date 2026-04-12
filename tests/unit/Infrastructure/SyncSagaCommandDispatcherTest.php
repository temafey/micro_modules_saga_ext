<?php

declare(strict_types=1);

namespace MicroModule\Saga\Tests\Unit\Infrastructure;

use League\Tactician\CommandBus;
use MicroModule\Saga\Infrastructure\SyncSagaCommandDispatcher;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

#[CoversClass(SyncSagaCommandDispatcher::class)]
final class SyncSagaCommandDispatcherTest extends TestCase
{
    private CommandBus&MockObject $commandBus;
    private SyncSagaCommandDispatcher $dispatcher;

    protected function setUp(): void
    {
        $this->commandBus = $this->createMock(CommandBus::class);
        $this->dispatcher = new SyncSagaCommandDispatcher($this->commandBus);
    }

    public function testDispatchForwardsCommandToCommandBus(): void
    {
        $command = new \stdClass();

        $this->commandBus
            ->expects(self::once())
            ->method('handle')
            ->with(self::identicalTo($command));

        $this->dispatcher->dispatch($command);
    }

    public function testDispatchWorksWithAnyObjectCommand(): void
    {
        $command = new class {
            public string $type = 'create-news';
        };

        $this->commandBus
            ->expects(self::once())
            ->method('handle')
            ->with(self::identicalTo($command));

        $this->dispatcher->dispatch($command);
    }
}
