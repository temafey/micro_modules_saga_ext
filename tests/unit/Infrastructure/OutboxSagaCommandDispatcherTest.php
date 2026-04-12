<?php

declare(strict_types=1);

namespace MicroModule\Saga\Tests\Unit\Infrastructure;

use Broadway\Serializer\Serializable;
use MicroModule\Saga\Application\SagaCommandQueueInterface;
use MicroModule\Saga\Infrastructure\OutboxSagaCommandDispatcher;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

#[CoversClass(OutboxSagaCommandDispatcher::class)]
final class OutboxSagaCommandDispatcherTest extends TestCase
{
    private SagaCommandQueueInterface&MockObject $queue;
    private OutboxSagaCommandDispatcher $dispatcher;

    protected function setUp(): void
    {
        $this->queue = $this->createMock(SagaCommandQueueInterface::class);
        $this->dispatcher = new OutboxSagaCommandDispatcher($this->queue);
    }

    public function testDispatchSerializableCommandEnqueuesWithClassNameAndPayload(): void
    {
        $payload = ['id' => 'abc-123', 'title' => 'Test'];
        $command = new class ($payload) implements Serializable {
            public function __construct(private array $data) {}

            public function serialize(): array
            {
                return $this->data;
            }

            public static function deserialize(array $data): static
            {
                return new static($data);
            }
        };

        $this->queue
            ->expects(self::once())
            ->method('enqueue')
            ->with($command::class, $payload);

        $this->dispatcher->dispatch($command);
    }

    public function testDispatchNonSerializableCommandThrowsInvalidArgumentException(): void
    {
        $nonSerializableCommand = new class {
            public string $name = 'non-serializable';
        };

        $this->queue
            ->expects(self::never())
            ->method('enqueue');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/must implement Broadway\\\\Serializer\\\\Serializable/');

        $this->dispatcher->dispatch($nonSerializableCommand);
    }

    public function testDispatchUsesInjectedQueueInterfaceNotConcreteOutboxImplementation(): void
    {
        // This test verifies DIP: the dispatcher depends on SagaCommandQueueInterface,
        // not on OutboxAwareTaskProducer or any other concrete class.
        $reflection = new \ReflectionClass(OutboxSagaCommandDispatcher::class);
        $constructor = $reflection->getConstructor();
        self::assertNotNull($constructor);

        $parameters = $constructor->getParameters();
        self::assertCount(1, $parameters);
        self::assertSame('queue', $parameters[0]->getName());

        $type = $parameters[0]->getType();
        self::assertInstanceOf(\ReflectionNamedType::class, $type);
        self::assertSame(SagaCommandQueueInterface::class, $type->getName());
    }
}
