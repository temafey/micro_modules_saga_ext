<?php

declare(strict_types=1);

namespace MicroModule\Saga\Testing;

use Broadway\Domain\DomainMessage;
use Broadway\Domain\Metadata;
use Broadway\Saga\MultipleSagaManager;
use PHPUnit\Framework\TestCase;

/**
 * Class Scenario.
 *
 * @category Tests\Unit
 */
class Scenario
{
    /**
     * @var TestCase
     */
    private $testCase;

    /**
     * @var MultipleSagaManager
     */
    private $sagaManager;

    /**
     * @var TraceableCommandBus
     */
    private $traceableCommandBus;

    /**
     * @var int
     */
    private $playhead;

    /**
     * Scenario constructor.
     *
     * @param TestCase            $testCase
     * @param MultipleSagaManager $sagaManager
     * @param TraceableCommandBus $traceableCommandBus
     */
    public function __construct(
        TestCase $testCase,
        MultipleSagaManager $sagaManager,
        TraceableCommandBus $traceableCommandBus
    ) {
        $this->testCase = $testCase;
        $this->sagaManager = $sagaManager;
        $this->traceableCommandBus = $traceableCommandBus;
        $this->playhead = -1;
    }

    /**
     * Set applied events in saga object.
     *
     * @param mixed[] $events
     *
     * @return Scenario
     */
    public function given(array $events = []): self
    {
        foreach ($events as $given) {
            $this->sagaManager->handle($this->createDomainMessageForEvent($given));
        }

        return $this;
    }

    /**
     * Apply(test) event in saga object.
     *
     * @param object $event
     *
     * @return Scenario
     */
    public function when($event): self
    {
        $this->traceableCommandBus->record();

        $this->sagaManager->handle($this->createDomainMessageForEvent($event));

        return $this;
    }

    /**
     * Compare, what command should be handled(returned) in saga object after apply event.
     *
     * @param mixed[] $commands
     *
     * @return Scenario
     */
    public function then(array $commands): self
    {
        $this->testCase->assertEquals($commands, $this->traceableCommandBus->getRecordedCommands());

        return $this;
    }

    /**
     * Create and return broadway message from event.
     *
     * @param object $event
     *
     * @return DomainMessage
     */
    private function createDomainMessageForEvent($event): DomainMessage
    {
        ++$this->playhead;

        return DomainMessage::recordNow(1, $this->playhead, new Metadata([]), $event);
    }
}
