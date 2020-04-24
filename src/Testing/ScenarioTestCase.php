<?php

declare(strict_types=1);

namespace MicroModule\Saga\Testing;

use MicroModule\Saga\AbstractSaga;
use Broadway\EventDispatcher\CallableEventDispatcher;
use Broadway\Saga\Metadata\StaticallyConfiguredSagaMetadataFactory;
use Broadway\Saga\MultipleSagaManager;
use Broadway\Saga\State\InMemoryRepository;
use Broadway\Saga\State\StateManager;
use Broadway\UuidGenerator\Rfc4122\Version4Generator;
use League\Tactician\CommandBus;
use PHPUnit\Framework\TestCase;

/**
 * Class ScenarioTestCase.
 *
 * @category Tests\Unit
 */
abstract class ScenarioTestCase extends TestCase
{
    /**
     * Test saga scenario object.
     *
     * @var Scenario|null
     */
    protected $scenario;

    /**
     * Tested saga.
     *
     * @var AbstractSaga|null
     */
    protected $saga;

    /**
     * Create the saga you want to test in this test case.
     *
     * @param CommandBus $commandBus
     *
     * @return AbstractSaga
     */
    abstract protected function createSaga(CommandBus $commandBus): AbstractSaga;

    /**
     * This method is called before each test.
     */
    protected function setUp(): void
    {
        $this->scenario = $this->createScenario();
    }

    /**
     * Initialize and return saga test scenario object.
     *
     * @return Scenario
     */
    protected function createScenario(): Scenario
    {
        $traceableCommandBus = new TraceableCommandBus();
        $this->saga = $this->createSaga($traceableCommandBus);
        $sagaStateRepository = new InMemoryRepository();
        $sagaManager = new MultipleSagaManager(
            $sagaStateRepository,
            [$this->saga],
            new StateManager($sagaStateRepository, new Version4Generator()),
            new StaticallyConfiguredSagaMetadataFactory(),
            new CallableEventDispatcher()
        );

        return new Scenario($this, $sagaManager, $traceableCommandBus);
    }

    /**
     * This method is called after each test.
     */
    protected function tearDown(): void
    {
        $this->scenario = null;
        $this->saga = null;
    }
}
