<?php

declare(strict_types=1);

namespace MicroModule\Saga;

use Broadway\Domain\DomainMessage;
use Broadway\Saga\Saga;
use Broadway\Saga\State;

/**
 * Class AbstractSaga.
 *
 * @category MicroModule\Saga
 */
abstract class AbstractSaga extends Saga
{
    /**
     * Saga last state.
     *
     * @var State|null
     */
    protected $lastState;

    /**
     * Handle saga event and return saga state.
     *
     * @param State         $state
     * @param DomainMessage $domainMessage
     *
     * @return State
     */
    public function handle(State $state, DomainMessage $domainMessage): State
    {
        $this->lastState = $state;

        return parent::handle($state, $domainMessage);
    }

    /**
     * Return saga last state.
     *
     * @return State|null
     */
    public function getLastState(): ?State
    {
        return $this->lastState;
    }
}
