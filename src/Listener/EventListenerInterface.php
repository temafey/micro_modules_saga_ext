<?php

declare(strict_types=1);

namespace MicroModule\Saga\Listener;

use Broadway\Domain\DomainMessage;
use Broadway\Saga\State;

/**
 * Interface EventListenerInterface.
 *
 * @category Infrastructure\Utils
 */
interface EventListenerInterface
{
    /**
     * Saga pre handle event action.
     *
     * @param State         $state
     * @param DomainMessage $domainMessage
     */
    public function preHandleSaga(State $state, DomainMessage $domainMessage): void;

    /**
     * Saga post handle event action.
     *
     * @param State         $state
     * @param DomainMessage $domainMessage
     */
    public function postHandleSaga(State $state, DomainMessage $domainMessage): void;
}
