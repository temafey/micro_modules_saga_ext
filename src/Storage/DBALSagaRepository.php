<?php

declare(strict_types=1);

namespace MicroModule\Saga\Storage;

use Broadway\Saga\State;
use Broadway\Saga\State\Criteria;
use Broadway\Saga\State\RepositoryException;
use Broadway\Saga\State\RepositoryInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Driver\AbstractMySQLDriver;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Exception;
use PDO;

/**
 * Class DBALSagaRepository.
 *
 * @category Infrastructure\Repository\Saga
 *
 * @SuppressWarnings(PHPMD)
 */
class DBALSagaRepository implements RepositoryInterface
{
    protected const FIELD_ID = 'id';
    protected const FIELD_SAGA_ID = 'saga_id';
    protected const FIELD_STATUS = 'status';
    protected const FIELD_VALUES = 'values';
    protected const FIELD_RECORDED_ON = 'recorded_on';

    protected const SUPPORTED_TYPES = [
        'NULL' => PDO::PARAM_NULL,
        'integer' => PDO::PARAM_INT,
        'string' => PDO::PARAM_STR,
        'boolean' => PDO::PARAM_BOOL,
    ];

    protected const CONNECTION_TYPE_POSTGRES = 'postgres';
    protected const CONNECTION_TYPE_MYSQL = 'mysql';

    /**
     * Database connection object.
     *
     * @var Connection
     */
    protected $connection;

    /**
     * Database connection type.
     *
     * @var string
     */
    protected $connectionType;

    /**
     * Saga state database table name.
     *
     * @var string
     */
    protected $tableName;

    /**
     * DBALSagaRepository constructor.
     *
     * @param Connection $connection
     * @param string $tableName
     *
     * @throws DBALException
     */
    public function __construct(Connection $connection, string $tableName)
    {
        $this->setConnection($connection);
        $this->tableName = $tableName;
    }

    /**
     * Set database connection and connection type.
     *
     * @param Connection $connection
     *
     * @throws DBALException
     */
    protected function setConnection(Connection $connection): void
    {
        $this->connection = $connection;
        $connectionDrive = $this->connection->getDriver();

        $this->connectionType = match (true) {
            $connectionDrive instanceof AbstractPostgreSQLDriver => self::CONNECTION_TYPE_POSTGRES,
            $connectionDrive instanceof AbstractMySQLDriver => self::CONNECTION_TYPE_MYSQL,
            default => throw new DBALException(sprintf('Unsupported database type: %s', get_class($connectionDrive)))
        };
    }

    /**
     * Find saga by criteria.
     *
     * @param Criteria $criteria
     * @param mixed $sagaId
     *
     * @return null|State
     */
    public function findOneBy(Criteria $criteria, $sagaId): ?State
    {
        $results = $this->getSagaStatesByCriteriaAndStatus($criteria, $sagaId);
        $count = count($results);
        if (1 === $count) {
            $result = current($results);
            $result[self::FIELD_VALUES] = json_decode($result[self::FIELD_VALUES], true);

            return State::deserialize($result);
        }
        if (1 < $count) {
            throw new RepositoryException('Multiple saga state instances found.');
        }

        return null;
    }

    /**
     * Find failed saga states.
     *
     * @param Criteria|null $criteria
     * @param string|null $sagaId
     *
     * @return State[]
     */
    public function findFailed(?Criteria $criteria = null, ?string $sagaId = null): array
    {
        $results = $this->getSagaStatesByCriteriaAndStatus($criteria, $sagaId, State::SAGA_STATE_STATUS_FAILED);
        $failedStates = [];

        foreach ($results as $result) {
            $result[self::FIELD_VALUES] = json_decode($result[self::FIELD_VALUES], true);
            $failedStates[] = State::deserialize($result);
        }

        return $failedStates;
    }

    /**
     * @param State $state
     *
     * @throws Exception
     */
    public function save(State $state): void
    {
        $serializedState = $state->serialize();
        $saveState = [
            self::FIELD_ID => $serializedState[self::FIELD_ID],
            self::FIELD_SAGA_ID => $serializedState[self::FIELD_SAGA_ID],
            self::FIELD_STATUS => $serializedState[self::FIELD_STATUS],
            self::FIELD_VALUES => json_encode($serializedState[self::FIELD_VALUES]),
        ];
        $this->connection->beginTransaction();
        $isNewState = !$this->activeSagaExists($serializedState[self::FIELD_SAGA_ID], $serializedState[self::FIELD_ID]);

        try {
            if ($isNewState) {
                $this->connection->insert($this->tableName, $saveState);
            } else {
                $this->connection->update(
                    $this->tableName,
                    $saveState,
                    [
                        self::FIELD_ID => $serializedState[self::FIELD_ID]
                    ]
                );
            }

            $this->quoteColumnNames(array_keys($saveState));
            $this->connection->commit();
        } catch (DBALException $exception) {
            $this->connection->rollBack();

            throw $exception;
        }
    }

    private function quoteColumnNames(array $columnNames): void
    {
        //Notice: For MySQL PDO `values` is a reserved name! It is needed to be quoted.
        if (self::CONNECTION_TYPE_MYSQL !== $this->connectionType) {
            return;
        }

        foreach ($columnNames as $columnName) {
            $this->connection->quoteSingleIdentifier($columnName);
        }
    }

    /**
     * Find and return sagas states by criteria and status.
     *
     * @param Criteria|null $criteria
     * @param string|null $sagaId
     * @param int[]|int $status
     *
     * @return mixed[]
     */
    protected function getSagaStatesByCriteriaAndStatus(
        ?Criteria $criteria,
        ?string $sagaId,
        $status = State::SAGA_STATE_STATUS_IN_PROGRESS
    ): array {
        $selects = [
            self::FIELD_ID,
            self::FIELD_SAGA_ID,
            self::FIELD_STATUS,
            self::FIELD_VALUES,
        ];
        $query = 'SELECT ' . implode(', ', $selects) . ' FROM ' . $this->tableName . ' WHERE ';
        $params = [];
        $queryConditions = [];
        if (null !== $status) {
            if (is_array($status)) {
                $queryConditions[] = self::FIELD_STATUS . ' IN(' . implode(',', array_fill(0, count($status), '?')) . ')';
                $params += $status;
            } else {
                $queryConditions[] = self::FIELD_STATUS . ' = ?';
                $params[] = $status;
            }
        }
        if (null !== $sagaId) {
            $queryConditions[] = self::FIELD_SAGA_ID . ' = ?';
            $params[] = $sagaId;
        }
        if (null !== $criteria) {
            $comparisons = $criteria->getComparisons();

            foreach ($comparisons as $key => $value) {
                $params[] = $value;
                $this->applyJsonFiltering($queryConditions, $key);
            }
        }

        $types = $this->getParamTypes($params);
        $query .= implode(' AND ', $queryConditions);

        return $this->connection->fetchAll($query, $params, $types);
    }

    /**
     * Add filtering by saga state criteria
     */
    protected function applyJsonFiltering(array &$queryConditions, string $key): void
    {
        switch ($this->connectionType) {
            case self::CONNECTION_TYPE_MYSQL:
                $queryConditions[] = 'JSON_EXTRACT(values, \'$.' . $key . '\') = ?';

                break;

            case self::CONNECTION_TYPE_POSTGRES:
                $queryConditions[] = 'values ->>\'' . $key . '\' = ?';

                break;
        }
    }

    protected function getParamTypes(array $params): array
    {
        $supportedTypes = self::SUPPORTED_TYPES;

        return array_map(
            static function ($param) use ($supportedTypes): int {
                return $supportedTypes[gettype($param)] ?? PDO::PARAM_STR;
            },
            $params
        );
    }

    /**
     * Check is saga still active
     */
    protected function activeSagaExists(string $sagaId, string $id): bool
    {
        $query = "SELECT 1 FROM $this->tableName WHERE saga_id = ? AND id = ? AND status IN (?,?)";
        $params = [$sagaId, $id, State::SAGA_STATE_STATUS_FAILED, State::SAGA_STATE_STATUS_IN_PROGRESS];
        $results = $this->connection->fetchAll($query, $params);

        if ($results) {
            return true;
        }

        return false;
    }

    /**
     * Check if table exists and return saga table schema
     */
    public function configureSchema(Schema $schema): ?Table
    {
        if ($schema->hasTable($this->tableName)) {
            return null;
        }

        return $this->configureTable($schema);
    }

    /**
     * Configure table schema for save sagas states
     */
    public function configureTable(Schema $schema): Table
    {
        $uuidColumnDefinition = [
            'type' => 'guid',
            'params' => [
                'length' => 36,
            ],
        ];
        $table = $schema->createTable($this->tableName);
        $table->addColumn(self::FIELD_ID, $uuidColumnDefinition['type'], $uuidColumnDefinition['params']);
        $table->addColumn(self::FIELD_SAGA_ID, 'string', ['length' => 32]);
        $table->addColumn(self::FIELD_STATUS, 'integer', ['unsigned' => true]);
        $table->addColumn(self::FIELD_VALUES, 'json_array', ['jsonb' => true]);
        $table->addColumn(self::FIELD_RECORDED_ON, 'datetime', ['default' => 'CURRENT_TIMESTAMP']);
        $table->setPrimaryKey([self::FIELD_ID]);
        $table->addIndex([self::FIELD_SAGA_ID]);

        return $table;
    }

    /**
     * Return TableName
     */
    public function getTableName(): string
    {
        return $this->tableName;
    }
}
