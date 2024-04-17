<?php

declare(strict_types=1);

namespace MicroModule\Saga\Storage;

use Broadway\Saga\State;
use Broadway\Saga\State\Criteria;
use Broadway\Saga\State\RepositoryException;
use Broadway\Saga\State\RepositoryInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
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
     * @param string     $tableName
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
        $databasePlatform = $this->connection->getDatabasePlatform();

        switch (true) {
            case $databasePlatform instanceof PostgreSQLPlatform:
                $this->connectionType = self::CONNECTION_TYPE_POSTGRES;

                break;

            case $databasePlatform instanceof MySQLPlatform:
                $this->connectionType = self::CONNECTION_TYPE_MYSQL;

                break;

            default:
                throw new DBALException('Unsupported database type: '.get_class($databasePlatform));

                break;
        }
    }

    /**
     * Find saga by criteria.
     *
     * @param Criteria $criteria
     * @param mixed    $sagaId
     *
     * @return null|State
     */
    public function findOneBy(Criteria $criteria, $sagaId): ?State
    {
        $results = $this->getSagaStatesByCriteriaAndStatus($criteria, $sagaId);
        $count = count($results);
                                                                                                                                                                                                                                                         
        if (1 === $count) {
            $result = current($results);
            $result['status'] = $result['status'];
            $result['values'] = json_decode($result['values'], true);

            return State::deserialize($result);
        }

        if ($count > 1) {
            throw new RepositoryException('Multiple saga state instances found.');
        }

        return null;
    }

    /**
     * Find failed saga states.
     *
     * @param Criteria|null $criteria
     * @param string|null   $sagaId
     *
     * @return State[]
     */
    public function findFailed(?Criteria $criteria = null, ?string $sagaId = null): array
    {
        $results = $this->getSagaStatesByCriteriaAndStatus($criteria, $sagaId, State::SAGA_STATE_STATUS_FAILED);
        $failedStates = [];

        foreach ($results as $result) {
            $result['status'] = $result['status'];
            $result['values'] = json_decode($result['values'], true);
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
            'id' => $serializedState['id'],
            'saga_id' => $serializedState['saga_id'],
            'status' => $serializedState['status'],
            'values' => json_encode($serializedState['values']),
        ];
        $this->connection->beginTransaction();
        $isNewState = !$this->activeSagaExists($serializedState['saga_id'], $serializedState['id']);

        try {
            if ($isNewState) {
                $this->connection->insert($this->tableName, $saveState);
            } else {
                $this->connection->update($this->tableName, $saveState, ['id' => $serializedState['id']]);
            }
            $this->connection->commit();
        } catch (DBALException $exception) {
            $this->connection->rollBack();

            throw $exception;
        }
    }

    /**
     * Find and return sagas states by criteria and status.
     *
     * @param Criteria|null $criteria
     * @param string|null   $sagaId
     * @param int[]|int     $status
     *
     * @return mixed[]
     */
    protected function getSagaStatesByCriteriaAndStatus(?Criteria $criteria, ?string $sagaId, $status = State::SAGA_STATE_STATUS_IN_PROGRESS): array
    {
        $selects = ['id', 'saga_id', 'status', 'values'];
        $query = 'SELECT '.implode(', ', $selects).' FROM '.$this->tableName.' WHERE ';
        $params = [];
        $queryConditions = [];

        if (null !== $status) {
            if (is_array($status)) {
                $queryConditions[] = 'status IN('.implode(',', array_fill(0, count($status), '?')).')';
                $params += $status;
            } else {
                $queryConditions[] = 'status = ?';
                $params[] = $status;
            }
        }

        if (null !== $sagaId) {
            $queryConditions[] = 'saga_id = ?';
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

        return $this->connection->fetchAllAssociative($query, $params, $types);
    }

    /**
     * Add filtering by saga state criteria.
     *
     * @param string[] $queryConditions
     * @param string   $key
     */
    protected function applyJsonFiltering(array &$queryConditions, string $key): void
    {
        switch ($this->connectionType) {
            case self::CONNECTION_TYPE_MYSQL:
                $queryConditions[] = 'JSON_EXTRACT(values, \'$.'.$key.'\') = ?';

                break;

            case self::CONNECTION_TYPE_POSTGRES:
                $queryConditions[] = 'values ->>\''.$key.'\' = ?';

                break;
        }
    }

    /**
     * @param mixed[] $params
     *
     * @return mixed[]
     */
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
     * Check is saga still active.
     *
     * @param string $sagaId
     * @param string $id
     *
     * @return bool
     */
    protected function activeSagaExists(string $sagaId, string $id): bool
    {
        $query = 'SELECT 1 FROM '.$this->tableName.' WHERE saga_id = ? AND id = ? AND status IN (?,?)';
        $params = [$sagaId, $id, State::SAGA_STATE_STATUS_FAILED, State::SAGA_STATE_STATUS_IN_PROGRESS];
        $results = $this->connection->fetchAssociative($query, $params);

        if ($results) {
            return true;
        }

        return false;
    }

    /**
     * Check if table exists and return saga table schema.
     *
     * @param Schema $schema
     *
     * @return Table|null
     */
    public function configureSchema(Schema $schema): ?Table
    {
        if ($schema->hasTable($this->tableName)) {
            return null;
        }

        return $this->configureTable($schema);
    }

    /**
     * Configure table schema for save sagas states.
     *
     * @param Schema $schema
     *
     * @return Table
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
        $table->addColumn('id', $uuidColumnDefinition['type'], $uuidColumnDefinition['params']);
        $table->addColumn('saga_id', 'string', ['length' => 100]);
        $table->addColumn('status', 'integer', ['unsigned' => true]);
        $table->addColumn('values', 'json_array', ['jsonb' => true]);
        $table->addColumn('recorded_on', 'datetime', ['default' => 'CURRENT_TIMESTAMP']);
        $table->setPrimaryKey(['id']);
        $table->addIndex(['saga_id']);
        //$table->addIndex(['values']);

        return $table;
    }

    /**
     * Return TableName.
     *
     * @return string
     */
    public function getTableName(): string
    {
        return $this->tableName;
    }
}
