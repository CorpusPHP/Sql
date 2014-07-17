<?php

namespace Corpus\Sql\Engine;

use Corpus\Sql\Interfaces\EngineInterface;

class MySQL implements EngineInterface {

	protected $numberOfRetries = 5;

	/**
	 * @return int
	 */
	public function getNumberOfRetries() {
		return $this->numberOfRetries;
	}

	/**
	 * @param int $numberOfRetries
	 */
	public function setNumberOfRetries( $numberOfRetries ) {
		$this->numberOfRetries = $numberOfRetries;
	}

	/**
	 * @param \PDO   $pdo
	 * @param string $query
	 * @param array  $map
	 * @param int    $fetch
	 * @return bool|\IteratorIterator
	 * @throws \PDOException
	 */
	public function execute( \PDO $pdo, $query, array $map, $fetch = \PDO::FETCH_BOTH ) {

		$statement = $pdo->prepare($query);

		$statement->setFetchMode($fetch);

		$retry = $this->numberOfRetries;
		while( $retry-- ) {
			try {
				$success = $statement->execute($map);
			} catch(\Exception $e) {
				throw new \PDOException($e->getMessage(), 0, $e);
			}

			if( $success ) {
				if( $statement->columnCount() ) {
					// only queries that return a result set should have a column count
					return new \IteratorIterator($statement);
				}

				return true;
			}

			// deadlock
			if( $statement->errorCode() == "40001" ) {
				continue;
			}

			throw new \PDOException($statement);
		}

		throw new \PDOException("Query Failed after {$this->numberOfRetries} attempts:\n\n{$query}");

	}

}
