<?php

namespace Corpus\Sql;

use Corpus\Sql\Exceptions\MappingInvalidValueException;
use Corpus\Sql\Interfaces\EngineInterface;

class PdoWrapper {

	public $num_retries = 5;

	/**
	 * @var \PDO
	 */
	protected $pdo;

	/**
	 * @var EngineInterface
	 */
	protected $engine;

	function __construct( \PDO $pdo, EngineInterface $engine ) {
		$this->pdo    = $pdo;
		$this->engine = $engine;
	}

	public function insert( $table, array $map, $multiple ) {
		list($columns, $tokens) = $this->paren_pairs($map, $multiple);
		$_SQL = sprintf("INSERT INTO %s %s VALUES %s;", $table, $columns, $tokens);

		return $_SQL;
	}

	public function fetchAssoc( $query, array $map = array() ) {
		$result = $this->execute($query, $map, \PDO::FETCH_ASSOC);

		return iterator_to_array($result) ? : array();
	}

	function execute( $query, array $map = array() ) {
		list($query, $map) = $this->processQueryMap($query, $map);

		return $this->engine->execute($this->pdo, $query, $map);
	}

	protected function processQueryMap( $query, array $map ) {
		// @todo maybe add some escape handling up here - look into pdo ? escaping.
		$splitQuery = preg_split('/(\?{1,2})/', $query, null, PREG_SPLIT_DELIM_CAPTURE);

		$value = reset($map);

		$rewrittenQuery = '';
		$rewrittenMap   = array();

		foreach( $splitQuery as $queryPart ) {

			if( $queryPart === '?' ) {
				if( is_scalar($value) || is_null($value) ) {
					$rewrittenQuery .= '?';
				} else {
					throw new MappingInvalidValueException('Invalid type: ' . gettype($value) . ' - expected null or scalar');
				}
			} elseif( $queryPart === '??' ) {
				$count = count($value);
				if( is_array($value) && $count > 0 ) {
					$rewrittenQuery .= rtrim(str_repeat("?,", $count), ",");
				} elseif( $count == 0 ) {
					throw new MappingInvalidValueException('Array escaping passed empty set.');
				} else {
					throw new MappingInvalidValueException('Invalid type: ' . gettype($value) . ' - expected array');
				}
			} else {
				$rewrittenQuery .= $queryPart;
				continue;
			}

			$rewrittenMap = array_merge($rewrittenMap, (array)$value);
			$value        = next($map);
		}

		return array( $rewrittenQuery, $rewrittenMap );
	}

	public function fetchRow( $query, array $map = array() ) {
		$result = $this->execute($query, $map);
		foreach( $result as $row ) {
			return $row;
		}

		return array();
	}

	public function fetchScalar( $query, array $map = array() ) {
		$result = $this->execute($query, $map);
		foreach( $result as $row ) {
			return $row[0];
		}

		return null;
	}

	public function fetchScalars( $query, array $map = array() ) {
		$result = $this->execute($query, $map);
		$final  = array();
		foreach( $result as $row ) {
			$final[] = $row[0];
		}

		return $final;
	}

	public function fetchKeypair( $query, array $map = array() ) {
		$result = $this->execute($query, $map);
		$final  = array();
		foreach( $result as $row ) {
			$final[$row[0]] = $row[1];
		}

		return $final ? : array();
	}

	public function fetchKeypairs( $query, array $map = array() ) {
		$result = $this->execute($query, $map);
		$final  = array();
		foreach( $result as $row ) {
			//prevents notices
			if( !isset($final[$row[0]]) ) {
				$final[$row[0]] = array();
			}
			$final[$row[0]][] = $row[1];
		}

		return $final;
	}

	public function fetchKeyrow( $query, array $map = array() ) {
		$result = $this->execute($query, $map);
		$final  = array();
		foreach( $result as $row ) {
			$final[$row[0]] = $row;
		}

		return $final ? : array();
	}

	public function fetchKeyrows( $query, array $map = array() ) {
		$result = $this->execute($query, $map);
		$final  = array();
		foreach( $result as $row ) {
			$final[$row[0]][] = $row;
		}

		return $final ? : array();
	}

}
