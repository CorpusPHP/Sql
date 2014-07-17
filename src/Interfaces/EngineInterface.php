<?php

namespace Corpus\Sql\Interfaces;

interface EngineInterface {

	public function execute(\PDO $pdo, $query, array $map);

}
 