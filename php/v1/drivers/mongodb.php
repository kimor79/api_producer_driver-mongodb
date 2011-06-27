<?php

/**

Copyright (c) 2011, Kimo Rosenbaum and contributors
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the owner nor the names of its contributors
      may be used to endorse or promote products derived from this
      software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**/

/**
 * ApiProducerDriverMongoDB
 * @author Kimo Rosenbaum <kimor79@yahoo.com>
 * @version $Id$
 * @package ApiProducerDriverMongoDB
 */

class ApiProducerDriverMongoDB {

	protected $config = array();
	protected $count = 0;
	protected $error = '';
	protected $mongo;
	protected $slave_okay = false;

	public function __construct($slave_okay = false, $config = array()) {
		$this->config = $config;
		$this->slave_okay = $slave_okay;

		$host = "mongodb://";
		$options = array();

		if(array_key_exists('user', $config)) {
			$host .= $this->getConfig('user');

			if(array_key_exists('password', $config)) {
				$host .= ':' . $this->getConfig('password');
			}

			$host .= '@';
		}

		$host .= $this->getConfig('host',
			ini_get('mongo.default_host'));

		$database = $this->getConfig('database', false);
		if($database) {
			$host .= '/' . $database;
		}

		if(array_key_exists('connect', $config)) {
			$options['connect'] = $this->getConfig('connect');
		}

		if(array_key_exists('replicaSet', $config)) {
			$options['replicaSet'] = $this->getConfig('replicaSet');
		}

		if(array_key_exists('username', $config)) {
			$options['username'] = $this->getConfig('username');
		}

		if(array_key_exists('password', $config)) {
			$options['password'] = $this->getConfig('password');
		}

		if(array_key_exists('persist', $config)) {
			$options['persist'] = $this->getConfig('persist');
		}

		if(array_key_exists('timeout', $config)) {
			$options['timeout'] = $this->getConfig('timeout');
		}

		try {
			$this->mongo = new Mongo($host, $options);
		} catch (Exception $e) {
			throw new Exception($e->getMessage());
		}
	}

	public function __deconstruct() {
	}

	/**
	 * Convert values from MongoId objects
	 * @param mixed $input
	 * @return mixed
	 */
	public function convertFromId($input) {
		$output = $input;

		if(!is_array($input)) {
			return $output . '';
		}

		while(list($key, $val) = each($input)) {
			if(substr($key, -3) !== '_id') {
				continue;
			}

			if($key === 'id') {
				unset($output[$key]);
				$key = '_id';
			}

			$output[$key] = $this->convertFromId($val);
		}

		return $output;
	}

	/**
	 * Convert values to MongoId objects
	 * @param mixed $input
	 * @return mixed
	 */
	public function convertToId($input) {
		$output = $input;

		if(!is_array($input)) {
			try {
				$output = new MongoId($input);
			} catch(Exception $e) {
				$this->error = $e->getMessage();
				return false;
			}

			return $output;
		}

		while(list($key, $val) = each($input)) {
			if(substr($key, -3) !== '_id') {
				continue;
			}

			if($key === 'id') {
				unset($output[$key]);
				$key = '_id';
			}

			$output[$key] = $this->convertFromId($val);

			if($output[$key] === false) {
				return false;
			}
		}

		return $output;
	}

	/**
	 * Return the total number of records from a query
	 * @return int
	 */
	public function count() {
		return (int) $this->count;
	}

	/**
	 * Return error (if any) from most recent query
	 * @return string
	 */
	public function error() {
		return $this->error;
	}

	/**
	 * Get a config value
	 * @param string $key
	 * @param string $default
	 * @return string
	 */
	protected function getConfig($key = '', $default = '') {
		if(array_key_exists($key, $this->config)) {
			return $this->config[$key];
		}

		return $default;
	}

	/**
	 * Build and run a find()
	 * @param string $collection
	 * @param array $input fields/values to search 
	 * @param array $options
	 * @return mixed array with details (which may be empty) or false
	 */
	public function find($collection, $input, $options) {
		$this->count = '';
		$this->error = '';
		$col = '';
		$cursor = NULL;
		$output = array();
		$query = array();

		while(list($key, $values) = each($input)) {
			$convert_id = false;

			if($options['_convert_id']) {
				if($key === 'id') {
					$convert_id = true;
					$key = '_id';
				} elseif(substr($key, -3) === '_id') {
					$convert_id = true;
				}
			}

			$query[$key] = array(
				'$in' => array(),
			);

			if(!is_array($values)) {
				try {
					$query[$key]['$in'][] = ($convert_id) ?
						new MongoId($values) : $values;
				} catch (Exception $e) {
					$this->error = $e->getMessage();
					return false;
				}

				continue;
			}

			if(array_key_exists('eq', $values)) {
				while(list($junk, $value) =
						each($values['eq'])) {
					try {
						$query[$key]['$in'][] =
							($convert_id) ?
							new MongoId($value) :
							$value;
					} catch (Exception $e) {
						$this->error = $e->getMessage();
						return false;
					}
				}
			}

			if(array_key_exists('re', $values)) {
				while(list($junk, $value) =
						each($values['re'])) {
					try {
						$query[$key]['$in'][] =
							new MongoRegex($value);
					} catch (Exception $e) {
						$this->error = $e->getMessage();
						return false;
					}
				}
			}

			if(empty($query[$key]['$in'])) {
				unset($query[$key]['$in']);
			}

			if(empty($query[$key])) {
				unset($query[$key]);
			}
		}

		try {
			$col = $this->db->selectCollection($collection);

			if(!empty($query)) {
				$cursor = $col->find($query);
			} else {
				$cursor = $col->find();
			}

			if(!array_key_exists('outputFields', $options)) {
				$options['outputFields'] = array();
			}

			if(!$options['_convert_id']) {
				if(!array_key_exists('_id',
						$options['outputFields'])) {
					$options['outputFields']['_id'] = false;
				}
			}

			$cursor->fields($options['outputFields']);

			if($options['numResults']) {
				$cursor->limit($options['numResults']);
			}

			if(array_key_exists('sortField', $options)) {
				$cursor->sort(array($options['sortField'] =>
					($options['sortDir'] == 'asc') ? 1 : 0,
				));
			}

			if(array_key_exists('startIndex', $options)) {
				$cursor->skip($options['startIndex']);
			}

			while($cursor->hasNext()) {
				$data = $cursor->getNext();

				if($options['_convert_id']) {
					$data['id'] = $data['_id'] . '';
					unset($data['_id']);

					while(list($key, $val) = each($data)) {
						if(substr($key, -3) === '_id') {
							$data[$key] = $val . '';
						}
					}
				}

				$output[] = $data;
			}

			$this->count = $cursor->count();

			return $output;
		} catch (Exception $e) {
			$this->error = $e->getMessage();
		}

		return false;
	}

	/**
	 * Build and run a find(One)
	 * @param string $collection
	 * @param array $input fields/values to search 
	 * @param array $options
	 * @return mixed array with details (which may be empty) or false
	 */
	public function findOne($collection, $input, $options = array()) {
		$this->count = '';
		$this->error = '';
		$col = '';
		$query = array();
		$result = array();

		while(list($key, $values) = each($input)) {
			$convert_id = false;

			if($options['_convert_id']) {
				if($key === 'id') {
					$convert_id = true;
					$key = '_id';
				} elseif(substr($key, -3) === '_id') {
					$convert_id = true;
				}
			}

			$query[$key] = array(
				'$in' => array(),
			);

			if(!is_array($values)) {
				try {
					$query[$key]['$in'][] = ($convert_id) ?
						new MongoId($values) : $values;
				} catch (Exception $e) {
					$this->error = $e->getMessage();
					return false;
				}

				continue;
			}

			if(array_key_exists('eq', $values)) {
				while(list($junk, $value) =
						each($values['eq'])) {
					try {
						$query[$key]['$in'][] =
							($convert_id) ?
							new MongoId($value) :
							$value;
					} catch (Exception $e) {
						$this->error = $e->getMessage();
						return false;
					}
				}
			}

			if(array_key_exists('re', $values)) {
				while(list($junk, $value) =
						each($values['re'])) {
					try {
						$query[$key]['$in'][] =
							new MongoRegex($value);
					} catch (Exception $e) {
						$this->error = $e->getMessage();
					}

					return false;
				}
			}

			if(empty($query[$key]['$in'])) {
				unset($query[$key]['$in']);
			}

			if(empty($query[$key])) {
				unset($query[$key]);
			}
		}

		try {
			$col = $this->db->selectCollection($collection);

			if(!array_key_exists('outputFields', $options)) {
				$options['outputFields'] = array();
			}

			if(!$options['_convert_id']) {
				if(!array_key_exists('_id',
						$options['outputFields'])) {
					$options['outputFields']['_id'] = false;
				}
			}

			$result = $col->findOne($query,
				$options['outputFields']);

			if(is_null($result)) {
				$this->count = 0;
				return array();
			}

			if($options['_convert_id']) {
				$result['id'] = $result['_id'] . '';
				unset($result['_id']);

				while(list($key, $val) = each($result)) {
					if(substr($key, -3) === '_id') {
						$result[$key] =
							$this->convertFromId(
								$val);
					}
				}
			}

			$this->count = 1;
			return $result;
		} catch (Exception $e) {
			$this->error = $e->getMessage();
		}

		return false;
	}

	/**
	 * Build and run insert()
	 * @param string $collection
	 * @param array $input field/values to add
	 * @param array $options
	 * @return mixed modified $input or false
	 */
	public function insert($collection, $input, $options = array()) {
		$this->error = '';
		$col = '';
		$output = array();

		if($options['_convert_id']) {
			$input = $this->convertToid($input);

			if($input === false) {
				return false;
			}
		}

		try {
			$col = $this->db->selectCollection($collection);

			$col->insert($input, array('safe' => true));

			if(array_key_exists('_id', $input)) {
				$output = $input;

				if($options['_convert_id']) {
					$output = $this->convertFromId($output);

					if($output === false) {
						return false;
					}
				}

				return $output;
			}

			$this->error = '_id was not created';
		} catch (Exception $e) {
			$this->error = $e->getMessage();
		}

		return false;
	}
}

?>
