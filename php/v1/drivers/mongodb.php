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
		$output = array();

		while(list($key, $val) = each($input)) {
			$convert = false;

			if($key === 'id') {
				$convert = true;
			} elseif(substr($key, -3) === '_id') {
				$convert = true;
			} elseif(is_array($val)) {
				$output[$key] = $this->convertFromId($val);
			} else {
				$output[$key] = $val;
			}

			if($convert) {
				$output[$key] = $this->_convertFromId($val);
			}
		}

		return $output;
	}

	/**
	 * Convert values to MongoId objects
	 * @param array $input
	 * @return mixed
	 */
	public function convertToId($input) {
		$output = array();

		while(list($key, $val) = each($input)) {
			$convert = false;

			if($key === 'id') {
				$convert = true;
			} elseif(substr($key, -3) === '_id') {
				$convert = true;
			} elseif(is_array($val)) {
				$output[$key] = $this->convertToId($val);
			} else {
				$output[$key] = $val;
			}

			if($convert) {
				$output[$key] = $this->_convertToId($val);
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
		$sub_details = true;

		if(array_key_exists('subDetails', $options)) {
			$sub_details = $options['subDetails'];
		}

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

			if($options['_replace_colon']) {
				$key = str_replace(':', '.', $key);
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

			if(array_key_exists('ge', $values)) {
				$query[$key]['$gte'] = $values['ge'];
			}

			if(array_key_exists('gt', $values)) {
				$query[$key]['$gt'] = $values['gt'];
			}

			if(array_key_exists('le', $values)) {
				$query[$key]['$lte'] = $values['le'];
			}

			if(array_key_exists('lt', $values)) {
				$query[$key]['$lt'] = $values['lt'];
			}

			if(array_key_exists('re', $values)) {
				while(list($junk, $value) =
						each($values['re'])) {
					if(substr($value, 0, 1) != '/') {
						$value = sprintf("/%s/i",
							$value);
					}

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

			if(is_null($options['outputFields']['_id'])) {
				unset($options['outputFields']['_id']);
			}

			$cursor->fields($options['outputFields']);

			if($options['numResults']) {
				$cursor->limit($options['numResults']);
			}

			if(array_key_exists('sortField', $options)) {
				$cursor->sort(array($options['sortField'] =>
					($options['sortDir'] == 'asc') ? 1 : -1,
				));
			}

			if(array_key_exists('startIndex', $options)) {
				$cursor->skip($options['startIndex']);
			}

			while($cursor->hasNext()) {
				$t_data = $cursor->getNext();
				$data = array();

				while(list($key, $val) = each($t_data)) {
					if(!$sub_details) {
						if(is_array($val)) {
							continue;
						}
					}

					$data[$key] = $val;
				}

				if($options['_convert_id']) {
					$data = $this->convertFromId($data);

					$data['id'] = $data['_id'];
					unset($data['_id']);
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

			if($options['_replace_colon']) {
				$key = str_replace(':', '.', $key);
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

			if(array_key_exists('ge', $values)) {
				$query[$key]['$gte'] = $values['ge'];
			}

			if(array_key_exists('gt', $values)) {
				$query[$key]['$gt'] = $values['gt'];
			}

			if(array_key_exists('le', $values)) {
				$query[$key]['$lte'] = $values['le'];
			}

			if(array_key_exists('lt', $values)) {
				$query[$key]['$lt'] = $values['lt'];
			}

			if(array_key_exists('re', $values)) {
				while(list($junk, $value) =
						each($values['re'])) {
					if(substr($value, 0, 1) != '/') {
						$value = sprintf("/%s/i",
							$value);
					}

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

			if(!array_key_exists('outputFields', $options)) {
				$options['outputFields'] = array();
			}

			if(!$options['_convert_id']) {
				if(!array_key_exists('_id',
						$options['outputFields'])) {
					$options['outputFields']['_id'] = false;
				}
			}

			if(is_null($options['outputFields']['_id'])) {
				unset($options['outputFields']['_id']);
			}

			$result = $col->findOne($query,
				$options['outputFields']);

			if(is_null($result)) {
				$this->count = 0;
				return array();
			}

			if($options['_convert_id']) {
				$result = $this->convertFromId($result);
				$result['id'] = $result['_id'];
				unset($result['_id']);
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
			$input = $this->convertToId($input);

			if($input === false) {
				return false;
			}

			if(array_key_exists('id', $input)) {
				$input['_id'] = $input['id'];
				unset($input['id']);
			}
		}

		try {
			$col = $this->db->selectCollection($collection);

			$col->insert($input, array('safe' => true));

			if(array_key_exists('_id', $input)) {
				$output = $input;

				if($options['_convert_id']) {
					$output = $this->convertFromId($output);

					$output['id'] = $output['_id'];
					unset($output['_id']);
				}

				return $output;
			}

			$this->error = '_id was not created';
		} catch (Exception $e) {
			$this->error = $e->getMessage();
		}

		return false;
	}

	/**
	 * Build and run update()
	 * @param string $collection
	 * @param array $key key to search for
	 * @param array $input field/values to add
	 * @param array $options
	 * @return bool
	 */
	public function update($collection, $key, $input, $options = array()) {
		$this->error = '';
		$col = '';
		$multiple = false;
		$output = array();

		if($options['multiple']) {
			$multiple = true;
		}

		if($options['_convert_id']) {
			$input = $this->convertToId($input);

			if($input === false) {
				return false;
			}

			if(array_key_exists('id', $input)) {
				$input['_id'] = $input['id'];
				unset($input['id']);
			}

			$key = $this->convertToId($key);

			if($key === false) {
				return false;
			}

			if(array_key_exists('id', $key)) {
				$key['_id'] = $key['id'];
				unset($key['id']);
			}
		}

		try {
			$col = $this->db->selectCollection($collection);

			$col->update($key, $input, array(
				'multiple' => $multiple,
				'safe' => true,
			));

			return true;
		} catch (Exception $e) {
			$this->error = $e->getMessage();
		}

		return false;
	}

	/**
	 * Convert an object id into a string
	 * @param mixed $input
	 * @return mixed
	 */
	public function _convertToId($input) {
		if(!is_array($input)) {
			try {
				$output = new MongoId($input);
				return $output;
			} catch(Exception $e) {
				$this->error = $e->getMessage();
				return false;
			}
		}

		$output = array();

		while(list($key, $value) = each($input)) {
			$output[$key] = $this->_convertToId($value);
		}

		return $output;
	}

	/**
	 * Convert a value into an object id
	 * @param mixed $input
	 * @return mixed
	 */
	public function _convertFromId($input) {
		if(!is_array($input)) {
			return $input . '';
		}

		$output = array();

		while(list($key, $value) = each($input)) {
			$output[$key] = $this->_convertFromId($value);
		}

		return $output;
	}
}

?>
