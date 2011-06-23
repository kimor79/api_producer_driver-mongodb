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
		} catch (MongoConnectionException $e) {
			throw new Exception($e->getMessage());
		}
	}

	public function __deconstruct() {
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
		$data = array();
		$query = array();

		while(list($key, $values) = each($input)) {
			$query[$key] = array(
				'$in' => array(),
			);

			if(array_key_exists('eq', $values)) {
				while(list($junk, $value) =
						each($values['eq'])) {
					$query[$key]['$in'][] = $value;
				}
			}

			if(array_key_exists('re', $values)) {
				while(list($junk, $value) =
						each($values['re'])) {
					$query[$key]['$in'][] =
						new MongoRegex($value);
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

			if(!$options['id_as_string']) {
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
				$t_data = $cursor->getNext();

				if($options['id_as_string']) {
					$t_data['id'] = $t_data['_id'] . '';
					unset($t_data['_id']);
				}

				$data[] = $t_data;
			}

			$this->count = $cursor->count();

			return $data;
		} catch (MongoCursorException $e) {
			$this->error = $e->getMessage();
		}

		return false;
	}
}

?>
