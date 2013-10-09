#!/usr/bin/php
<?php

/* 
 * This software is a property of Color Anomaly.
 * Use of this software for commercial purposes is strictly
 * prohibited.
 */

require_once 'bootstrap.php';

$config = nestConfig(cascadeConfig(APPLICATION_ENV, parse_ini_file(APPLICATION_PATH . '/config/settings.ini', true)));

/**
 * Demonstrate usage here.
 */

use ColorAnomaly\Vines;

$v = new Vines($config['app']['db']);