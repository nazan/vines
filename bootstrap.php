<?php

/**
 * Setup autoloader for here.
 */

if(!defined('APPLICATION_ENV')) {
    define('APPLICATION_ENV', 'development');
}

define('APPLICATION_PATH', realpath(__DIR__));

function cascadeConfig($name, $config) {
    foreach ($config as $k => $v) {
        if (preg_match("/^$name\s?:?/", $k) === 1) {
            $matches = array();
            if (preg_match("/:\s?\w+$/", $k, $matches) === 1) {
                $parent = trim($matches[0], "\t\r\n :");
                
                $v = array_merge(cascadeConfig($parent, $config), $v);
            }
            
            return $v;
        }
    }
    
    throw new \Exception('Invalid configuration file encountered.');
}

function nestConfig($config) {
    $a = array();
    foreach($config as $k => $v) {
        $b = &$a;
        $keys = explode('.', $k);
        $lk = array_pop($keys);
        foreach($keys as $j) {
            if(!isset($b[$j])) {
                $b[$j] = array();
            }
            
            $b = &$b[$j];
        }
        
        $b[$lk] = $v;
    }
    
    return $a;
}


spl_autoload_register(function ($name) {
    require_once 'src/' . trim(str_replace('\\', '/', $name) , "\t\r\n \\") . '.php';
});