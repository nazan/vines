<?php

/*
 * This software is a property of Color Anomaly.
 * Use of this software for commercial purposes is strictly
 * prohibited.
 */

/**
 * Description of Vines
 *
 * @author Hussain Nazan Naeem <hussennaeem@gmail.com>
 */

namespace ColorAnomaly;

use Composer\Script\Event;

class Vines {

    const VERSION = '1.0.0';
    const TREE_ROOT_ALIAS = 'root';
    const RESOURCE_TABLE = 'resource';
    const ROLE_TABLE = 'role';
    const ACTION_TABLE = 'action';
    const CONTROL_TABLE = 'control';
    const TAG_TABLE = 'tag';
    const ROLE_TAG_TABLE = 'role_tag';
    const TCONTROL_TABLE = 'tcontrol';
    const ROLE_STRUCT_HIER = 1;
    const ROLE_STRUCT_FLAT_W_TAGS = 2;

    const EVENT_ID_ROLE_DELETED = 'role_deleted';

    protected $pdo;
    protected $roleStructure;

    protected $eventHandlers = [];
    
    public static function showSetupInfo(Event $event)
    {
        $io = $event->getIO();
        
        $io->write(array(
            "Vines Setup",
            "-------------",
            "Create database for vines using the schema file vines_schema.sql.",
            "Schema file is located under config directory of vines source package."
        ));
        
        return;
    }

    public function __construct($pdoConfig, $roleStructure = null) {
        //echo "Constructing Vines main class." . PHP_EOL;

        if($pdoConfig instanceof \PDO) {
            $this->pdo = $pdoConfig;
        } else {
            $this->pdo = new \PDO("mysql:host={$pdoConfig['host']};dbname={$pdoConfig['dbname']}", $pdoConfig['username'], $pdoConfig['password']);
        }


        $this->roleStructure = (
                (
                !is_null($roleStructure) &&
                in_array($roleStructure, array(static::ROLE_STRUCT_HIER, static::ROLE_STRUCT_FLAT_W_TAGS))
                ) ? $roleStructure : static::ROLE_STRUCT_FLAT_W_TAGS
                );
    }

    public function pushEventHandler($eventId, $handler) {
        if(!isset($this->eventHandlers[$eventId]) || !is_array($this->eventHandlers[$eventId])) {
            $this->eventHandlers[$eventId] = [];
        }

        $this->eventHandlers[$eventId][] = $handler;

        return count($this->eventHandlers[$eventId]) - 1;
    }

    public function unregisterEventHandlers($eventId, $index = null) {
        if(!isset($this->eventHandlers[$eventId]) || !is_array($this->eventHandlers[$eventId])) {
            return;
        }

        if(is_null($index) || !is_numeric($index)) {
            unset($this->eventHandlers[$eventId]);
            return;
        }

        unset($this->eventHandlers[$eventId][$index]);
    }

    public function executeEventHandlers($eventId, $payload) {
        if(!isset($this->eventHandlers[$eventId]) || !is_array($this->eventHandlers[$eventId])) {
            return;
        }
        
        foreach($this->eventHandlers[$eventId] as $handler) {
            call_user_func($handler, $payload);
        }
    }

    public function prepareTrees() {
        $this->checkTreeStructure(static::RESOURCE_TABLE);

        if ($this->roleStructure == static::ROLE_STRUCT_FLAT_W_TAGS) {
            $q = $this->pdo->query("DELETE FROM `" . static::ROLE_TABLE . "` WHERE `lt` IS NOT NULL AND `rt` IS NOT NULL");

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Unable to reset role table. ERROR: " . $err[2]);
            }
        } else {
            $this->checkTreeStructure(static::ROLE_TABLE);
        }
    }

    public function checkTreeStructure($table) {
        $q = $this->pdo->prepare("SELECT * FROM `$table` WHERE `alias`=:alias");
        $q->bindValue(':alias', static::TREE_ROOT_ALIAS, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Unable to check if parent node of $table table exist. ERROR: " . $err[2]);
        }

        if ($q->rowCount() == 0) {
            $this->resetTables([$table], true);

            $q = $this->pdo->prepare("INSERT INTO `$table` (`lt`, `rt`, `alias`, `description`) VALUES (:lt, :rt, :alias, :dscp);");
            $q->bindValue(':lt', 1, \PDO::PARAM_INT);
            $q->bindValue(':rt', 2, \PDO::PARAM_INT);
            $q->bindValue(':alias', static::TREE_ROOT_ALIAS, \PDO::PARAM_STR);
            $q->bindValue(':dscp', static::TREE_ROOT_ALIAS, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Unable to create root node on $table table. ERROR: " . $err[2]);
            }
        }

        return true;
    }

    public function resetTables($list, $resetAutoIncrement = false) {
        foreach($list as $table) {
            $r = $this->pdo->query("DELETE FROM `$table`;");
            $this->checkPdoResult($r, "Unable to delete $table rows. ERROR: ");

            if($resetAutoIncrement) {
                $r = $this->pdo->query("ALTER TABLE `$table` AUTO_INCREMENT = 1;");
                $this->checkPdoResult($r, "Unable to alter $table table. ERROR: ");
            }
        }
    }

    private function checkPdoResult($result, $msg) {
        $err = ($result !== false ? $result->errorInfo() : $this->pdo->errorInfo());
    
        if ($err[0] !== '00000') {
            throw new \PDOException($msg . $err[2]);
        }
    }

    public function getVersion() {
        return static::VERSION;
    }

    public function addTreeNode($table, $alias, $parentAlias, $description = null) {
        try {
            $this->pdo->beginTransaction();
            $q = $this->pdo->prepare("SELECT * FROM `$table` WHERE `alias`=:alias");
            $q->bindValue(':alias', $parentAlias, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Retrieval of given $table parent failed while trying to add a new node. ERROR: " . $err[2]);
            }

            if ($q->rowCount() > 0) {
                $parent = $q->fetch(\PDO::FETCH_ASSOC);
                $newLt = $parent['rt'];
                $newRt = $newLt + 1;

                $q = $this->pdo->prepare("UPDATE `$table` SET " .
                        "`lt` = CASE WHEN `lt` >= :pr THEN `lt`+2 ELSE `lt` END, " .
                        "`rt` = `rt`+2 " .
                        "WHERE `rt` >= :pl AND `rt` >= :pr2");

                $q->bindValue(':pr', $parent['rt'], \PDO::PARAM_INT);
                $q->bindValue(':pl', $parent['lt'], \PDO::PARAM_INT);
                $q->bindValue(':pr2', $parent['rt'], \PDO::PARAM_INT);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Recalculation of left/right values failed for $table while adding a new node. ERROR: " . $err[2]);
                }

                $q = $this->pdo->prepare("INSERT INTO `$table` (`lt`, `rt`, `alias`, `description`) VALUES (:lt, :rt, :alias, :dscp)");
                $q->bindValue(':lt', $newLt, \PDO::PARAM_INT);
                $q->bindValue(':rt', $newRt, \PDO::PARAM_INT);
                $q->bindValue(':alias', $alias, \PDO::PARAM_STR);
                $q->bindValue(':dscp', is_null($description) ? null : $description, is_null($description) ? \PDO::PARAM_INT : \PDO::PARAM_STR);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Insertion of new node into $table failed. ERROR: " . $err[2]);
                }

                $this->pdo->commit();
                return true;
            } else {
                return false;
            }
        } catch (\PDOException $pdoExcp) {
            $this->pdo->rollBack();
            throw $pdoExcp;
        }
    }

    public function editTreeNode($table, $alias, $description) {
        $q = $this->pdo->prepare("UPDATE `$table` SET `description`=:dscp WHERE `alias`=:alias");
        $q->bindValue(':alias', $alias, \PDO::PARAM_STR);
        $q->bindValue(':dscp', $description, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000') {
            if ($q->rowCount() == 0) {
                throw new \PDOException("Given record not found while trying to edit '$table' table row.");
            }

            return true;
        } else {
            throw new \PDOException("Update of existing record in '$table' table failed. ERROR: " . $err[2]);
        }
    }

    public function removeTreeNode($table, $alias) {
        if ($alias === static::TREE_ROOT_ALIAS) {
            throw new \Exception("Root node of $table cannot be removed.");
        }

        try {
            $this->pdo->beginTransaction();
            $q = $this->pdo->prepare("SELECT * FROM `$table` WHERE `alias`=:alias");
            $q->bindValue(':alias', $alias, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to retrieve $table while trying to remove node. ERROR: " . $err[2]);
            }

            if ($q->rowCount() > 0) {
                $node = $q->fetch(\PDO::FETCH_ASSOC);

                $q = $this->pdo->prepare("DELETE FROM `$table` WHERE `lt` >= :lt AND `rt` <= :rt");

                $q->bindValue(':lt', $node['lt'], \PDO::PARAM_INT);
                $q->bindValue(':rt', $node['rt'], \PDO::PARAM_INT);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Query failed while trying to remove children of given node. ERROR: " . $err[2]);
                }

                $toDeduct = $node['rt'] - $node['lt'] + 1;

                $q = $this->pdo->prepare("UPDATE `$table` SET " .
                        "`lt` = CASE WHEN `lt` >= :rt1 THEN `lt`-:deduction1 ELSE `lt` END, " .
                        "`rt` = `rt`-:deduction2 " .
                        "WHERE `rt` >= :lt1 AND `rt` >= :rt2");

                $q->bindValue(':lt1', $node['lt'], \PDO::PARAM_INT);
                $q->bindValue(':rt1', $node['rt'], \PDO::PARAM_INT);
                $q->bindValue(':rt2', $node['rt'], \PDO::PARAM_INT);
                $q->bindValue(':deduction1', $toDeduct, \PDO::PARAM_INT);
                $q->bindValue(':deduction2', $toDeduct, \PDO::PARAM_INT);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Recalculation of left/right values failed for $table while removing a new node. ERROR: " . $err[2]);
                }

                $this->pdo->commit();
            } else {
                throw new \Exception("Non existing $table node deletion attempt.");
            }

            return true;
        } catch (\PDOException $pdoExcp) {
            $this->pdo->rollBack();
            throw $pdoExcp;
        }
    }

    public function addResource($alias, $parentAlias, $description = null) {
        return $this->addTreeNode(static::RESOURCE_TABLE, $alias, $parentAlias, $description);
    }

    public function editResource($alias, $description) {
        return $this->editTreeNode(static::RESOURCE_TABLE, $alias, $description);
    }

    public function removeResource($alias) {
        return $this->removeTreeNode(static::RESOURCE_TABLE, $alias);
    }

    public function addTag($name) {
        $q = $this->pdo->prepare("INSERT INTO `" . static::TAG_TABLE . "` (`name`) VALUES (:name)");
        $q->bindValue(':name', $name, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000' || $err[0] === '23000') {
            return true;
        } else {
            throw new \PDOException("Insertion of new tag failed. ERROR: " . $err[2]);
        }
    }

    public function removeTag($name) {
        $q = $this->pdo->prepare("DELETE FROM `" . static::TAG_TABLE . "` WHERE `name` = :name");

        $q->bindValue(':name', $name, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Query failed while trying to remove tag. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent tag deletion attempt.");
        }

        return true;
    }

    public function tagRole($role, $tags) {
        if (is_array($tags) && count($tags) > 0) {
            try {
                $q = $this->pdo->prepare("SELECT `id` FROM `" . static::ROLE_TABLE . "` WHERE `alias`=:alias");
                $q->bindValue(':alias', $role, \PDO::PARAM_STR);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Failed to retrieve role while trying to associate with tag. ERROR: " . $err[2]);
                }

                if ($q->rowCount() > 0) {
                    $roleId = $q->fetch(\PDO::FETCH_COLUMN);

                    foreach ($tags as $tag) {
                        try {
                            $q = $this->pdo->prepare("SELECT `id` FROM `" . static::TAG_TABLE . "` WHERE `name`=:name");
                            $q->bindValue(':name', $tag, \PDO::PARAM_STR);

                            $q->execute();

                            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                            if ($err[0] !== '00000') {
                                throw new \PDOException("Failed to retrieve tag while trying to associate with tag. ERROR: " . $err[2]);
                            }

                            if ($q->rowCount() > 0) {
                                $tagId = $q->fetch(\PDO::FETCH_COLUMN);

                                $q = $this->pdo->prepare("INSERT INTO `" . static::ROLE_TAG_TABLE . "` (`role_id`, `tag_id`) VALUES (:role, :tag)");
                                $q->bindValue(':role', $roleId, \PDO::PARAM_INT);
                                $q->bindValue(':tag', $tagId, \PDO::PARAM_INT);

                                $q->execute();

                                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                                if ($err[0] === '00000' || $err[0] === '23000') {
                                    continue;
                                } else {
                                    throw new \PDOException("Association of role #$roleId & tag #$tagId failed. ERROR: " . $err[2]);
                                }
                            }
                        } catch (\PDOException $pdoExcp1) {
                            continue;
                        }
                    }
                } else {
                    throw new \PDOException("Given role not found while attempting to associate with tag(s).");
                }
            } catch (\PDOException $pdoExcp2) {
                return false;
            }
        }

        return true;
    }

    public function untagRole($role, $tags) {
        if (!is_array($tags)) {
            $tags = array($tags);
        }

        try {
            $q = $this->pdo->prepare("DELETE `role_tag` FROM `role_tag` INNER JOIN `" . static::ROLE_TABLE . "` AS `rol` ON `role_tag`.`role_id` = `rol`.`id` INNER JOIN `" . static::TAG_TABLE . "` AS `tag` ON `role_tag`.`tag_id` = `tag`.`id` WHERE `rol`.`alias` = :roleAlias AND `tag`.`name` IN ('" . join("','", $tags) . "')");
            $q->bindValue(':roleAlias', $role, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to dissociate role & tag during untag attempt. ERROR: " . $err[2]);
            }
        } catch (\PDOException $pdoExcp2) {
            return false;
        }

        return true;
    }

    public function addRole($alias, $related = null, $description = null) {
        if ($this->roleStructure == static::ROLE_STRUCT_FLAT_W_TAGS) {
            try {
                $this->pdo->beginTransaction();

                $q = $this->pdo->prepare("INSERT INTO `" . static::ROLE_TABLE . "` (`alias`, `description`) VALUES (:alias, :dscp)");
                $q->bindValue(':alias', $alias, \PDO::PARAM_STR);
                $q->bindValue(':dscp', is_null($description) ? null : $description, is_null($description) ? \PDO::PARAM_INT : \PDO::PARAM_STR);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Insertion of new role failed. ERROR: " . $err[2]);
                }

                if (!is_null($related)) {
                    if (!is_array($related)) {
                        $related = array($related);
                    }

                    if (count($related) > 0) {
                        foreach ($related as $tagName) {
                            $this->addTag($tagName);
                        }

                        $this->tagRole($alias, $related);
                    }
                }

                $this->pdo->commit();

                return true;
            } catch (\PDOException $pdoExcp) {
                $this->pdo->rollBack();
                throw $pdoExcp;
            }
        } else {
            if (is_array($related)) {
                $related = array_shift($related);
            }

            return $this->addTreeNode(static::ROLE_TABLE, $alias, $related);
        }
    }

    public function editRole($alias, $description) {
        return $this->editTreeNode(static::ROLE_TABLE, $alias, $description);
    }

    public function removeRole($alias) {
        if ($this->roleStructure == static::ROLE_STRUCT_FLAT_W_TAGS) {
            $q = $this->pdo->prepare("DELETE FROM `" . static::ROLE_TABLE . "` WHERE `alias` = :alias");

            $q->bindValue(':alias', $alias, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Query failed while trying to remove given role. ERROR: " . $err[2]);
            } elseif ($q->rowCount() == 0) {
                throw new \PDOException("Non-existent role deletion attempt.");
            }
        } else {
            $this->removeTreeNode(static::ROLE_TABLE, $alias);
        }

        $this->executeEventHandlers(self::EVENT_ID_ROLE_DELETED, ['alias'=>$alias]);

        return true;
    }

    public function addAction($alias, $description, $flags = null) {
        $q = $this->pdo->prepare("INSERT INTO `" . static::ACTION_TABLE . "` (`alias`, `description`) VALUES (:alias, :dscp)");
        $q->bindValue(':alias', $alias, \PDO::PARAM_STR);
        $q->bindValue(':dscp', $description, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000' || $err[0] === '23000') {
            return true;
        } else {
            throw new \PDOException("Insertion of new action failed. ERROR: " . $err[2]);
        }
    }

    public function editAction($alias, $description, $flags = null) {
        $q = $this->pdo->prepare("UPDATE `" . static::ACTION_TABLE . "` SET `description`=:dscp WHERE `alias`=:alias");
        $q->bindValue(':alias', $alias, \PDO::PARAM_STR);
        $q->bindValue(':dscp', $description, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000') {
            if ($q->rowCount() == 0) {
                throw new \PDOException("Given action not found while trying to edit action.");
            }

            return true;
        } else {
            throw new \PDOException("Update of existing action failed. ERROR: " . $err[2]);
        }
    }

    public function removeAction($alias) {
        $q = $this->pdo->prepare("DELETE FROM `" . static::ACTION_TABLE . "` WHERE `alias` = :alias");

        $q->bindValue(':alias', $alias, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Query failed while trying to remove action. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent action deletion attempt.");
        }

        return true;
    }

    public function getActionId($action) {
        $q = $this->pdo->prepare("SELECT `id` FROM `" . static::ACTION_TABLE . "` WHERE `alias`=:alias");
        $q->bindValue(':alias', $action, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve action while trying to enforce access policy. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent action given.");
        }

        return $q->fetch(\PDO::FETCH_COLUMN);
    }

    public function getRoleId($role) {
        $q = $this->pdo->prepare("SELECT `id` FROM `" . static::ROLE_TABLE . "` WHERE `alias`=:alias");
        $q->bindValue(':alias', $role, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve role while trying to enforce access policy. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent role given.");
        }

        return $q->fetch(\PDO::FETCH_COLUMN);
    }

    public function getResourceId($resource) {
        $q = $this->pdo->prepare("SELECT `id` FROM `" . static::RESOURCE_TABLE . "` WHERE `alias`=:alias");
        $q->bindValue(':alias', $resource, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve resource while trying to enforce access policy. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent resource given.");
        }

        return $q->fetch(\PDO::FETCH_COLUMN);
    }

    /**
     * $allowed = true -> allow
     * $allowed = false -> deny
     * 
     * Enforce $allowed of $action by $role to $resource
     * Enforce 'allowance' of 'write' by 'admin' to 'company'
     * Enforce 'denial' of 'write' by 'admin' to 'high-profile-client-data'
     */
    public function enforce($allowed, $action, $role, $resource) {
        $actionId = $this->getActionId($action);

        $roleId = $this->getRoleId($role);

        $resourceId = $this->getResourceId($resource);

        $q = $this->pdo->prepare("INSERT INTO `" . static::CONTROL_TABLE . "` (`role_id`, `action_id`, `resource_id`, `allowed`) VALUES (:roleId, :actionId, :resourceId, :allowed)");
        $q->bindValue(':roleId', $roleId, \PDO::PARAM_INT);
        $q->bindValue(':actionId', $actionId, \PDO::PARAM_INT);
        $q->bindValue(':resourceId', $resourceId, \PDO::PARAM_INT);
        $q->bindValue(':allowed', ($allowed ? true : false), \PDO::PARAM_INT);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000' || $err[0] === '23000') {
            return true;
        } else {
            throw new \PDOException("Query failed while inserting control record for access policy. ERROR: " . $err[2]);
        }
    }

    public function lift($action, $role, $resource) {
        $actionId = $this->getActionId($action);

        $roleId = $this->getRoleId($role);

        $resourceId = $this->getResourceId($resource);

        $q = $this->pdo->prepare("DELETE FROM `" . static::CONTROL_TABLE . "` WHERE `role_id` = :roleId AND `action_id` = :actionId AND `resource_id` = :resourceId");
        $q->bindValue(':roleId', $roleId, \PDO::PARAM_INT);
        $q->bindValue(':actionId', $actionId, \PDO::PARAM_INT);
        $q->bindValue(':resourceId', $resourceId, \PDO::PARAM_INT);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000' || $err[0] === '23000') {
            return true;
        } else {
            throw new \PDOException("Query failed while deleting control record from access policy. ERROR: " . $err[2]);
        }
    }

    public function enforceTag($allowed, $action, $tag, $resource) {
        $q = $this->pdo->prepare("SELECT `id` FROM `" . static::ACTION_TABLE . "` WHERE `alias`=:alias");
        $q->bindValue(':alias', $action, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve action while trying to enforce access policy. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent action given.");
        }

        $actionId = $q->fetch(\PDO::FETCH_COLUMN);

        $q = $this->pdo->prepare("SELECT `id` FROM `" . static::TAG_TABLE . "` WHERE `name`=:name");
        $q->bindValue(':name', $tag, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve tag while trying to enforce access policy. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent tag given.");
        }

        $tagId = $q->fetch(\PDO::FETCH_COLUMN);

        $q = $this->pdo->prepare("SELECT `id` FROM `" . static::RESOURCE_TABLE . "` WHERE `alias`=:alias");
        $q->bindValue(':alias', $resource, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve resource while trying to enforce access policy. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent resource given.");
        }

        $resourceId = $q->fetch(\PDO::FETCH_COLUMN);

        $q = $this->pdo->prepare("INSERT INTO `" . static::TCONTROL_TABLE . "` (`tag_id`, `action_id`, `resource_id`, `allowed`) VALUES (:tagId, :actionId, :resourceId, :allowed)");
        $q->bindValue(':tagId', $tagId, \PDO::PARAM_INT);
        $q->bindValue(':actionId', $actionId, \PDO::PARAM_INT);
        $q->bindValue(':resourceId', $resourceId, \PDO::PARAM_INT);
        $q->bindValue(':allowed', ($allowed ? true : false), \PDO::PARAM_INT);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000' || $err[0] === '23000') {
            return true;
        } else {
            throw new \PDOException("Query failed while inserting tcontrol record for access policy. ERROR: " . $err[2]);
        }
    }

    public function allowed($role, $action, $resource) {
        if(empty($role)) {
            return false;
        }

        $allowed = false; // assume denied.
        $controls = [];

        if ($this->roleStructure == static::ROLE_STRUCT_FLAT_W_TAGS) {
            $byTag = false;
            $byRole = false;

            if(is_scalar($role)) {
                $roles = [$role];
            } else {
                $roles = $role;
            }
    
            $qMarks = "(" . implode(",", array_fill(0, count($roles), '?')) . ")";

            $q = $this->pdo->prepare("SELECT `res_anc`.`rt` AS `right`, `res_anc`.`alias` AS `resource`, `act`.`alias` AS `action`, `rol`.`alias` AS `role`, `con`.`allowed` AS `access` " .
                    "FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` " .
                    "LEFT JOIN `" . static::CONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` " .
                    "LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` " .
                    "LEFT JOIN `" . static::ROLE_TABLE . "` AS `rol` ON `con`.`role_id` = `rol`.`id` " .
                    "WHERE `res`.`alias` = ? AND `act`.`alias` = ? AND `rol`.`alias` IN $qMarks " .
                    "ORDER BY `res_anc`.`rt` DESC");

            $q->execute(array_merge([$resource, $action], array_values($roles)));

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to retrieve access enforcement for given role. ERROR: " . $err[2]);
            }

            if ($q->rowCount() > 0) {
                $controls = array_merge($controls, $q->fetchAll(\PDO::FETCH_ASSOC));
                $byRole = true;
            }

            $roleConditions = " WHERE `rol_sub`.`alias` IN " . $qMarks;

            $q = $this->pdo->prepare("SELECT `res_anc`.`rt` AS `right`, `res_anc`.`alias` AS `resource`, `act`.`alias` AS `action`, `tag`.`name` AS `tag`, `con`.`allowed` AS `access` " .
                    "FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` " .
                    "LEFT JOIN `" . static::TCONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` " .
                    "LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` " .
                    "LEFT JOIN `" . static::TAG_TABLE . "` AS `tag` ON `con`.`tag_id` = `tag`.`id` " .
                    "WHERE `res`.`alias` = ? AND `act`.`alias` = ? AND `tag`.`id` IN (" .
                    "SELECT `tag_sub`.`id` AS `id` FROM `" . static::ROLE_TAG_TABLE . "` AS `rtag_sub` INNER JOIN `" . static::ROLE_TABLE . "` AS `rol_sub` ON `rtag_sub`.`role_id` = `rol_sub`.`id` INNER JOIN `" . static::TAG_TABLE . "` AS `tag_sub` ON `rtag_sub`.`tag_id` = `tag_sub`.`id`  WHERE `rol_sub`.`alias` IN $qMarks" .
                    ") " .
                    "ORDER BY `res_anc`.`rt` DESC;");

            $q->execute(array_merge([$resource, $action], array_values($roles)));

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to retrieve access enforcement for tags associated with given role. ERROR: " . $err[2]);
            }

            if ($q->rowCount() > 0) {
                $controls = array_merge($controls, $q->fetchAll(\PDO::FETCH_ASSOC));
                $byTag = true;
            }

            if ($byTag && $byRole) {
                usort($controls, function($a, $b) {
                    if ($a['right'] == $b['right']) {
                        if ($a['access'] == $b['access']) {
                            return 0;
                        }

                        return ($a['access'] ? -1 : 1);
                    }
                    return ($a['right'] < $b['right']) ? 1 : -1;
                });
            }
        } else {
            if(is_array($role) && count($role) > 0) {
                $role = array_values($role)[0];
            }

            $q = $this->pdo->prepare("SELECT `res_anc`.`alias` AS `resource`, `act`.`alias` AS `action`, `rol`.`alias` AS `role`, `con`.`allowed` AS `access` " .
                "FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` " .
                "LEFT JOIN `" . static::CONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` " .
                "LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` " .
                "LEFT JOIN `" . static::ROLE_TABLE . "` AS `rol` ON `con`.`role_id` = `rol`.`id` " .
                "WHERE `res`.`alias` = :resource AND `act`.`alias` = :action AND `rol`.`id` IN " .
                "(" .
                "SELECT `rol_anc_sub`.`id` AS `id` " .
                "FROM `" . static::ROLE_TABLE . "` AS `rol_sub` JOIN `" . static::ROLE_TABLE . "` as `rol_anc_sub` ON `rol_sub`.`lt` BETWEEN `rol_anc_sub`.`lt` AND `rol_anc_sub`.`rt` " .
                "WHERE `rol_sub`.`alias` = :role " .
                "ORDER BY `rol_anc_sub`.`rt` DESC" .
                ")" .
                "ORDER BY `res_anc`.`rt` DESC;"
            );

            $q->bindValue(':resource', $resource, \PDO::PARAM_STR);
            $q->bindValue(':action', $action, \PDO::PARAM_STR);
            $q->bindValue(':role', $role, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to retrieve access enforcement for given resource. ERROR: " . $err[2]);
            }

            $controls = $q->fetchAll(\PDO::FETCH_ASSOC);
        }

        foreach ($controls as $control) {
            $allowed = $control['access'] == 1;
        }

        return $allowed;
    }

    public function getTargetResourceNode($alias) {
        // retrieve the left and right value of the $root node
        $rsrcTable = static::RESOURCE_TABLE;
        $q = $this->pdo->prepare("SELECT `id`, `lt`, `rt`, `alias`, `description` FROM `$rsrcTable` WHERE `alias` = :target_alias;");

        $q->bindValue(':target_alias', $alias, \PDO::PARAM_STR);
        
        $q->execute(); 

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve target resource node. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent target resource node given.");
        }

        return $q->fetch(\PDO::FETCH_ASSOC);
    }

    public function getNestedResource($targetNodeAlias = null, $excludePrefixes = []) {
        if(is_null($targetNodeAlias)) {
            $targetNodeAlias = static::TREE_ROOT_ALIAS;
        }

        $targetNode = $this->getTargetResourceNode($targetNodeAlias);

        $references = [];

        $tree = [];

        if(count($excludePrefixes) > 0) {
            $excludeConditions = " AND " . implode(" AND ", array_fill(0, count($excludePrefixes), '`alias` NOT LIKE ?'));
        } else {
            $excludeConditions = '';
        }
    
        // Now, retrieve all descendants of the target node.
        $q = $this->pdo->prepare("SELECT `id`, `lt`, `rt`, `alias`, `description` FROM `" . static::RESOURCE_TABLE . "` WHERE `lt` BETWEEN ? AND ?$excludeConditions ORDER BY `lt` ASC;");
        
        $q->execute(array_merge([$targetNode['lt'], $targetNode['rt']], array_map(function($rec) { return trim($rec) . '%'; }, $excludePrefixes)));

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve descendents of target resource node while trying to build nested data structure. ERROR: " . $err[2]);
        }
    
        while($row = $q->fetch(\PDO::FETCH_ASSOC)) {
            // Only check stack if there is one.
            if(count($references) > 0) {
                // Check if we should remove a node from the stack.
                while($references[count($references)-1]['rt'] < $row['rt']) {
                    array_pop($references);
                }
            }

            $newNode = [
                'id' => $row['alias'],
                'text' => $row['description'],
                'rt' => $row['rt'],
                'state' => [
                    'expanded' => true,
                ],
                'nodes' => [],
            ];

            if(count($references) <= 0) {
                $tree[] = $newNode;
                $references[] = &$tree[count($tree)-1];
            } else {
                $refTip = &$references[count($references)-1]['nodes'];
                $refTip[] = $newNode;

                $references[] = &$refTip[count($refTip)-1];
            }
        }

        return $tree;
    }

    public function getPathToResourceNode($targetNodeAlias, $targetInclusive = false) {
        $targetNode = $this->getTargetResourceNode($targetNodeAlias);

        $conditions[] = "`lt` ".($targetInclusive ? '<=' : '<')." :in_lt";
        $conditions[] = "`rt` ".($targetInclusive ? '>=' : '>')." :in_rt";

        $conditionsStr = implode(' AND ', $conditions);

        $rsrcTable = static::RESOURCE_TABLE;
        $q = $this->pdo->prepare("SELECT * FROM `$rsrcTable` WHERE $conditionsStr ORDER BY `lt` ASC;");

        $q->bindValue(':in_lt', $targetNode['lt'], \PDO::PARAM_INT);
        $q->bindValue(':in_rt', $targetNode['rt'], \PDO::PARAM_INT);
        
        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve anscestors of target resource node while trying to build list of ancestors. ERROR: " . $err[2]);
        }

        $rows = $q->fetchAll(\PDO::FETCH_ASSOC);

        return array_map(function($row) {
            return [
                'id' => $row['alias'],
                'text' => $row['description'],
                'lt' => $row['lt'],
                'rt' => $row['rt']
            ];
        }, $rows);
    }

    public function getNodeDescendents($targetNodeAlias, $includePrefixes, $conditions, $pageSize, $page) {
        if(is_null($targetNodeAlias)) {
            $targetNodeAlias = static::TREE_ROOT_ALIAS;
        }

        $targetNode = $this->getTargetResourceNode($targetNodeAlias);

        if(count($includePrefixes) === 0) {
            return [0, []];
        }

        $queryParams = array_merge([$targetNode['lt'], $targetNode['rt']], array_map(function($rec) { return trim($rec) . '%'; }, $includePrefixes), array_map(function($condition) { return $condition[2]; }, $conditions));

        $includePrefixes = " AND " . implode(' AND ', array_fill(0, count($includePrefixes), '`alias` LIKE ?'));

        $conditionsStr = count($conditions) > 0 ? ' AND (' . implode(' OR ', array_map(function($condition) { return "`{$condition[0]}` {$condition[1]} ?"; }, $conditions)) . ')' : '';

        $queryStr = "SELECT COUNT(*) FROM `" . static::RESOURCE_TABLE . "` WHERE `lt` BETWEEN ? AND ?{$includePrefixes}{$conditionsStr} ORDER BY `id` ASC;";

        $q = $this->pdo->prepare($queryStr);

        $q->execute($queryParams);

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve count of total number of resources under given node. ERROR: " . $err[2]);
        }

        if ($q->rowCount() === 0) {
            return [0, []];
        }
        
        $totalRowCount = $q->fetch(\PDO::FETCH_COLUMN);

        $offset = $pageSize * ($page - 1);

        $queryStr = "SELECT `id`, `lt`, `rt`, `alias`, `description` FROM `" . static::RESOURCE_TABLE . "` WHERE `lt` BETWEEN ? AND ?{$includePrefixes}{$conditionsStr} ORDER BY `id` ASC LIMIT $offset, $pageSize;";
        
        //dd($queryStr, $queryParams);
    
        // Now, retrieve all descendants of the target node.
        $q = $this->pdo->prepare($queryStr);
        
        $q->execute($queryParams);

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve descendents of target resource node while trying to fetch descendents. ERROR: " . $err[2]);
        }
    
        return [$totalRowCount, array_map(function($row) {
            return $row['alias'];
        }, $q->fetchAll(\PDO::FETCH_ASSOC))];
    }

    public function getControls($resource) {
        $allowed = false; // assume denied.
        $controls = [];

        if ($this->roleStructure == static::ROLE_STRUCT_FLAT_W_TAGS) {
            $q = $this->pdo->prepare("SELECT `res_anc`.`rt` AS `right`, `res_anc`.`alias` AS `resource`, `res_anc`.`description` AS `resource_description`, `act`.`alias` AS `action`, `act`.`description` AS `action_description`, `rol`.`alias` AS `role`, `rol`.`description` AS `role_description`, `con`.`allowed` AS `access` " .
                    "FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` " .
                    "LEFT JOIN `" . static::CONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` " .
                    "LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` " .
                    "LEFT JOIN `" . static::ROLE_TABLE . "` AS `rol` ON `con`.`role_id` = `rol`.`id` " .
                    "WHERE `res`.`alias` = ? " .
                    "ORDER BY `res_anc`.`rt` DESC, `rol`.`description` ASC, `act`.`description` ASC");

            $q->execute([$resource]);

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to retrieve access enforcement for given role. ERROR: " . $err[2]);
            }

            if ($q->rowCount() > 0) {
                $controls = $q->fetchAll(\PDO::FETCH_ASSOC);
            }
        } else {
            $q = $this->pdo->prepare("SELECT `res_anc`.`alias` AS `resource`, `res_anc`.`description` AS `resource_description`, `act`.`alias` AS `action`, `act`.`description` AS `action_description`, `rol`.`alias` AS `role`, `rol`.`description` AS `role_description`, `con`.`allowed` AS `access` " .
                "FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` " .
                "LEFT JOIN `" . static::CONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` " .
                "LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` " .
                "LEFT JOIN `" . static::ROLE_TABLE . "` AS `rol` ON `con`.`role_id` = `rol`.`id` " .
                "WHERE `res`.`alias` = :resource ORDER BY `res_anc`.`rt` DESC, `rol`.`description` ASC, `act`.`description` ASC;"
            );

            $q->bindValue(':resource', $resource, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to retrieve access enforcement for given resource. ERROR: " . $err[2]);
            }

            $controls = $q->fetchAll(\PDO::FETCH_ASSOC);
        }

        return $controls;
    }

    public function rolesSearch($keyword) {
        $q = $this->pdo->prepare("SELECT `rol`.`id` AS `id`, `rol`.`alias` AS `alias`, `rol`.`description` AS `description` " .
                "FROM `" . static::ROLE_TABLE . "` AS `rol` " .
                "WHERE `rol`.`alias` LIKE ? " .
                "ORDER BY `rol`.`description` ASC");

        $q->execute(["%$keyword%"]);

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve roles while searching for roles. ERROR: " . $err[2]);
        }

        return $q->rowCount() > 0 ? array_map(function($item) { return ['value'=>$item, 'label'=>$item['description']]; }, $q->fetchAll(\PDO::FETCH_ASSOC)) : [];
    }

    public function getRoles($roleAliases) {
        if(is_array($roleAliases) && count($roleAliases) > 0) {
            $conditions = " WHERE `rol`.`alias` in (" . implode(",", array_fill(0, count($roleAliases), '?')) . ")";
        } else {
            $conditions = '';
        }

        $q = $this->pdo->prepare("SELECT `rol`.`id` AS `id`, `rol`.`alias` AS `alias`, `rol`.`description` AS `description` " .
                "FROM `" . static::ROLE_TABLE . "` AS `rol`$conditions " .
                "ORDER BY `rol`.`description` ASC");

        $q->execute($roleAliases);

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve roles while converting aliases to array of objects. ERROR: " . $err[2]);
        }

        return $q->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * WARNING: This function deletes all roles, resources, actions, tags and controls. Use with caution in a production environment.
     * 
     * @return boolean
     * @throws \PDOException
     */
    public function purgeDatabase() {
        $this->resetTables([static::CONTROL_TABLE, static::TCONTROL_TABLE, static::ROLE_TAG_TABLE]);
        $this->resetTables([static::ROLE_TABLE, static::RESOURCE_TABLE, static::ACTION_TABLE, static::TAG_TABLE], true);

        return true;
    }

    public function migrateUp() {
        $sql = file_get_contents(realpath(__DIR__ . '/../../config/vines_schema.sql'));

        $result = $this->pdo->exec($sql);
    }

    public function migrateDown() {
        $sql = file_get_contents(realpath(__DIR__ . '/../../config/vines_schema_down.sql'));

        $result = $this->pdo->exec($sql);
    }
}
