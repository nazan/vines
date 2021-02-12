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

    protected $pdo;
    protected $roleStructure;
    
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

            $q = $this->pdo->prepare("INSERT INTO `$table` (`lt`, `rt`, `alias`) VALUES (:lt, :rt, :alias);");
            $q->bindValue(':lt', 1, \PDO::PARAM_INT);
            $q->bindValue(':rt', 2, \PDO::PARAM_INT);
            $q->bindValue(':alias', static::TREE_ROOT_ALIAS, \PDO::PARAM_STR);

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
            $q = $this->pdo->query("DELETE FROM `$table`;");
            $this->checkPdoResult($q);

            if($resetAutoIncrement) {
                $q = $this->pdo->query("ALTER TABLE `$table` AUTO_INCREMENT = 1;");
                $this->checkPdoResult($q);
            }
        }
    }

    private function checkPdoResult($result) {
        $err = ($result !== false ? $result->errorInfo() : $this->pdo->errorInfo());
    
        if ($err[0] !== '00000') {
            throw new \PDOException("Unable to delete $table rows. ERROR: " . $err[2]);
        }
    }

    public function getVersion() {
        return static::VERSION;
    }

    public function addTreeNode($table, $alias, $parentAlias) {
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
                        "WHERE `rt` >= :pl AND `rt` >= :pr");

                $q->bindValue(':pl', $parent['lt'], \PDO::PARAM_INT);
                $q->bindValue(':pr', $parent['rt'], \PDO::PARAM_INT);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Recalculation of left/right values failed for $table while adding a new node. ERROR: " . $err[2]);
                }

                $q = $this->pdo->prepare("INSERT INTO `$table` (`lt`, `rt`, `alias`) VALUES (:lt, :rt, :alias)");
                $q->bindValue(':lt', $newLt, \PDO::PARAM_INT);
                $q->bindValue(':rt', $newRt, \PDO::PARAM_INT);
                $q->bindValue(':alias', $alias, \PDO::PARAM_STR);

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

                $q = $this->pdo->prepare("DELETE FROM `$table` WHERE `lt` >= :l AND `rt` <= :r");

                $q->bindValue(':l', $node['lt'], \PDO::PARAM_INT);
                $q->bindValue(':r', $node['rt'], \PDO::PARAM_INT);

                $q->execute();

                $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

                if ($err[0] !== '00000') {
                    throw new \PDOException("Query failed while trying to remove children of given node. ERROR: " . $err[2]);
                }

                $toDeduct = $node['rt'] - $node['lt'] + 1;

                $q = $this->pdo->prepare("UPDATE `$table` SET " .
                        "`lt` = CASE WHEN `lt` >= :r THEN `lt`-:deduction ELSE `lt` END, " .
                        "`rt` = `rt`-:deduction " .
                        "WHERE `rt` >= :l AND `rt` >= :r");

                $q->bindValue(':l', $node['lt'], \PDO::PARAM_INT);
                $q->bindValue(':r', $node['rt'], \PDO::PARAM_INT);
                $q->bindValue(':deduction', $toDeduct, \PDO::PARAM_INT);

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

    public function addResource($alias, $parentAlias) {
        return $this->addTreeNode(static::RESOURCE_TABLE, $alias, $parentAlias);
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

    public function addRole($alias, $related = null) {
        if ($this->roleStructure == static::ROLE_STRUCT_FLAT_W_TAGS) {
            try {
                $this->pdo->beginTransaction();

                $q = $this->pdo->prepare("INSERT INTO `" . static::ROLE_TABLE . "` (`alias`) VALUES (:alias)");
                $q->bindValue(':alias', $alias, \PDO::PARAM_STR);

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

            return true;
        } else {
            return $this->removeTreeNode(static::ROLE_TABLE, $alias);
        }
    }

    public function addAction($alias, $description, $flags = null) {
        $q = $this->pdo->prepare("INSERT INTO `" . static::ACTION_TABLE . "` (`alias`, `description`) VALUES (:alias, :description)");
        $q->bindValue(':alias', $alias, \PDO::PARAM_STR);
        $q->bindValue(':description', $description, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000' || $err[0] === '23000') {
            return true;
        } else {
            throw new \PDOException("Insertion of new action failed. ERROR: " . $err[2]);
        }
    }

    public function editAction($alias, $description, $flags = null) {
        $q = $this->pdo->prepare("UPDATE `" . static::ACTION_TABLE . "` SET `description`=:description WHERE `alias`=:alias");
        $q->bindValue(':alias', $alias, \PDO::PARAM_STR);
        $q->bindValue(':description', $description, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] === '00000') {
            if ($q->rowCount() == 0) {
                throw new \PDOException("Given action not found while trying to edit action.");
            }

            return true;
        } else {
            throw new \PDOException("Insertion of new action failed. ERROR: " . $err[2]);
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

    /**
     * $allowed = true -> allow
     * $allowed = false -> deny
     * 
     * Enforce $allowed of $action by $role to $resource
     * Enforce 'allowance' of 'write' by 'admin' to 'company'
     * Enforce 'denial' of 'write' by 'admin' to 'high-profile-client-data'
     */
    public function enforce($allowed, $action, $role, $resource) {
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

        $q = $this->pdo->prepare("SELECT `id` FROM `" . static::ROLE_TABLE . "` WHERE `alias`=:alias");
        $q->bindValue(':alias', $role, \PDO::PARAM_STR);

        $q->execute();

        $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

        if ($err[0] !== '00000') {
            throw new \PDOException("Failed to retrieve role while trying to enforce access policy. ERROR: " . $err[2]);
        } elseif ($q->rowCount() == 0) {
            throw new \PDOException("Non-existent role given.");
        }

        $roleId = $q->fetch(\PDO::FETCH_COLUMN);

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
        $allowed = false; // assume denied.
        $controls = array();

        if ($this->roleStructure == static::ROLE_STRUCT_FLAT_W_TAGS) {
            $byTag = false;
            $byRole = false;

            $q = $this->pdo->prepare("SELECT `res_anc`.`rt` AS `right`, `res_anc`.`alias` AS `resource`, `act`.`alias` AS `action`, `rol`.`alias` AS `role`, `con`.`allowed` AS `access` " .
                    "FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` " .
                    "LEFT JOIN `" . static::CONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` " .
                    "LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` " .
                    "LEFT JOIN `" . static::ROLE_TABLE . "` AS `rol` ON `con`.`role_id` = `rol`.`id` " .
                    "WHERE `res`.`alias` = :resource AND `act`.`alias` = :action AND `rol`.`alias` = :role " .
                    "ORDER BY `res_anc`.`rt` DESC");

            $q->bindValue(':resource', $resource, \PDO::PARAM_STR);
            $q->bindValue(':action', $action, \PDO::PARAM_STR);
            $q->bindValue(':role', $role, \PDO::PARAM_STR);

            $q->execute();

            $err = ($q !== false ? $q->errorInfo() : $this->pdo->errorInfo());

            if ($err[0] !== '00000') {
                throw new \PDOException("Failed to retrieve access enforcement for given role. ERROR: " . $err[2]);
            }

            if ($q->rowCount() > 0) {
                $controls = array_merge($controls, $q->fetchAll(\PDO::FETCH_ASSOC));
                $byRole = true;
            }

            $q = $this->pdo->prepare("SELECT `res_anc`.`rt` AS `right`, `res_anc`.`alias` AS `resource`, `act`.`alias` AS `action`, `tag`.`name` AS `tag`, `con`.`allowed` AS `access` " .
                    "FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` " .
                    "LEFT JOIN `" . static::TCONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` " .
                    "LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` " .
                    "LEFT JOIN `" . static::TAG_TABLE . "` AS `tag` ON `con`.`tag_id` = `tag`.`id` " .
                    "WHERE `res`.`alias` = :resource AND `act`.`alias` = :action AND `tag`.`id` IN (" .
                    "SELECT `tag_sub`.`id` AS `id` FROM `" . static::ROLE_TAG_TABLE . "` AS `rtag_sub` INNER JOIN `" . static::ROLE_TABLE . "` AS `rol_sub` ON `rtag_sub`.`role_id` = `rol_sub`.`id` INNER JOIN `" . static::TAG_TABLE . "` AS `tag_sub` ON `rtag_sub`.`tag_id` = `tag_sub`.`id`  WHERE `rol_sub`.`alias`=:role" .
                    ") " .
                    "ORDER BY `res_anc`.`rt` DESC;");

            $q->bindValue(':resource', $resource, \PDO::PARAM_STR);
            $q->bindValue(':action', $action, \PDO::PARAM_STR);
            $q->bindValue(':role', $role, \PDO::PARAM_STR);

            $q->execute();

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
            $q = $this->pdo->prepare("SELECT `res_anc`.`alias` AS `resource`, `act`.`alias` AS `action`, `rol`.`alias` AS `role`, `con`.`allowed` AS `access` 
                FROM `" . static::RESOURCE_TABLE . "` AS `res` JOIN `" . static::RESOURCE_TABLE . "` as `res_anc` ON `res`.`lt` BETWEEN `res_anc`.`lt` AND `res_anc`.`rt` 
                LEFT JOIN `" . static::CONTROL_TABLE . "` as `con` ON `con`.`resource_id` = `res_anc`.`id` 
                LEFT JOIN `" . static::ACTION_TABLE . "` AS `act` ON `act`.`id` = `con`.`action_id` 
                LEFT JOIN `" . static::ROLE_TABLE . "` AS `rol` ON `con`.`role_id` = `rol`.`id` 
                WHERE `res`.`alias` = :resource AND `act`.`alias` = :action AND `rol`.`id` IN 
                (
                SELECT `rol_anc_sub`.`id` AS `id`
                FROM `" . static::ROLE_TABLE . "` AS `rol_sub` JOIN `" . static::ROLE_TABLE . "` as `rol_anc_sub` ON `rol_sub`.`lt` BETWEEN `rol_anc_sub`.`lt` AND `rol_anc_sub`.`rt` 
                WHERE `rol_sub`.`alias` = :role 
                ORDER BY `rol_anc_sub`.`rt` DESC
                )
                ORDER BY `res_anc`.`rt` DESC;"
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
