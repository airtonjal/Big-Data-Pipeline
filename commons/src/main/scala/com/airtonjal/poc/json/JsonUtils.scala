package com.airtonjal.poc.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{NullNode, ArrayNode, ObjectNode}
import org.slf4j.LoggerFactory

/**
 * Utilities class to handle JSON manipulation
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object JsonUtils {

  @transient private val log = LoggerFactory.getLogger(getClass)

  /**
   * Sets a given child with a [[JsonNode]]
   * @param root The parent [[JsonNode]]
   * @param node The [[JsonNode]] to be inserted
   * @param name The name of the node
   * @param forceReplace <code>false</code> to fail if a node with the given name already exists,  <code>true</code>
   *                     to replace it anyway
   */
  private def set(root: JsonNode, node: JsonNode, name: String, forceReplace: Boolean): Unit = {
    if (!forceReplace && root.has(name)) throw new IllegalArgumentException("root node " + root + " already contains child " + name)

    getObj(root) match {
      case Left(obj) => obj.set(name, node)
      case Right(array) => throw new IllegalArgumentException("root parameter is ArrayNode, but ObjectNode was expected. Use addToArray method")
    }
  }

  /**
   * Adds a node to the hierarchy if it does not exist
   * @param root The root [[JsonNode]]
   * @param node The [[JsonNode]] to be added
   * @param path The name of the node
   * @throws IllegalArgumentException Thrown if the name already exists in root
   */
  def add(root: JsonNode, node: JsonNode, path: String): Unit = {
    getParent(root, path) match {
      case Some(foundNode) => this.set(foundNode, node, getName(path), false)
      case _ =>
    }
  }

  /**
   * Adds a [[JsonNode]] element to an [[ArrayNode]]
   * @param root The root [[JsonNode]]. Must be a [[ArrayNode]] for the method to properly work
   * @param node The [[JsonNode]] to be added
   */
  def addToArray(root: JsonNode, node: JsonNode): Unit = {
    root match {
      case arrayNode: ArrayNode => arrayNode.add(node)
      case _ => log.debug("node " + root + " does not correspond to an ArrayNode object")
    }
  }

  /**
   * Adds a [[JsonNode]] element to an [[ArrayNode]]
   * @param root The root [[JsonNode]]
   * @param node The [[JsonNode]] to be added
   * @param path The path to search for the [[ArrayNode]]
   */
  def addToArray(root: JsonNode, node: JsonNode, path: Option[String]): Unit = {
    findNode(root, path) match {
      case Some(found) => addToArray(found, node)
      case None => log.debug("Could not find node " + path + " in node " + root)
    }

  }

  /**
   * Copies the contents of a given [[JsonNode]] to another node
   * @param from The [[JsonNode]] to acquire data from
   * @param to The [[JsonNode]] to inject data
   * @param oldPath The path to find the node inside root. Split by the '.' char
   * @param newPath The path of the node to be copied to. Split by the '.' char
   * @throws IllegalArgumentException Thrown if the name already exists in root
   */
  def copy(from: JsonNode, to: JsonNode, oldPath: Option[String], newPath: String): Unit = {
    // Finds a node in the hierarchy
    findNode(from, oldPath) match {
      case Some(node) => {
        val destNames = newPath.split("\\.")
        var destNode = to

        for(name <- destNames.slice(0, destNames.length - 1)) {
          if (destNode.isObject) destNode = destNode.`with`(name)
          else return  // Found a leaf, array or null node
        }

        set(destNode, node, destNames.last, forceReplace = false)
      }
      case None =>
    }
  }

  /**
   * Removes a node from the hierarchy
   * @param root The root [[JsonNode]]
   * @param path The path of the [[JsonNode]] to be removed. Split by the '.' char
   * @return The [[JsonNode]] removed. Might be a missingNode
   */
  def remove(root: JsonNode, path: String): Option[JsonNode] = {
    val childName = getName(path)
    getParent(root, path) match {
      case Some(parent) =>
        if(parent.has(childName)) Some(parent.remove(childName))
        else None
      case None => None
    }
  }

  /**
   * Finds a [[JsonNode]] in the hierarchy
   * @param root The root [[JsonNode]]
   * @param path The path of the [[JsonNode]] to be found. Split by the '.' char
   * @return The [[JsonNode]] if it exists
   */
  def findNode(root: JsonNode, path: Option[String]): Option[JsonNode] = {
    path match {
      case Some(pathStr) => {
        val nodeNames = pathStr.split("\\.")
        var newNode = root

        for(name <- nodeNames) {
          newNode = newNode.path(name)
          if (newNode.isMissingNode) return None
        }

        Some(newNode)
      }
      case None => Some(root)
    }
  }

  /**
   * Checks if a given node exists
   * @param root The root [[JsonNode]]
   * @param path The path to search for the [[JsonNode]]. Split by the '.' char
   * @return <code>true</code> if the node exists, <code>false</code> otherwise
   */
  def exists(root: JsonNode, path: Option[String]): Boolean = {
    val node = findNode(root, path)
    node.isDefined
  }

  /**
   * Gets the penultimate node on the path trail
   * @param node The [[JsonNode]]
   * @param path The path
   * @return The [[JsonNode]] parent of the last node
   */
  private def getParent(node: JsonNode, path: String): Option[ObjectNode] = {
    val nodeNames = path.split("\\.")
    var parent = node

    for(name <- nodeNames.slice(0, nodeNames.size - 1)) {
      parent = parent.get(name)
      if (parent == null || parent.isMissingNode || parent.isInstanceOf[NullNode]) return None
    }

    parent match {
      case parent: ObjectNode => Some(parent)
      case _ => None
    }
  }

  /**
   * Gets the parent and child nodes, if they exist
   * @param node The parent [[JsonNode]]
   * @param path The path to find the child. Split by the '.' char
   * @return A tuple of parent and child, if both exists
   */
  private def getParentAndChild(node: JsonNode, path: String): Option[(JsonNode, JsonNode)] = {
    val nodeNames = path.split("\\.")
    getParent(node, path) match {
      case Some(parent) => {
        val child = parent.get(nodeNames.last)

        if (child == null || child.isMissingNode) None
        else Some(parent, child)
      }
      case None => None
    }
  }

  private def getName(path: String) = path.split("\\.").last

  private def getObj(node: JsonNode): Either[ObjectNode, ArrayNode] = {
    node match {
      case s: ObjectNode => Left(s.asInstanceOf[ObjectNode])
      case a: ArrayNode => Right(a.asInstanceOf[ArrayNode])
      case _ => throw new InternalError("Wrong assumption, JsonNode is not an ObjectNode")
    }
  }

}
