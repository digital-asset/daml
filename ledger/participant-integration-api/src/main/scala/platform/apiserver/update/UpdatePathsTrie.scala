// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import collection.mutable
import scala.annotation.tailrec
import scala.collection.immutable.SortedMap

object UpdatePathsTrie {
  case class MatchResult(
      isExact: Boolean,
      matchedPath: UpdatePath,
  ) {
    def getUpdatePathModifier: UpdatePathModifier = matchedPath.modifier
  }

  def apply(
      modifierO: Option[UpdatePathModifier],
      nodes: (String, UpdatePathsTrie)*
  ) = new UpdatePathsTrie(
    nodes = SortedMap.from(nodes),
    modifierO = modifierO,
  )

  def fromPaths(paths: Seq[UpdatePath]): Result[UpdatePathsTrie] = {
    val builder: Result[Builder] = Right(Builder(None))
    val buildResult = paths.foldLeft(builder)((builderResult, path) =>
      for {
        b <- builderResult
        _ <- b.insertUniquePath(path)
      } yield b
    )
    buildResult.map(build)
  }

  private def build(builder: Builder): UpdatePathsTrie = {
    new UpdatePathsTrie(
      modifierO = builder.modifierO,
      nodes = SortedMap.from(builder.nodes.view.mapValues(build)),
    )
  }

  private object Builder {
    def apply(
        modifierO: Option[UpdatePathModifier],
        nodes: (String, Builder)*
    ) = new Builder(
      nodes = mutable.SortedMap.from(nodes),
      modifierO = modifierO,
    )
  }

  private case class Builder(
      nodes: mutable.SortedMap[String, Builder],
      var modifierO: Option[UpdatePathModifier],
  ) {

    /** @param updatePath unique path to be inserted
      */
    def insertUniquePath(updatePath: UpdatePath): Result[Unit] = {
      if (!doInsertUniquePath(updatePath.fieldPath, updatePath.modifier)) {
        Left(UpdatePathError.DuplicatedFieldPath(updatePath.toRawString))
      } else Right(())
    }

    /** @return true if successfully inserted the field path, false if the field path was already present in this trie
      */
    @tailrec
    private def doInsertUniquePath(
        fieldPath: List[String],
        modifier: UpdatePathModifier,
    ): Boolean = {
      fieldPath match {
        case Nil =>
          if (this.modifierO.nonEmpty) {
            false
          } else {
            this.modifierO = Some(modifier)
            true
          }
        case key :: subpath =>
          if (!nodes.contains(key)) {
            val empty = new Builder(nodes = mutable.SortedMap.empty, modifierO = None)
            nodes.put(key, empty)
          }
          nodes(key).doInsertUniquePath(subpath, modifier)
      }
    }

  }
}

/** Data structure for storing and querying update paths.
  *
  * Each update path specifies:
  * - a field path corresponding to a field of an update request proto message,
  * - an update modifier.
  *
  * See also [[com.google.protobuf.field_mask.FieldMask]]).
  *
  * @param modifierO - non empty to signify a path ending in this node exists
  */
private[update] case class UpdatePathsTrie(
    nodes: SortedMap[String, UpdatePathsTrie],
    modifierO: Option[UpdatePathModifier],
) {
  import UpdatePathsTrie._

  /** @return true if 'path' matches some prefix of some field path
    */
  @tailrec
  final def containsPrefix(path: List[String]): Boolean = {
    path match {
      case Nil => true
      case head :: rest if nodes.contains(head) => nodes(head).containsPrefix(rest)
      case _ => false
    }
  }

  /** There is a match if this trie contains 'path' or if it contains a prefix of 'path'.
    * @return the match corresponding to the longest matched field path, none otherwise.
    */
  def findMatch(path: List[String]): Option[MatchResult] = {
    findPath(path) match {
      case Some(modifier) =>
        Some(MatchResult(isExact = true, matchedPath = UpdatePath(path, modifier)))
      case None =>
        val properPrefixesLongestFirst =
          path.inits.filter(init => init.size != path.size).toList.sortBy(-_.length)
        properPrefixesLongestFirst.iterator
          .map(prefix => findPath(prefix).map(_ -> prefix))
          .collectFirst { case Some((modifier, prefix)) =>
            MatchResult(isExact = false, matchedPath = UpdatePath(prefix, modifier))
          }
    }
  }

  /** @return an update modifier of a matching field path, none if there is no matching field path
    */
  @tailrec
  final private[update] def findPath(path: List[String]): Option[UpdatePathModifier] = {
    path match {
      case Nil => this.modifierO
      case head :: subpath if nodes.contains(head) => nodes(head).findPath(subpath)
      case _ => None
    }
  }

}
