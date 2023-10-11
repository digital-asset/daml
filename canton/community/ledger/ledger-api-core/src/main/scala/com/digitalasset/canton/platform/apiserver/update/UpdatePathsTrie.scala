// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.digitalasset.canton.DiscardOps

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap

import collection.mutable

object UpdatePathsTrie {
  final case class MatchResult(
      isExact: Boolean,
      matchedPath: UpdatePath,
  )

  def apply(
      exists: Boolean,
      nodes: (String, UpdatePathsTrie)*
  ) = new UpdatePathsTrie(
    exists = exists,
    nodes = SortedMap.from(nodes),
  )

  def fromPaths(
      paths: Seq[List[String]]
  )(implicit dummy: DummyImplicit): Result[UpdatePathsTrie] = {
    fromPaths(paths.map(UpdatePath(_)))
  }

  def fromPaths(paths: Seq[UpdatePath]): Result[UpdatePathsTrie] = {
    val builder: Result[Builder] = Right(Builder(exists = false))
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
      exists = builder.exists,
      nodes = SortedMap.from(builder.nodes.view.mapValues(build)),
    )
  }

  private object Builder {
    def apply(
        exists: Boolean,
        nodes: (String, Builder)*
    ) = new Builder(
      exists = exists,
      nodes = mutable.SortedMap.from(nodes),
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final case class Builder(
      nodes: mutable.SortedMap[String, Builder],
      var exists: Boolean = false,
  ) {

    /** @param updatePath unique path to be inserted
      */
    def insertUniquePath(updatePath: UpdatePath): Result[Unit] = {
      if (!doInsertUniquePath(updatePath.fieldPath)) {
        Left(UpdatePathError.DuplicatedFieldPath(updatePath.toRawString))
      } else Right(())
    }

    /** @return true if successfully inserted the field path, false if the field path was already present in this trie
      */
    @tailrec
    private def doInsertUniquePath(
        fieldPath: List[String]
    ): Boolean = {
      fieldPath match {
        case Nil =>
          if (this.exists) {
            false
          } else {
            this.exists = true
            true
          }
        case key :: subpath =>
          if (!nodes.contains(key)) {
            val empty = new Builder(nodes = mutable.SortedMap.empty, exists = false)
            nodes.put(key, empty).discard
          }
          nodes(key).doInsertUniquePath(subpath)
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
  */
private[update] final case class UpdatePathsTrie(
    nodes: SortedMap[String, UpdatePathsTrie],
    exists: Boolean,
) {
  import UpdatePathsTrie.*

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
    if (pathExists(path)) {
      Some(MatchResult(isExact = true, matchedPath = UpdatePath(path)))
    } else {
      val properPrefixesLongestFirst =
        path.inits.filter(init => init.size != path.size).toList.sortBy(-_.length)
      properPrefixesLongestFirst.iterator
        .find(pathExists)
        .map { prefix =>
          MatchResult(isExact = false, matchedPath = UpdatePath(prefix))
        }
    }
  }

  /** @return an update modifier of a matching field path, none if there is no matching field path
    */
  @tailrec
  final private[update] def pathExists(path: List[String]): Boolean = {
    path match {
      case Nil => this.exists
      case head :: subpath if nodes.contains(head) => nodes(head).pathExists(subpath)
      case _ => false
    }
  }

}
