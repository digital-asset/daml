// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import cats.syntax.either.*
import com.digitalasset.canton.platform.apiserver.update.UpdatePathsTrie.MatchResult
import com.google.protobuf.field_mask.FieldMask

trait UpdateMapperBase {

  type Resource
  type Update

  /** A trie containing all update paths. Used for validating the input update mask paths.
    */
  def fullResourceTrie: UpdatePathsTrie

  protected[update] def makeUpdateObject(
      apiObject: Resource,
      updateTrie: UpdatePathsTrie,
  ): Result[Update]

  /** Validates its input and produces an update object.
    * NOTE: The return update object might represent an empty (no-op) update.
    *
    * @param apiObject       represents the new values for the update
    * @param updateMask indicates which fields should get updated
    */
  final def toUpdate(
      apiObject: Resource,
      updateMask: FieldMask,
  ): Result[Update] =
    for {
      updateTrie <- makeUpdateTrie(updateMask)
      updateObject <- makeUpdateObject(apiObject, updateTrie)
    } yield {
      updateObject
    }

  private def makeUpdateTrie(updateMask: FieldMask): Result[UpdatePathsTrie] =
    for {
      _ <- Either.cond(updateMask.paths.nonEmpty, (), UpdatePathError.EmptyUpdateMask)
      parsedPaths <- UpdatePath.parseAll(updateMask.paths)
      _ <- validatePathsMatchValidFields(parsedPaths)
      updateTrie <- UpdatePathsTrie.fromPaths(parsedPaths)
    } yield updateTrie

  protected[update] final def noUpdate[A]: Result[Option[A]] = Right(None)

  protected[update] final def validatePathsMatchValidFields(
      paths: Seq[UpdatePath]
  ): Result[Unit] =
    paths.foldLeft(Either.unit[UpdatePathError]) { (ax, parsedPath) =>
      for {
        _ <- ax
        _ <- Either.cond(
          fullResourceTrie.containsPrefix(parsedPath.fieldPath),
          (),
          UpdatePathError.UnknownFieldPath(parsedPath.toRawString),
        )
      } yield ()
    }

  protected[update] final def makeAnnotationsUpdate(
      updateMatch: MatchResult,
      newValue: Map[String, String],
  ): Result[Option[Map[String, String]]] = {
    val isDefaultValue = newValue == Map.empty
    def some = Right(Some(newValue))
    if (updateMatch.isExact) {
      some
    } else {
      if (isDefaultValue) noUpdate else some
    }
  }

  protected[update] final def makePrimitiveFieldUpdate[A](
      updateMatch: MatchResult,
      defaultValue: A,
      newValue: A,
  ): Result[Option[A]] = {
    val isDefaultValue = newValue == defaultValue
    val some = Right(Some(newValue))
    if (updateMatch.isExact) {
      some
    } else {
      if (isDefaultValue) noUpdate else some
    }
  }
}
