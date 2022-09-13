// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.participant.state.index.v2.AnnotationsUpdate
import com.daml.platform.apiserver.update.UpdatePathsTrie.MatchResult
import com.google.protobuf.field_mask.FieldMask

trait UpdateMapperBase {

  type DomainObject
  type UpdateObject

  /** A trie containing all update paths. Used for validating the input update mask paths.
    */
  def fullUpdateTrie: UpdatePathsTrie

  protected[update] def makeUpdateObject(
      domainObject: DomainObject,
      updateTrie: UpdatePathsTrie,
  ): Result[UpdateObject]

  /** Validates its input and produces an update object.
    * NOTE: The return update object might represent an empty (no-op) update.
    *
    * @param domainObject       represents the new values for the update
    * @param updateMask indicates which fields should get updated
    */
  final def toUpdate(
      domainObject: DomainObject,
      updateMask: FieldMask,
  ): Result[UpdateObject] = {
    for {
      updateTrie <- makeUpdateTrie(updateMask)
      updateObject <- makeUpdateObject(domainObject, updateTrie)
    } yield {
      updateObject
    }
  }

  private def makeUpdateTrie(updateMask: FieldMask): Result[UpdatePathsTrie] = {
    for {
      _ <- if (updateMask.paths.isEmpty) Left(UpdatePathError.EmptyFieldMask) else Right(())
      parsedPaths <- UpdatePath.parseAll(updateMask.paths)
      _ <- validatePathsMatchValidFields(parsedPaths)
      updateTrie <- UpdatePathsTrie.fromPaths(parsedPaths)
    } yield updateTrie
  }

  protected[update] final def noUpdate[A]: Result[Option[A]] = Right(None)

  protected[update] final def validatePathsMatchValidFields(
      paths: Seq[UpdatePath]
  ): Result[Unit] =
    paths.foldLeft[Result[Unit]](Right(())) { (ax, parsedPath) =>
      for {
        _ <- ax
        _ <-
          if (!fullUpdateTrie.containsPrefix(parsedPath.fieldPath)) {
            Left(UpdatePathError.UnknownFieldPath(parsedPath.toRawString))
          } else Right(())
      } yield ()
    }

  protected[update] final def makeAnnotationsUpdate(
      newValue: Map[String, String],
      updateMatch: MatchResult,
  ): Result[Option[AnnotationsUpdate]] = {
    val isDefaultValue = newValue.isEmpty
    val modifier = updateMatch.getUpdatePathModifier
    def mergeUpdate = Right(Some(AnnotationsUpdate.Merge.fromNonEmpty(newValue)))
    val replaceUpdate = Right(Some(AnnotationsUpdate.Replace(newValue)))
    if (updateMatch.isExact) {
      modifier match {
        case UpdatePathModifier.NoModifier =>
          if (isDefaultValue) replaceUpdate else mergeUpdate
        case UpdatePathModifier.Merge =>
          if (isDefaultValue)
            Left(
              UpdatePathError.MergeUpdateModifierOnEmptyMapField(
                updateMatch.matchedPath.toRawString
              )
            )
          else mergeUpdate
        case UpdatePathModifier.Replace =>
          replaceUpdate
      }
    } else {
      modifier match {
        case UpdatePathModifier.NoModifier | UpdatePathModifier.Merge =>
          if (isDefaultValue) noUpdate else mergeUpdate
        case UpdatePathModifier.Replace =>
          replaceUpdate
      }
    }
  }

  protected[update] final def makePrimitiveFieldUpdate[A](
      updateMatch: MatchResult,
      defaultValue: A,
      newValue: A,
  ): Result[Option[A]] = {
    val isDefaultValue = newValue == defaultValue
    val modifier = updateMatch.getUpdatePathModifier
    val some = Right(Some(newValue))
    if (updateMatch.isExact) {
      modifier match {
        case UpdatePathModifier.NoModifier | UpdatePathModifier.Replace => some
        case UpdatePathModifier.Merge =>
          if (isDefaultValue)
            Left(
              UpdatePathError.MergeUpdateModifierOnPrimitiveFieldWithDefaultValue(
                updateMatch.matchedPath.toRawString
              )
            )
          else some
      }
    } else {
      modifier match {
        case UpdatePathModifier.NoModifier | UpdatePathModifier.Merge =>
          if (isDefaultValue) noUpdate else some
        case UpdatePathModifier.Replace => some
      }
    }
  }
}
