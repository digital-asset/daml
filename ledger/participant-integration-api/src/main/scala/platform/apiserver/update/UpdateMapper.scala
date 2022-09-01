// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.api.domain.{ParticipantParty, User}
import com.daml.ledger.participant.state.index.v2._
import com.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import com.daml.platform.apiserver.update.UpdatePathsTrie.MatchResult

object UpdateMapper {

  private val userAnnotationsPath = List(
    FieldNames.UpdateUserRequest.user,
    FieldNames.User.metadata,
    FieldNames.Metadata.annotations,
  )
  private val userPrimaryPartyPath =
    List(FieldNames.UpdateUserRequest.user, FieldNames.User.primaryParty)
  private val userIsDeactivatePath =
    List(FieldNames.UpdateUserRequest.user, FieldNames.User.isDeactivated)

  private val fullUpdateUserTree = UpdatePathsTrie
    .fromPaths(
      Seq(
        UpdatePath(userAnnotationsPath, modifier = UpdatePathModifier.NoModifier),
        UpdatePath(userPrimaryPartyPath, modifier = UpdatePathModifier.NoModifier),
        UpdatePath(userIsDeactivatePath, modifier = UpdatePathModifier.NoModifier),
      )
    )
    .getOrElse(sys.error("Failed to create full update user tree. This should never happen"))

  private def noUpdate[A]: Result[Option[A]] = Right(None)

  /** Validates its input and produces an update object.
    * NOTE: The return update object might represent an empty (no-op) update.
    *
    * @param user represents the new values for the update
    * @param updateMask indicates which fields should get updated
    */
  def toUserUpdate(user: User, updateMask: FieldMask): Result[UserUpdate] = {

    def validatePathsMatchValidFields(paths: Seq[UpdatePath]): Result[Unit] =
      paths.foldLeft[Result[Unit]](Right(())) { (ax, parsedPath) =>
        for {
          _ <- ax
          _ <-
            if (!fullUpdateUserTree.containsPrefix(parsedPath.fieldPath)) {
              Left(UpdatePathError.UnknownFieldPath(parsedPath.toRawString))
            } else Right(())
        } yield ()
      }
    def resolveAnnotationsUpdate(updateTrie: UpdatePathsTrie): Result[Option[AnnotationsUpdate]] =
      updateTrie
        .findMatch(userAnnotationsPath)
        .fold(noUpdate[AnnotationsUpdate])(updateMatch =>
          makeAnnotationsUpdate(newValue = user.metadata.annotations, updateMatch = updateMatch)
        )
    def resolvePrimaryPartyUpdate(updateTrie: UpdatePathsTrie): Result[Option[Option[Ref.Party]]] =
      updateTrie
        .findMatch(userPrimaryPartyPath)
        .fold(noUpdate[Option[Ref.Party]])(updateMatch =>
          makePrimitiveFieldUpdate(
            updateMatch = updateMatch,
            defaultValue = None,
            newValue = user.primaryParty,
          )
        )
    def isDeactivatedUpdateResult(updateTrie: UpdatePathsTrie): Result[Option[Boolean]] =
      updateTrie
        .findMatch(userIsDeactivatePath)
        .fold(noUpdate[Boolean])(matchResult =>
          makePrimitiveFieldUpdate(
            updateMatch = matchResult,
            defaultValue = false,
            newValue = user.isDeactivated,
          )
        )
    for {
      _ <- if (updateMask.paths.isEmpty) Left(UpdatePathError.EmptyFieldMask) else Right(())
      parsedPaths <- UpdatePath.parseAll(updateMask.paths)
      _ <- validatePathsMatchValidFields(parsedPaths)
      updateTrie <- UpdatePathsTrie.fromPaths(parsedPaths)
      annotationsUpdate <- resolveAnnotationsUpdate(updateTrie)
      primaryPartyUpdate <- resolvePrimaryPartyUpdate(updateTrie)
      isDeactivatedUpdate <- isDeactivatedUpdateResult(updateTrie)
    } yield {
      UserUpdate(
        id = user.id,
        primaryPartyUpdateO = primaryPartyUpdate,
        isDeactivatedUpdateO = isDeactivatedUpdate,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = user.metadata.resourceVersionO,
          annotationsUpdateO = annotationsUpdate,
        ),
      )
    }
  }

  // TODO um-for-hub major: Implement me
  def toParticipantPartyUpdate(
      partyRecord: ParticipantParty.PartyRecord,
      updateMask: FieldMask,
  ): PartyRecordUpdate = {
    ???
  }

  private def makeAnnotationsUpdate(
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

  private def makePrimitiveFieldUpdate[A](
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
              UpdatePathError.MergeUpdateModifierOnPrimitiveField(
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
