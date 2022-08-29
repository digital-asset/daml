// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.ledger.api.domain.{ParticipantParty, User}
import com.daml.ledger.api.v1.admin.{user_management_service => proto_um}
import com.daml.ledger.api.v1.admin.{party_management_service => proto_pm}
import com.daml.ledger.api.v1.admin.{object_meta => proto_om}
import com.daml.ledger.participant.state.index.v2.MyFieldMaskUtils.{
  fieldNameForNumber,
  fieldNameForNumber2,
}
import com.daml.ledger.participant.state.index.v2.{
  AnnotationsUpdate,
  ObjectMetaUpdate,
  PartyRecordUpdate,
  UpdateMaskTrie_mut,
  UpdateModifier,
  UpdatePath,
  UserUpdate,
}
import com.google.protobuf.field_mask.FieldMask

// TODO pbatko: Cleanup the code
// TODO pbatko: Map from api.User directly?
// TODO pbatko: Consider doing 'merge' update of annotations map by default, with an option to do a 'replace-all'.
// TODO         It is what FieldMask's doc suggest.
object UpdateMapper {

  def parseUpdateMask2(updateMask: FieldMask): Seq[UpdatePath] = {

    updateMask.paths.map { updatePath =>
      val (path, updateSemantics) = updatePath.split('!') match {
        case Array(path, "merge") => (path, UpdateModifier.Merge)
        case Array(path, "replace") => (path, UpdateModifier.Replace)
        case Array(path) => (path, UpdateModifier.Merge)
        case other => sys.error(s"TODO invalid update path ${other.mkString("[", ",", "]")}")
      }
      UpdatePath(
        path = path.split('.').toSeq,
        updateSemantics = updateSemantics,
      )
    }
  }

  private def parseUpdateMask(updateMaskPaths: Seq[String]): Seq[Seq[String]] = {
    updateMaskPaths.map { path =>
      path.split('.').toSeq
    }
  }

  /** A field (e.g 'foo.bar.baz') is set to be updated if in the update mask:
    * - its exact path is specified (e.g. 'foo.bar.baz'),
    * - a prefix of its exact path is specified (e.g. 'foo.bar' or 'foo') and it's new value is non-default.
    *
    * Corollary: To delete a field (i.e. set it to a default value) you need to specify its exact path.
    */
  def toUserUpdate(user: User, updateMask: FieldMask): UserUpdate = {
    val paths: Seq[String] = updateMask.paths

    val annotationsPath: String = Seq(
      fieldNameForNumber2(proto_um.UpdateUserRequest)(_.USER_FIELD_NUMBER),
      fieldNameForNumber2(proto_um.User)(_.METADATA_FIELD_NUMBER),
      fieldNameForNumber2(proto_om.ObjectMeta)(_.ANNOTATIONS_FIELD_NUMBER),
    ).mkString(".")
    val annotationsPathMerge = `annotationsPath` + "!merge"
    val annotationsPathReplace = `annotationsPath` + "!replace"

    var replaceAnnotations = false
    var explicitMerge = false
    val paths2 = paths.map {
      case `annotationsPathMerge` => {
        explicitMerge = true
        annotationsPath
      }
      case `annotationsPathReplace` => {
        replaceAnnotations = true
        annotationsPath
      }
      case o => o
    }

    val trie = UpdateMaskTrie_mut.fromPaths(parseUpdateMask(paths2))
    val userSubTrieO: Option[UpdateMaskTrie_mut] =
      trie.subtree(
        Seq(
          fieldNameForNumber[proto_um.UpdateUserRequest](
            proto_um.UpdateUserRequest.USER_FIELD_NUMBER
          )
        )
      )

    if (userSubTrieO.isEmpty) {
      // TODO pbatko: This is an empty (no-op) update so consider modeling it as None
      UserUpdate(
        id = user.id,
        primaryPartyUpdateO = None,
        isDeactivatedUpdateO = None,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = user.metadata.resourceVersionO,
          annotationsUpdateO = None,
        ),
      )
    } else {
      val userSubTrie = userSubTrieO.get

      val annotationsUpdate: Option[AnnotationsUpdate] = userSubTrie
        .determineUpdate[Map[String, String]](
          Seq(
            fieldNameForNumber[proto_um.User](proto_um.User.METADATA_FIELD_NUMBER),
            fieldNameForNumber[proto_om.ObjectMeta](
              proto_om.ObjectMeta.ANNOTATIONS_FIELD_NUMBER
            ),
          )
        )(
          newValueCandidate = user.metadata.annotations,
          defaultValue = Map.empty,
        )
        .map { updateValue =>
          if (replaceAnnotations) {
            AnnotationsUpdate.Replace(updateValue)
          } else {
            if (explicitMerge) {
              AnnotationsUpdate.Merge
                .apply(updateValue)
                .getOrElse(
                  sys.error("TODO tried to do merge update with the default value!")
                )
            } else {
              if (updateValue.isEmpty) {
                AnnotationsUpdate.Replace(updateValue)
              } else {
                AnnotationsUpdate.Merge.fromNonEmpty(updateValue)
              }
            }
          }
        }

      UserUpdate(
        id = user.id,
        primaryPartyUpdateO = userSubTrie.determineUpdate(
          Seq(
            fieldNameForNumber[proto_um.User](proto_um.User.PRIMARY_PARTY_FIELD_NUMBER)
          )
        )(
          newValueCandidate = user.primaryParty,
          defaultValue = None,
        ),
        isDeactivatedUpdateO = userSubTrie.determineUpdate(
          Seq(
            fieldNameForNumber[proto_um.User](proto_um.User.IS_DEACTIVATED_FIELD_NUMBER)
          )
        )(
          newValueCandidate = user.isDeactivated,
          defaultValue = false,
        ),
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = user.metadata.resourceVersionO,
          annotationsUpdateO = annotationsUpdate,
        ),
      )
    }
  }

  // TODO pbatko: Validate correct tries: Construct the fullest tree and implement isRootedSubtree operation

  def toParticipantPartyUpdate(
      partyRecord: ParticipantParty.PartyRecord,
      updateMask: FieldMask,
  ): PartyRecordUpdate = {

    val paths: Seq[String] = updateMask.paths

    val annotationsPath = Seq(
      fieldNameForNumber2(proto_pm.UpdatePartyDetailsRequest)(_.PARTY_DETAILS_FIELD_NUMBER),
      fieldNameForNumber2(proto_pm.PartyDetails)(_.LOCAL_METADATA_FIELD_NUMBER),
      fieldNameForNumber2(proto_om.ObjectMeta)(_.ANNOTATIONS_FIELD_NUMBER),
    ).mkString(".")
    val annotationsPathMerge = `annotationsPath` + "!merge"
    val annotationsPathReplace = `annotationsPath` + "!replace"

    var replaceAnnotations = false
    var explicitMerge = false
    val paths2 = paths.map {
      case `annotationsPathMerge` => {
        explicitMerge = true
        annotationsPath
      }
      case `annotationsPathReplace` => {
        replaceAnnotations = true
        annotationsPath
      }
      case o => o
    }

    val trie = UpdateMaskTrie_mut.fromPaths(parseUpdateMask(paths2))
    val userSubTrieO: Option[UpdateMaskTrie_mut] =
      trie.subtree(
        Seq(fieldNameForNumber2(proto_pm.UpdatePartyDetailsRequest)(_.PARTY_DETAILS_FIELD_NUMBER))
      )

    if (userSubTrieO.isEmpty) {
      // TODO pbatko: This is an empty (no-op) update so consider modeling it as None
      PartyRecordUpdate(
        party = partyRecord.party,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = partyRecord.metadata.resourceVersionO,
          annotationsUpdateO = None,
        ),
      )
    } else {
      val userSubTrie = userSubTrieO.get

      val annotationsUpdate: Option[AnnotationsUpdate] = userSubTrie
        .determineUpdate[Map[String, String]](
          Seq(
            fieldNameForNumber2(proto_pm.PartyDetails)(_.LOCAL_METADATA_FIELD_NUMBER),
            fieldNameForNumber2(proto_om.ObjectMeta)(_.ANNOTATIONS_FIELD_NUMBER),
          )
        )(
          newValueCandidate = partyRecord.metadata.annotations,
          defaultValue = Map.empty[String, String],
        )
        .map { updateValue: Map[String, String] =>
          val x: AnnotationsUpdate = if (replaceAnnotations) {
            AnnotationsUpdate.Replace(updateValue)
          } else {
            if (explicitMerge) {
              AnnotationsUpdate.Merge
                .apply(updateValue)
                .getOrElse(
                  sys.error("TODO tried to do merge update with the default value!")
                )
            } else {
              if (updateValue.isEmpty) {
                AnnotationsUpdate.Replace(updateValue)
              } else {
                AnnotationsUpdate.Merge.fromNonEmpty(updateValue)
              }
            }
          }
          x
        }

      PartyRecordUpdate(
        party = partyRecord.party,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = partyRecord.metadata.resourceVersionO,
          annotationsUpdateO = annotationsUpdate,
        ),
      )
    }
  }
}
