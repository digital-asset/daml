// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
  LookupByKeyActionDescription,
}
import com.digitalasset.canton.data.ViewParticipantData.{InvalidViewParticipantData, RootAction}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
  SerializationCheckFailed,
}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{
  LfCommand,
  LfCreateCommand,
  LfExerciseByKeyCommand,
  LfExerciseCommand,
  LfFetchByKeyCommand,
  LfFetchCommand,
  LfLookupByKeyCommand,
  LfPackageId,
  LfPartyId,
  LfVersioned,
  ProtoDeserializationError,
  checked,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** Information concerning every '''participant''' involved in processing the underlying view.
  *
  * @param coreInputs  [[LfContractId]] used by the core of the view and not assigned by a Create node in the view or its subviews,
  *                    independently of whether the creation is rolled back.
  *                    Every contract id is mapped to its contract instances and their meta-information.
  *                    Contracts are marked as being [[InputContract.consumed]] iff
  *                    they are consumed in the core of the view.
  * @param createdCore associates contract ids of Create nodes in the core of the view to the corresponding contract
  *                instance. The elements are ordered in execution order.
  * @param createdInSubviewArchivedInCore
  *   The contracts that are created in subviews and archived in the core.
  *   The archival has the same rollback scope as the view.
  *   For [[com.digitalasset.canton.protocol.WellFormedTransaction]]s, the creation therefore is not rolled
  *   back either as the archival can only refer to non-rolled back creates.
  * @param resolvedKeys
  * Specifies how to resolve [[com.digitalasset.daml.lf.engine.ResultNeedKey]] requests from DAMLe (resulting from e.g., fetchByKey,
  * lookupByKey) when interpreting the view. The resolved contract IDs must be in the [[coreInputs]].
  * Stores only the resolution difference between this view's global key inputs
  * [[com.digitalasset.canton.data.TransactionView.globalKeyInputs]]
  * and the aggregated global key inputs from the subviews
  * (see [[com.digitalasset.canton.data.TransactionView.globalKeyInputs]] for the aggregation algorithm).
  * In [[com.digitalasset.daml.lf.transaction.ContractKeyUniquenessMode.Strict]],
  * the [[com.digitalasset.canton.data.FreeKey]] resolutions must be checked during conflict detection.
  * @param actionDescription The description of the root action of the view
  * @param rollbackContext The rollback context of the root action of the view.
  * @throws ViewParticipantData$.InvalidViewParticipantData
  * if [[createdCore]] contains two elements with the same contract id,
  * if [[coreInputs]]`(id).contractId != id`
  * if [[createdInSubviewArchivedInCore]] overlaps with [[createdCore]]'s ids or [[coreInputs]]
  * if [[coreInputs]] does not contain the resolved contract ids of [[resolvedKeys]]
  * if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
  * and the created id is not the first contract ID in [[createdCore]]
  * if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
  * or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[coreInputs]]
  * if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
  * and the key is not in [[resolvedKeys]].
  * @throws com.digitalasset.canton.serialization.SerializationCheckFailed if this instance cannot be serialized
  */
final case class ViewParticipantData private (
    coreInputs: Map[LfContractId, InputContract],
    createdCore: Seq[CreatedContract],
    createdInSubviewArchivedInCore: Set[LfContractId],
    resolvedKeys: Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]],
    actionDescription: ActionDescription,
    rollbackContext: RollbackContext,
    salt: Salt,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ViewParticipantData.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewParticipantData](hashOps)
    with HasProtocolVersionedWrapper[ViewParticipantData]
    with ProtocolVersionedMemoizedEvidence {
  {
    def requireDistinct[A](vals: Seq[A])(message: A => String): Unit = {
      val set = scala.collection.mutable.Set[A]()
      vals.foreach { v =>
        if (set(v)) throw InvalidViewParticipantData(message(v))
        else set += v
      }
    }

    val createdIds = createdCore.map(_.contract.contractId)
    requireDistinct(createdIds) { id =>
      val indices = createdIds.zipWithIndex.collect {
        case (createdId, idx) if createdId == id => idx
      }
      s"createdCore contains the contract id $id multiple times at indices ${indices.mkString(", ")}"
    }

    coreInputs.foreach { case (id, usedContract) =>
      if (id != usedContract.contractId)
        throw InvalidViewParticipantData(
          s"Inconsistent ids for used contract: $id and ${usedContract.contractId}"
        )

      if (createdInSubviewArchivedInCore.contains(id))
        throw InvalidViewParticipantData(
          s"Contracts created in a subview overlap with core inputs: $id"
        )
    }

    val transientOverlap = createdInSubviewArchivedInCore intersect createdIds.toSet
    if (transientOverlap.nonEmpty)
      throw InvalidViewParticipantData(
        s"Contract created in a subview are also created in the core: $transientOverlap"
      )

    def isAssignedKeyInconsistent(
        keyWithResolution: (LfGlobalKey, LfVersioned[SerializableKeyResolution])
    ): Boolean = {
      val (key, LfVersioned(_, resolution)) = keyWithResolution
      resolution.resolution.fold(false) { cid =>
        val inconsistent = for {
          inputContract <- coreInputs.get(cid)
          declaredKey <- inputContract.contract.metadata.maybeKey
        } yield declaredKey != key
        inconsistent.getOrElse(true)
      }
    }
    val keyInconsistencies = resolvedKeys.filter(isAssignedKeyInconsistent)

    if (keyInconsistencies.nonEmpty) {
      throw InvalidViewParticipantData(show"Inconsistencies for resolved keys: $keyInconsistencies")
    }
  }

  val rootAction: RootAction =
    actionDescription match {
      case CreateActionDescription(contractId, _seed) =>
        val createdContract = createdCore.headOption.getOrElse(
          throw InvalidViewParticipantData(
            show"No created core contracts declared for a view that creates contract $contractId at the root"
          )
        )
        if (createdContract.contract.contractId != contractId)
          throw InvalidViewParticipantData(
            show"View with root action Create $contractId declares ${createdContract.contract.contractId} as first created core contract."
          )
        val metadata = createdContract.contract.metadata
        val contractInst = createdContract.contract.rawContractInstance.contractInstance

        RootAction(
          LfCreateCommand(
            templateId = contractInst.unversioned.template,
            argument = contractInst.unversioned.arg,
          ),
          metadata.signatories,
          failed = false,
          packageIdPreference = Set.empty,
        )

      case ExerciseActionDescription(
            inputContractId,
            templateId,
            choice,
            interfaceId,
            packagePreference,
            chosenValue,
            actors,
            byKey,
            _seed,
            failed,
          ) =>
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Exercise root action is not declared as core input."
          ),
        )

        val cmd = if (byKey) {
          val key = inputContract.contract.metadata.maybeKey
            .map(_.key)
            .getOrElse(
              throw InvalidViewParticipantData(
                "Flag byKey set on an exercise of a contract without key."
              )
            )
          LfExerciseByKeyCommand(
            templateId = templateId,
            contractKey = key,
            choiceId = choice,
            argument = chosenValue.unversioned,
          )
        } else {
          LfExerciseCommand(
            templateId = templateId,
            interfaceId = interfaceId,
            contractId = inputContractId,
            choiceId = choice,
            argument = chosenValue.unversioned,
          )
        }
        RootAction(cmd, actors, failed, packagePreference)

      case fetch @ FetchActionDescription(
            inputContractId,
            actors,
            byKey,
            templateId,
            interfaceId,
          ) =>
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Fetch root action is not declared as core input."
          ),
        )

        val cmd = if (byKey) {
          val key = inputContract.contract.metadata.maybeKey
            .map(_.key)
            .getOrElse(
              throw InvalidViewParticipantData(
                "Flag byKey set on a fetch of a contract without key."
              )
            )
          LfFetchByKeyCommand(templateId = templateId, key = key)
        } else {
          LfFetchCommand(templateId = templateId, interfaceId = interfaceId, coid = inputContractId)
        }
        RootAction(cmd, actors, failed = false, packageIdPreference = fetch.packagePreference)

      case LookupByKeyActionDescription(LfVersioned(_version, key)) =>
        val LfVersioned(_, keyResolution) = resolvedKeys.getOrElse(
          key,
          throw InvalidViewParticipantData(
            show"Key $key of LookupByKey root action is not resolved."
          ),
        )
        val maintainers = keyResolution match {
          case AssignedKey(contractId) => checked(coreInputs(contractId)).maintainers
          case FreeKey(maintainers) => maintainers
        }

        RootAction(
          LfLookupByKeyCommand(templateId = key.templateId, contractKey = key.key),
          maintainers,
          failed = false,
          packageIdPreference = Set.empty,
        )
    }

  @transient override protected lazy val companionObj: ViewParticipantData.type =
    ViewParticipantData

  private[ViewParticipantData] def toProtoV30: v30.ViewParticipantData = v30.ViewParticipantData(
    coreInputs = coreInputs.values.map(_.toProtoV30).toSeq,
    createdCore = createdCore.map(_.toProtoV30),
    createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSeq.map(_.toProtoPrimitive),
    resolvedKeys = resolvedKeys.toList.map(ResolvedKey.fromPair(_).toProtoV30),
    actionDescription = Some(actionDescription.toProtoV30),
    rollbackContext = if (rollbackContext.isEmpty) None else Some(rollbackContext.toProtoV30),
    salt = Some(salt.toProtoV30),
  )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.ViewParticipantData

  override protected def pretty: Pretty[ViewParticipantData] = prettyOfClass(
    paramIfNonEmpty("core inputs", _.coreInputs),
    paramIfNonEmpty("created core", _.createdCore),
    paramIfNonEmpty("created in subview, archived in core", _.createdInSubviewArchivedInCore),
    paramIfNonEmpty("resolved keys", _.resolvedKeys),
    param("action description", _.actionDescription),
    param("rollback context", _.rollbackContext),
    param("salt", _.salt),
  )

  /** Extends [[resolvedKeys]] with the maintainers of assigned keys */
  val resolvedKeysWithMaintainers: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]] =
    resolvedKeys.fmap(_.map {
      case AssignedKey(contractId) =>
        val maintainers =
          // checked by `inconsistentAssignedKey` above
          checked(
            coreInputs.getOrElse(
              contractId,
              throw InvalidViewParticipantData(
                s"No input contract $contractId for a resolved key found"
              ),
            )
          ).maintainers
        AssignedKeyWithMaintainers(contractId, maintainers)
      case free @ FreeKey(_) => free
    })

  @VisibleForTesting
  def copy(
      coreInputs: Map[LfContractId, InputContract] = this.coreInputs,
      createdCore: Seq[CreatedContract] = this.createdCore,
      createdInSubviewArchivedInCore: Set[LfContractId] = this.createdInSubviewArchivedInCore,
      resolvedKeys: Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]] = this.resolvedKeys,
      actionDescription: ActionDescription = this.actionDescription,
      rollbackContext: RollbackContext = this.rollbackContext,
      salt: Salt = this.salt,
  ): ViewParticipantData =
    ViewParticipantData(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      resolvedKeys,
      actionDescription,
      rollbackContext,
      salt,
    )(hashOps, representativeProtocolVersion, None)
}

object ViewParticipantData
    extends VersioningCompanionContextMemoization[ViewParticipantData, HashOps] {
  override val name: String = "ViewParticipantData"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.ViewParticipantData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  /** Creates a view participant data.
    *
    * @throws InvalidViewParticipantData
    * if [[ViewParticipantData.createdCore]] contains two elements with the same contract id,
    * if [[ViewParticipantData.coreInputs]]`(id).contractId != id`
    * if [[ViewParticipantData.createdInSubviewArchivedInCore]] overlaps with [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]]
    * if [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in [[ViewParticipantData.resolvedKeys]]
    * if [[ViewParticipantData.createdCore]] creates a contract with a key that is not in [[ViewParticipantData.resolvedKeys]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
    * and the created id is not the first contract ID in [[ViewParticipantData.createdCore]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
    * or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[ViewParticipantData.coreInputs]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
    * and the key is not in [[ViewParticipantData.resolvedKeys]].
    * @throws com.digitalasset.canton.serialization.SerializationCheckFailed if this instance cannot be serialized
    */
  @throws[SerializationCheckFailed[com.digitalasset.daml.lf.value.ValueCoder.EncodeError]]
  def tryCreate(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ViewParticipantData =
    ViewParticipantData(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      resolvedKeys,
      actionDescription,
      rollbackContext,
      salt,
    )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)

  /** Creates a view participant data.
    *
    * Yields `Left(...)`
    * if [[ViewParticipantData.createdCore]] contains two elements with the same contract id,
    * if [[ViewParticipantData.coreInputs]]`(id).contractId != id`
    * if [[ViewParticipantData.createdInSubviewArchivedInCore]] overlaps with [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]]
    * if [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in [[ViewParticipantData.resolvedKeys]]
    * if [[ViewParticipantData.createdCore]] creates a contract with a key that is not in [[ViewParticipantData.resolvedKeys]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
    *   and the created id is not the first contract ID in [[ViewParticipantData.createdCore]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
    *   or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[ViewParticipantData.coreInputs]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
    *   and the key is not in [[ViewParticipantData.resolvedKeys]].
    * if this instance cannot be serialized.
    */
  def create(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): Either[String, ViewParticipantData] =
    returnLeftWhenInitializationFails(
      ViewParticipantData.tryCreate(hashOps)(
        coreInputs,
        createdCore,
        createdInSubviewArchivedInCore,
        resolvedKeys,
        actionDescription,
        rollbackContext,
        salt,
        protocolVersion,
      )
    )

  private[this] def returnLeftWhenInitializationFails[A](initialization: => A): Either[String, A] =
    try {
      Right(initialization)
    } catch {
      case InvalidViewParticipantData(message) => Left(message)
      case SerializationCheckFailed(err) => Left(err.toString)
    }

  private def fromProtoV30(hashOps: HashOps, dataP: v30.ViewParticipantData)(
      bytes: ByteString
  ): ParsingResult[ViewParticipantData] = {
    val v30.ViewParticipantData(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      rbContextP,
    ) = dataP

    for {
      coreInputsSeq <- coreInputsP.traverse(InputContract.fromProtoV30)
      coreInputs = coreInputsSeq.view
        .map(inputContract => inputContract.contract.contractId -> inputContract)
        .toMap
      createdCore <- createdCoreP.traverse(CreatedContract.fromProtoV30)
      createdInSubviewArchivedInCore <- createdInSubviewArchivedInCoreP
        .traverse(ProtoConverter.parseLfContractId)
      resolvedKeys <- resolvedKeysP.traverse(
        ResolvedKey.fromProtoV30(_).map(_.toPair)
      )
      resolvedKeysMap = resolvedKeys.toMap
      actionDescription <- ProtoConverter
        .required("action_description", actionDescriptionP)
        .flatMap(ActionDescription.fromProtoV30)

      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltP)
        .leftMap(_.inField("salt"))

      rollbackContext <- RollbackContext
        .fromProtoV30(rbContextP)
        .leftMap(_.inField("rollbackContext"))

      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      viewParticipantData <- returnLeftWhenInitializationFails(
        ViewParticipantData(
          coreInputs = coreInputs,
          createdCore = createdCore,
          createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSet,
          resolvedKeys = resolvedKeysMap,
          actionDescription = actionDescription,
          rollbackContext = rollbackContext,
          salt = salt,
        )(hashOps, rpv, Some(bytes))
      ).leftMap(ProtoDeserializationError.OtherError.apply)
    } yield viewParticipantData
  }

  final case class RootAction(
      command: LfCommand,
      authorizers: Set[LfPartyId],
      failed: Boolean,
      packageIdPreference: Set[LfPackageId],
  )

  /** Indicates an attempt to create an invalid [[ViewParticipantData]]. */
  final case class InvalidViewParticipantData(message: String) extends RuntimeException(message)
}
