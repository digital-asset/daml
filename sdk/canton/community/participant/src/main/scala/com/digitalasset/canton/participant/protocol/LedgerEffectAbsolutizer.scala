// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.data.{
  AssignedKey,
  FreeKey,
  SerializableKeyResolution,
  ViewParticipantData,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.protocol.{
  ContractIdAbsolutizer,
  CreatedContract,
  InputContract,
  LfContractId,
  LfGlobalKey,
}
import com.digitalasset.canton.{LfPartyId, LfVersioned}

class LedgerEffectAbsolutizer(absolutizer: ContractIdAbsolutizer) {

  def absoluteViewEffects(
      vpd: ViewParticipantData,
      informees: Set[LfPartyId],
  ): Either[String, ViewAbsoluteLedgerEffect] =
    for {
      coreInputs <- vpd.coreInputs.toSeq.traverse { case (cid, input) =>
        if (input.contractId.isAbsolute) Right(cid -> input)
        else
          absolutizeInputContract(input).map(absolutizedInstance =>
            // The ViewParticipantData ensures that the keys of the core inputs are the same contract ID as in the referenced value.
            // So we can skip absolutizing the keys
            (absolutizedInstance.contractId, absolutizedInstance)
          )
      }
      createdCore <- vpd.createdCore.traverse(absolutizeCreatedContract)
      createdInSubviewArchivedInCore <- vpd.createdInSubviewArchivedInCore.toSeq
        .traverse(absolutizer.absolutizeContractId)
        .map(_.toSet)
      resolvedKeys <- vpd.resolvedKeys.toSeq.traverse { case (gkey, resolution) =>
        for {
          absolutizedKey <- gkey.key.traverseCid(absolutizer.absolutizeContractId)
          absolutizedResolution <- absolutizeKeyResolution(resolution.unversioned)
        } yield (
          LfGlobalKey.assertBuild(gkey.templateId, absolutizedKey, gkey.packageName),
          resolution.copy(unversioned = absolutizedResolution),
        )
      }
    } yield ViewAbsoluteLedgerEffect(
      coreInputs = coreInputs.toMap,
      createdCore = createdCore,
      createdInSubviewArchivedInCore = createdInSubviewArchivedInCore,
      resolvedKeys = resolvedKeys.toMap,
      inRollback = vpd.rollbackContext.inRollback,
      informees = informees,
    )

  def absolutizeInputContract(input: InputContract): Either[String, InputContract] =
    for {
      absolutizedInstance <- absolutizer.absolutizeContractInstance(input.contract)
    } yield input.copy(contract = absolutizedInstance)

  def absolutizeCreatedContract(created: CreatedContract): Either[String, CreatedContract] =
    for {
      absolutizedInstance <- absolutizer.absolutizeContractInstance(created.contract)
      absolutizedCreated <- CreatedContract.create(
        contract = absolutizedInstance,
        consumedInCore = created.consumedInCore,
        rolledBack = created.rolledBack,
      )
    } yield absolutizedCreated

  def absolutizeKeyResolution(
      resolution: SerializableKeyResolution
  ): Either[String, SerializableKeyResolution] =
    resolution match {
      case AssignedKey(cid) =>
        absolutizer.absolutizeContractId(cid).map(AssignedKey.apply)
      case free: FreeKey => Right(free)
    }
}

object LedgerEffectAbsolutizer {

  /** Projection of [[com.digitalasset.canton.data.ViewParticipantData]] to relevant fields with
    * absolutized contract IDs
    */
  final case class ViewAbsoluteLedgerEffect(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]],
      inRollback: Boolean,
      informees: Set[LfPartyId],
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ViewAbsoluteLedgerEffect] = prettyOfClass(
      paramIfNonEmpty("core inputs", _.coreInputs),
      paramIfNonEmpty("created core", _.createdCore),
      paramIfNonEmpty("created in subview, archived in core", _.createdInSubviewArchivedInCore),
      paramIfNonEmpty("resolved keys", _.resolvedKeys),
      paramIfTrue("in rollback", _.inRollback),
      param("informees", _.informees),
    )
  }
}
