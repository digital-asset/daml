// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.validation.ExtractUsedAndCreated.{
  CreatedContractPrep,
  InputContractPrep,
  ViewData,
  viewDataInPreOrder,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.transaction.Versioned

import scala.concurrent.{ExecutionContext, Future}

/** Helper to extract information from transaction view trees.
  */
object ExtractUsedAndCreated {

  private[validation] final case class ViewData(
      participant: ViewParticipantData,
      common: ViewCommonData,
  ) {
    def informees: Set[LfPartyId] = common.viewConfirmationParameters.informees

    def transientContracts(): Seq[LfContractId] = {

      // Only track transient contracts outside of rollback scopes.
      if (!participant.rollbackContext.inRollback) {
        val transientCore =
          participant.createdCore
            .filter(x => x.consumedInCore && !x.rolledBack)
            .map(_.contract.contractId)

        // The participant might host only an actor and not a stakeholder of the contract that is archived in the core.
        // We nevertheless add all of them here because we will intersect this set with `createdContractsOfHostedStakeholdersB` later.
        // This ensures that we only cover contracts of which the participant hosts a stakeholder.
        transientCore ++ participant.createdInSubviewArchivedInCore
      } else {
        Seq.empty
      }
    }

  }

  private def viewDataInPreOrder(view: TransactionView): Seq[ViewData] = {
    def viewData(v: TransactionView) = ViewData(
      v.viewParticipantData.tryUnwrap,
      v.viewCommonData.tryUnwrap,
    )
    view.subviews.assertAllUnblinded(hash =>
      s"View ${view.viewHash} contains an unexpected blinded subview $hash"
    )
    Seq(viewData(view)) ++ view.subviews.unblindedElements.flatMap(viewDataInPreOrder)
  }

  private def extractPartyIds(
      rootViews: NonEmpty[Seq[TransactionView]]
  ): Set[LfPartyId] = {
    val parties = Set.newBuilder[LfPartyId]
    rootViews.forgetNE.flatMap(viewDataInPreOrder).foreach { data =>
      parties ++= data.informees
      data.participant.coreInputs.values.foreach { c =>
        parties ++= c.stakeholders
        parties ++= c.maintainers
      }
      data.participant.resolvedKeys.values
        .collect { case Versioned(_, FreeKey(maintainers)) => maintainers }
        .foreach(parties ++=)
    }
    parties.result()
  }

  def fetchHostedParties(
      parties: Set[LfPartyId],
      participantId: ParticipantId,
      topologySnapshot: TopologySnapshot,
  )(implicit ec: ExecutionContext, tc: TraceContext): Future[Map[LfPartyId, Boolean]] = {
    topologySnapshot.hostedOn(parties, participantId).map { partyWithAttributes =>
      parties
        .map(partyId => partyId -> partyWithAttributes.contains(partyId))
        .toMap
    }
  }

  def apply(
      participantId: ParticipantId,
      staticDomainParameters: StaticDomainParameters,
      rootViews: NonEmpty[Seq[TransactionView]],
      topologySnapshot: TopologySnapshot,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[UsedAndCreated] = {

    val partyIds = extractPartyIds(rootViews)

    fetchHostedParties(partyIds, participantId, topologySnapshot)
      .map { hostedParties =>
        new ExtractUsedAndCreated(
          staticDomainParameters.protocolVersion,
          hostedParties,
          loggerFactory,
        ).usedAndCreated(rootViews)
      }
  }

  private[validation] final case class CreatedContractPrep(
      // The contract will be optional if it has been rolled back
      createdContractsOfHostedInformees: Map[LfContractId, Option[SerializableContract]],
      witnessed: Map[LfContractId, SerializableContract],
  )

  private[validation] final case class InputContractPrep(
      used: Map[LfContractId, SerializableContract],
      divulged: Map[LfContractId, SerializableContract],
      consumedOfHostedStakeholders: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
      contractIdsOfHostedInformeeStakeholder: Set[LfContractId],
  )

}

private[validation] class ExtractUsedAndCreated(
    protocolVersion: ProtocolVersion,
    hostedParties: Map[LfPartyId, Boolean],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit traceContext: TraceContext)
    extends NamedLogging {

  private[validation] def usedAndCreated(
      rootViews: NonEmpty[Seq[TransactionView]]
  ): UsedAndCreated = {

    val dataViews = rootViews.forgetNE.flatMap(v => viewDataInPreOrder(v))

    val createdContracts = createdContractPrep(dataViews)
    val inputContracts = inputContractPrep(dataViews)
    val transientContracts = transientContractsPrep(dataViews)

    UsedAndCreated(
      contracts = usedAndCreatedContracts(createdContracts, inputContracts, transientContracts),
      hostedWitnesses = hostedParties.filter(_._2).keySet,
    )
  }

  private[validation] def inputContractPrep(
      dataViews: Seq[ViewData]
  ): InputContractPrep = {

    val usedB =
      Map.newBuilder[LfContractId, SerializableContract]
    val contractIdsOfHostedInformeeStakeholderB = Set.newBuilder[LfContractId]
    val consumedOfHostedStakeholdersB =
      Map.newBuilder[LfContractId, WithContractHash[Set[LfPartyId]]]
    val divulgedB =
      Map.newBuilder[LfContractId, SerializableContract]

    (for {
      viewData <- dataViews: Seq[ViewData]
      inputContractWithMetadata <- viewData.participant.coreInputs.values
    } yield {
      val informees = viewData.informees
      val contract = inputContractWithMetadata.contract
      val stakeholders = contract.metadata.stakeholders
      val informeeStakeholders = stakeholders.intersect(informees)

      usedB += contract.contractId -> contract

      if (hostsAny(informeeStakeholders)) {
        contractIdsOfHostedInformeeStakeholderB += contract.contractId
        // We do not need to include in consumedInputsOfHostedStakeholders the contracts created in the core
        // because they are not inputs even if they are consumed.
        if (inputContractWithMetadata.consumed) {
          // Input contracts consumed under rollback node are not necessarily consumed in the transaction.
          if (!viewData.participant.rollbackContext.inRollback) {
            consumedOfHostedStakeholdersB +=
              contract.contractId -> WithContractHash.fromContract(contract, stakeholders)
          }
        }
      } else if (hostsAny(stakeholders.diff(informees))) {
        // TODO(i12901) report view participant data as malformed
        ErrorUtil.requireArgument(
          !inputContractWithMetadata.consumed,
          s"Participant hosts non-informee stakeholder(s) of consumed ${contract.contractId}; stakeholders: $stakeholders, informees: $informees",
        )
        // If the participant hosts a non-informee stakeholder of a used contract,
        // it shouldn't check activeness, so we don't add it to checkActivenessOrRelative
        // If another view adds the contract nevertheless to it, it will not matter since the participant
        // will not send a confirmation for this view.
      } else {
        divulgedB += (contract.contractId -> contract)
      }
    }).discard

    InputContractPrep(
      used = usedB.result(),
      divulged = divulgedB.result(),
      consumedOfHostedStakeholders = consumedOfHostedStakeholdersB.result(),
      contractIdsOfHostedInformeeStakeholder = contractIdsOfHostedInformeeStakeholderB.result(),
    )
  }

  private[validation] def createdContractPrep(dataViews: Seq[ViewData]): CreatedContractPrep = {

    val createdContractsOfHostedInformeesB =
      Map.newBuilder[LfContractId, Option[SerializableContract]]

    val witnessedB =
      Map.newBuilder[LfContractId, SerializableContract]

    (for {
      viewData <- dataViews
      createdAndHosts <-
        viewData.participant.createdCore.map { cc =>
          (cc, hostsAny(cc.contract.metadata.stakeholders))
        }
      (created, hosts) = createdAndHosts
      rolledBack = viewData.participant.rollbackContext.inRollback || created.rolledBack
      contract = created.contract
    } yield {
      if (hosts) {
        createdContractsOfHostedInformeesB += contract.contractId -> Option.when(!rolledBack)(
          contract
        )
      } else if (!rolledBack) {
        witnessedB += (contract.contractId -> contract)
      }
    }).discard

    CreatedContractPrep(
      createdContractsOfHostedInformees = createdContractsOfHostedInformeesB.result(),
      witnessed = witnessedB.result(),
    )
  }

  private[validation] def transientContractsPrep(dataViews: Seq[ViewData]): Set[LfContractId] = {

    val transientContractsB = Set.newBuilder[LfContractId]

    (for {
      viewData <- dataViews: Seq[ViewData]
      if hostsAny(viewData.informees)
    } yield {
      transientContractsB ++= viewData.transientContracts()
    }).discard

    transientContractsB.result()
  }

  private def usedAndCreatedContracts(
      createdContracts: CreatedContractPrep,
      inputContracts: InputContractPrep,
      transientContracts: Set[LfContractId],
  ): UsedAndCreatedContracts = {

    // includes contracts created under rollback nodes
    val maybeCreated = createdContracts.createdContractsOfHostedInformees

    // Remove the contracts created in the same transaction from the contracts to be checked for activeness
    val checkActivenessTxInputs =
      inputContracts.contractIdsOfHostedInformeeStakeholder -- maybeCreated.keySet

    // Among the consumed relative contracts, the activeness check on the participant cares only about those
    // for which the participant hosts a stakeholder, i.e., the participant must also see the creation.
    // If the contract is created in a view (including subviews) and archived in the core,
    // then it does not show up as a consumed input of another view, so we explicitly add those.
    val allConsumed =
      inputContracts.consumedOfHostedStakeholders.keySet.union(transientContracts)

    val transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]] =
      maybeCreated.collect {
        case (cid, Some(contract)) if allConsumed.contains(cid) =>
          cid -> WithContractHash.fromContract(contract, contract.metadata.stakeholders)
      }

    val consumedInputsOfHostedStakeholders =
      inputContracts.consumedOfHostedStakeholders -- maybeCreated.keySet

    UsedAndCreatedContracts(
      witnessed = createdContracts.witnessed,
      divulged = inputContracts.divulged,
      checkActivenessTxInputs = checkActivenessTxInputs,
      consumedInputsOfHostedStakeholders = consumedInputsOfHostedStakeholders,
      maybeCreated = maybeCreated,
      transient = transient,
      used = inputContracts.used,
    )
  }

  private def hostsAny(
      parties: IterableOnce[LfPartyId]
  )(implicit loggingContext: ErrorLoggingContext): Boolean =
    parties.iterator.exists(party => {
      hostedParties.getOrElse(
        party, {
          loggingContext.error(
            s"Prefetch of parties is wrong and missed to load data for party $party"
          )
          false
        },
      )
    })

}
