// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.transaction.Versioned

import scala.concurrent.ExecutionContext

/** Helper to extract information from transaction view trees.
  */
object ExtractUsedAndCreated {

  private[validation] final case class ViewData(
      participant: ViewParticipantData,
      common: ViewCommonData,
  ) {
    def informees: Set[LfPartyId] = common.viewConfirmationParameters.informees

    def transientContracts(): Seq[LfContractId] =
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
      data.participant.createdCore.foreach { c =>
        // The object invariants of metadata enforce that every maintainer is also a stakeholder.
        // Therefore, we don't have to explicitly add maintainers.
        parties ++= c.contract.metadata.stakeholders
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
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Map[LfPartyId, Option[ParticipantAttributes]]] =
    topologySnapshot.hostedOn(parties, participantId).map { partyWithAttributes =>
      parties
        .map(partyId => partyId -> partyWithAttributes.get(partyId))
        .toMap
    }

  def apply(
      participantId: ParticipantId,
      rootViews: NonEmpty[Seq[TransactionView]],
      topologySnapshot: TopologySnapshot,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[UsedAndCreated] = {

    val partyIds = extractPartyIds(rootViews)

    fetchHostedParties(partyIds, participantId, topologySnapshot)
      .map { hostedParties =>
        new ExtractUsedAndCreated(
          hostedParties,
          loggerFactory,
        ).usedAndCreated(rootViews)
      }
  }

  private[validation] final case class CreatedContractPrep(
      // The contract will be optional if it has been rolled back
      createdContractsOfHostedInformees: Map[LfContractId, Option[ContractInstance]],
      witnessed: Map[LfContractId, ContractInstance],
  )

  private[validation] final case class InputContractPrep(
      used: Map[LfContractId, ContractInstance],
      divulged: Map[LfContractId, ContractInstance],
      consumedOfHostedStakeholders: Map[LfContractId, Set[LfPartyId]],
      contractIdsOfHostedInformeeStakeholder: Set[LfContractId],
      contractIdsAllowedToBeUnknown: Set[LfContractId],
  )

}

private[validation] class ExtractUsedAndCreated(
    hostedParties: Map[LfPartyId, Option[ParticipantAttributes]],
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
      hostedWitnesses = hostedParties.filter(_._2.nonEmpty).keySet,
    )
  }

  private[validation] def inputContractPrep(
      dataViews: Seq[ViewData]
  ): InputContractPrep = {

    val usedB =
      Map.newBuilder[LfContractId, ContractInstance]
    val contractIdsOfHostedInformeeStakeholderB = Set.newBuilder[LfContractId]
    val contractIdsAllowedToBeUnknownB = Set.newBuilder[LfContractId]
    val consumedOfHostedStakeholdersB =
      Map.newBuilder[LfContractId, Set[LfPartyId]]
    val divulgedB =
      Map.newBuilder[LfContractId, ContractInstance]

    (for {
      viewData <- dataViews: Seq[ViewData]
      inputContractWithMetadata <- viewData.participant.coreInputs.values
    } yield {
      val informees = viewData.informees
      val contract = inputContractWithMetadata.contract
      val stakeholders = contract.metadata.stakeholders
      val informeeStakeholders = stakeholders.intersect(informees)

      usedB += contract.contractId -> contract

      if (hostsAny(stakeholders)) {
        if (hostsAny(informeeStakeholders)) {
          contractIdsOfHostedInformeeStakeholderB += contract.contractId
        }
        // We do not need to include in consumedInputsOfHostedStakeholders the contracts created in the core
        // because they are not inputs even if they are consumed.
        if (inputContractWithMetadata.consumed) {
          // Input contracts consumed under rollback node are not necessarily consumed in the transaction.
          if (!viewData.participant.rollbackContext.inRollback) {
            consumedOfHostedStakeholdersB +=
              contract.contractId -> stakeholders
          }
        }
        // Track input contracts that might legitimately be unknown (due to party onboarding).
        if (areAllHostedStakeholdersOnboarding(stakeholders)) {
          contractIdsAllowedToBeUnknownB += contract.contractId
        }
      } else {
        divulgedB += (contract.contractId -> contract)
      }
    }).discard

    InputContractPrep(
      used = usedB.result(),
      divulged = divulgedB.result(),
      consumedOfHostedStakeholders = consumedOfHostedStakeholdersB.result(),
      contractIdsOfHostedInformeeStakeholder = contractIdsOfHostedInformeeStakeholderB.result(),
      contractIdsAllowedToBeUnknown = contractIdsAllowedToBeUnknownB.result(),
    )
  }

  private[validation] def createdContractPrep(dataViews: Seq[ViewData]): CreatedContractPrep = {

    val createdContractsOfHostedInformeesB =
      Map.newBuilder[LfContractId, Option[ContractInstance]]

    val witnessedB =
      Map.newBuilder[LfContractId, ContractInstance]

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

    val transient: Map[LfContractId, Set[LfPartyId]] =
      maybeCreated.collect {
        case (cid, Some(contract)) if allConsumed.contains(cid) =>
          cid -> contract.metadata.stakeholders
      }

    val consumedInputsOfHostedStakeholders =
      inputContracts.consumedOfHostedStakeholders -- maybeCreated.keySet

    UsedAndCreatedContracts(
      witnessed = createdContracts.witnessed,
      checkActivenessTxInputs = checkActivenessTxInputs,
      consumedInputsOfHostedStakeholders = consumedInputsOfHostedStakeholders,
      maybeCreated = maybeCreated,
      transient = transient,
      used = inputContracts.used,
      maybeUnknown = inputContracts.contractIdsAllowedToBeUnknown,
    )
  }

  private def hostsAny(parties: IterableOnce[LfPartyId]): Boolean =
    parties.iterator.exists(lookupParty(_).nonEmpty)

  private def lookupParty(
      party: LfPartyId
  )(implicit loggingContext: ErrorLoggingContext): Option[ParticipantAttributes] =
    hostedParties
      .getOrElse(
        party, {
          loggingContext.error(
            s"Prefetch of parties is wrong and missed to load data for party $party"
          )
          None
        },
      )

  /** Indicate whether all (non-empty) hosted parties are onboarding. */
  private def areAllHostedStakeholdersOnboarding(parties: IterableOnce[LfPartyId]): Boolean = {
    val hostedPartyParticipantAttribs = parties.iterator.flatMap(lookupParty(_))
    hostedPartyParticipantAttribs.nonEmpty && hostedPartyParticipantAttribs.forall(_.onboarding)
  }
}
