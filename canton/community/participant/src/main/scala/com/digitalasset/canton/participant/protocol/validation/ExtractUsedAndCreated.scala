// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.daml.lf.transaction.ContractStateMachine.{KeyInactive, KeyMapping}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.validation.ExtractUsedAndCreated.{
  CreatedContractPrep,
  InputContractPrep,
  ViewData,
  viewDataInPreOrder,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{DiscardOps, LfPartyId}

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
        .collect { case FreeKey(maintainers) => maintainers }
        .foreach(parties ++=)
    }
    parties.result()
  }

  def fetchHostedParties(
      parties: Set[LfPartyId],
      participantId: ParticipantId,
      topologySnapshot: TopologySnapshot,
  )(implicit ec: ExecutionContext): Future[Map[LfPartyId, Boolean]] = {
    parties.toSeq
      .parTraverse(partyId =>
        topologySnapshot.hostedOn(partyId, participantId).map {
          case Some(relationship) if relationship.permission.isActive => partyId -> true
          case _ => partyId -> false
        }
      )
      .map(_.toMap)
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
          staticDomainParameters.uniqueContractKeys,
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
    uniqueContractKeys: Boolean,
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
      keys = inputAndUpdatedKeys(rootViews.forgetNE),
      hostedWitnesses = hostedParties.filter(_._2).map(_._1).toSet,
    )
  }

  private def inputAndUpdatedKeys(rootViews: Seq[TransactionView]): InputAndUpdatedKeys =
    extractInputAndUpdatedKeysV3(rootViews)

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
      hosts = hostsAny(viewData.informees)
      created <- viewData.participant.createdCore
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
      witnessedAndDivulged = inputContracts.divulged ++ createdContracts.witnessed,
      checkActivenessTxInputs = checkActivenessTxInputs,
      consumedInputsOfHostedStakeholders = consumedInputsOfHostedStakeholders,
      maybeCreated = maybeCreated,
      transient = transient,
      used = inputContracts.used,
    )
  }

  /** For [[com.digitalasset.canton.data.FullTransactionViewTree]]s produced by
    * [[com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactoryImplV3]]
    */
  private def extractInputAndUpdatedKeysV3(
      rootViews: Seq[TransactionView]
  ): InputAndUpdatedKeys = {
    val (updatedKeys, freeKeys) = if (uniqueContractKeys) {
      /* In UCK mode, the globalKeyInputs have been computed with `ContractKeyUniquenessMode.Strict`,
       * i.e., the global key input of each view contain the expected pre-view state of the key.
       * So for key freshness, it suffices to combine the global key inputs with earlier view's resolution taking precedence
       *
       * For the updates of the key state, we count the created contracts for a key and the archivals for contracts with this key,
       * and look at the difference. The counting ignore nodes underneath rollback nodes.
       * This is fine because the nodes under a rollback do not take effect;
       * even if the transaction was committed only partially,
       * the committed subtransaction would still contain the rollback node.
       */
      val freeKeysB = Set.newBuilder[LfGlobalKey]
      rootViews
        .foldLeft(Set.empty[LfGlobalKey]) { (seenKeys, rootView) =>
          val gki = rootView.globalKeyInputs
          gki.foldLeft(seenKeys) { case (seenKeys, (key, resolution)) =>
            if (seenKeys.contains(key)) seenKeys
            else {
              if (resolution.resolution.isEmpty && hostsAny(resolution.maintainers)) {
                freeKeysB.addOne(key)
              }
              seenKeys.incl(key)
            }
          }
        }
        .discard
      val freeKeys = freeKeysB.result()

      // Now find out the keys that this transaction updates.
      // We cannot just compare the end state of the key against the initial state,
      // because a key may be free at the start and at the end, and yet be allocated in between to a transient contract.
      // Since transactions can be committed partially, the transient contract may actually be created.
      // So we have to lock the key.

      val allUpdatedKeys = rootViews.foldLeft(Map.empty[LfGlobalKey, Set[LfPartyId]]) {
        (acc, rootView) => acc ++ rootView.updatedKeys
      }
      val updatedKeysOfHostedMaintainer = allUpdatedKeys.filter { case (_, maintainers) =>
        hostsAny(maintainers)
      }

      // As the participant receives all views that update a key it hosts a maintainer of,
      // we simply merge the active ledger states at the end of all root views for the updated keys.
      // This gives the final resolution for the key.
      val mergedKeys = rootViews.foldLeft(Map.empty[LfGlobalKey, KeyMapping]) {
        (accKeys, rootView) => accKeys ++ rootView.updatedKeyValues
      }

      val updatedKeys = mergedKeys.collect {
        case (key, keyMapping) if updatedKeysOfHostedMaintainer.contains(key) =>
          val status =
            if (keyMapping == KeyInactive) ContractKeyJournal.Unassigned
            else ContractKeyJournal.Assigned
          key -> status
      }

      (updatedKeys, freeKeys)
    } else (Map.empty[LfGlobalKey, ContractKeyJournal.Status], Set.empty[LfGlobalKey])

    InputAndUpdatedKeysV3(
      uckFreeKeysOfHostedMaintainers = freeKeys,
      uckUpdatedKeysOfHostedMaintainers = updatedKeys,
    )
  }

  private def hostsAny(
      parties: IterableOnce[LfPartyId]
  )(implicit loggingContext: ErrorLoggingContext): Boolean = {
    parties.iterator.exists(party =>
      hostedParties.getOrElse(
        party, {
          loggingContext.error(
            s"Prefetch of parties is wrong and missed to load data for party $party"
          )
          false
        },
      )
    )
  }

}
