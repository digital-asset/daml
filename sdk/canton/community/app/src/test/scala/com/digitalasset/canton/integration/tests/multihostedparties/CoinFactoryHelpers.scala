// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedContractEntry
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.console.ConsoleMacros.ledger_api_utils
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lfv21.java as M
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

object CoinFactoryHelpers {
  import org.scalatest.OptionValues.*

  def createCoinsFactory(
      consortiumPartyId: PartyId,
      owner: PartyId,
      participant: ParticipantReference,
      sync: Boolean = true,
  ): M.coins.CoinsFactory.Contract = {

    val createCoinsFactoryProposalCmd = M.coins.CoinsFactoryProposal
      .create(consortiumPartyId.toProtoPrimitive, owner.toProtoPrimitive)
      .commands()
      .asScala
      .toSeq

    val optTimeout = Option.when(sync)(ConsoleCommandTimeout.defaultLedgerCommandsTimeout)
    participant.ledger_api.javaapi.commands
      .submit(Seq(consortiumPartyId), createCoinsFactoryProposalCmd, optTimeout = optTimeout)

    val acceptFactoryProposalCmds = participant.ledger_api.javaapi.state.acs
      .await(M.coins.CoinsFactoryProposal.COMPANION)(owner)
      .id
      .exerciseCreateFactory()
      .commands
      .asScala
      .toSeq

    participant.ledger_api.javaapi.commands
      .submit(Seq(owner), acceptFactoryProposalCmds, optTimeout = optTimeout)

    participant.ledger_api.javaapi.state.acs
      .await(M.coins.CoinsFactory.COMPANION)(owner)
  }

  def createCoins(
      owner: PartyId,
      participant: ParticipantReference,
      amounts: Seq[Double],
      sync: Boolean = true,
  ): Unit = {
    val factory = participant.ledger_api.state.acs
      .find_generic(owner, _.templateId.isModuleEntity("Coins", "CoinsFactory"))

    val cmds = amounts.map { amount =>
      ledger_api_utils.exercise("CreateCoin", Map("amount" -> amount), factory.event)
    }

    val optTimeout = Option.when(sync)(ConsoleCommandTimeout.defaultLedgerCommandsTimeout)
    participant.ledger_api.commands
      .submit(Seq(owner), cmds, optTimeout = optTimeout)
      .discard
  }

  def isCoin(contract: WrappedContractEntry): Boolean =
    contract.templateId.isModuleEntity("Coins", "Coin")

  def getAmount(contract: WrappedContractEntry): Double = contract.event.getCreateArguments.fields
    .find(_.label == "amount")
    .value
    .getValue
    .getNumeric
    .toDouble

  /*
  Queries the ACS for the coins visible to `as` on participant `participant`.
  For each coin, return the amount and the signatories.
   */
  def getCoins(
      participant: ParticipantReference,
      as: PartyId,
      signedBy: Option[PartyId] = None,
  ): Set[(Set[PartyId], Double)] = {

    def signatoryCheck(contract: WrappedContractEntry) =
      signedBy.forall(s => contract.event.signatories.contains(s.toProtoPrimitive))

    participant.ledger_api.state.acs
      .of_party(as)
      .collect {
        case contract if isCoin(contract) && signatoryCheck(contract) =>
          val signatories = contract.event.signatories.map(PartyId.tryFromProtoPrimitive)

          (signatories.toSet, getAmount(contract))
      }
      .toSet
  }

  protected def getCoinsAmount(
      participant: ParticipantReference,
      as: PartyId,
      signedBy: Option[PartyId] = None,
  ): Set[Double] =
    getCoins(participant, as, signedBy).map { case (_, amount) => amount }

  /** Transfer coin of the given amount to the specified owner and assert that the transfer was
    * successful.
    * @param sync
    *   If true (default) ensures that all involved participants see the transaction.
    */
  def transferCoin(
      consortiumPartyId: PartyId,
      owner: PartyId,
      ownerParticipant: ParticipantReference,
      futureOwner: PartyId,
      futureOwnerParticipant: ParticipantReference,
      amount: Double,
      sync: Boolean = true,
  ): Assertion = {
    import Matchers.*

    def getCoin() = ownerParticipant.ledger_api.state.acs
      .of_party(
        owner,
        resultFilter = response => {
          val contract = WrappedContractEntry(response.contractEntry)
          isCoin(contract) && getAmount(contract) == amount
        },
      )
      .headOption
      .value

    val optTimeout =
      Option.when(sync)(ConsoleCommandTimeout.defaultLedgerCommandsTimeout)

    getCoins(ownerParticipant, owner) should contain(
      (Set(consortiumPartyId, owner), amount)
    )
    getCoinsAmount(futureOwnerParticipant, futureOwner) should not contain amount

    ownerParticipant.ledger_api.commands
      .submit(
        Seq(owner),
        Seq(
          ledger_api_utils
            .exercise("AddObserver", Map("newObserver" -> futureOwner), getCoin().event)
        ),
        optTimeout = optTimeout,
      )
      .discard

    ownerParticipant.ledger_api.commands
      .submit(
        Seq(owner),
        Seq(
          ledger_api_utils
            .exercise("ProposeTransfer", Map("futureOwner" -> futureOwner), getCoin().event)
        ),
        optTimeout = optTimeout,
      )
      .discard

    val transferProposal = ownerParticipant.ledger_api.state.acs
      .of_party(owner)
      .find(_.templateId.isModuleEntity("Coins", "CoinTransferProposal"))
      .value

    if (!sync) {
      // Wait for the counterparty to see the newly-created proposal before exercising accept
      futureOwnerParticipant.ledger_api.state.acs.await_active_contract(
        party = futureOwner,
        contractId = LfContractId.assertFromString(transferProposal.contractId),
      )
    }

    val acceptanceTxTree = futureOwnerParticipant.ledger_api.commands
      .submit(
        Seq(futureOwner),
        Seq(
          ledger_api_utils.exercise("AcceptCoinTransfer", Map.empty, transferProposal.event)
        ),
        optTimeout = optTimeout,
      )

    if (!sync) {
      // Wait for the owner to observe the effects of the transfer acceptance transaction
      ownerParticipant.ledger_api.updates.by_id(Set(owner), acceptanceTxTree.updateId).isDefined
    }

    getCoinsAmount(ownerParticipant, owner) should not contain amount
    getCoins(futureOwnerParticipant, futureOwner) should contain(
      (
        Set(consortiumPartyId, futureOwner),
        amount,
      )
    )
  }

}
