// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.reindexfromstate

import java.time.Instant
import java.util.UUID

import com.daml.ledger.on.memory.InMemoryState.MutableState
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractState,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.v1.Update.{
  ConfigurationChanged,
  PartyAddedToParticipant,
  PublicPackageUpload,
  TransactionAccepted,
}
import com.daml.ledger.participant.state.v1._
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction, NodeId}
import com.daml.lf.value.Value

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class StateToUpdateMapping(state: MutableState) {
  def generateUpdates(): Iterable[Update] = {
    val result = new ListBuffer[Update]
    for ((key, value) <- state) {
      val damlStateKey = DamlStateKey.parseFrom(key.bytes)
      for {
        damlStateValue <- kvutils.Envelope.openStateValue(value).toOption
        update <- processStateItem(damlStateKey, damlStateValue)
      } {
        result.append(update)
      }
    }
    result
  }

  private def processStateItem(
      damlStateKey: DamlStateKey,
      damlStateValue: DamlStateValue,
  ): Option[Update] = {
    damlStateKey.getKeyCase match {
      case DamlStateKey.KeyCase.PACKAGE_ID =>
        Some(
          PublicPackageUpload(
            List(damlStateValue.getArchive),
            None,
            recordTime(),
            None,
          )
        )

      case DamlStateKey.KeyCase.CONFIGURATION =>
        val configurationEntry = damlStateValue.getConfigurationEntry
        Configuration.decode(configurationEntry.getConfiguration).toOption.map { config =>
          ConfigurationChanged(
            recordTime(),
            SubmissionId.assertFromString(configurationEntry.getSubmissionId),
            ParticipantId.assertFromString(configurationEntry.getParticipantId),
            config,
          )
        }

      case DamlStateKey.KeyCase.PARTY =>
        val party = damlStateValue.getParty
        Some(
          PartyAddedToParticipant(
            Ref.Party.assertFromString(damlStateKey.getParty),
            party.getDisplayName,
            ParticipantId.assertFromString(party.getParticipantId),
            recordTime(),
            None,
          )
        )

      case DamlStateKey.KeyCase.CONTRACT_KEY =>
        // Handled as part of CONTRACT_ID.
        None

      case DamlStateKey.KeyCase.CONTRACT_ID =>
        val contract = damlStateValue.getContractState
        if (contract.hasArchivedAt) {
          println(s"Skipping archived contract ${contract.getContractKey}")
          None
        } else {
          val contractInstance = Conversions.decodeContractInstance(contract.getContractInstance)
          //          // Look up contract key.
          //          val contractKey = contract.getContractKey
          //          val damlContractKey = DamlKvutils.DamlStateKey.newBuilder
          //            .setContractKey(contractKey)
          //            .build
          //          val contractKeyValueMaybe = state.get(Raw.StateKey(damlContractKey)).flatMap { value =>
          //            kvutils.Envelope.openStateValue(value).toOption
          //          }
          //          val contractKeyValue: DamlStateValue = contractKeyValueMaybe
          //            .getOrElse(throw new IllegalArgumentException("Invalid contract key value"))
          val ledgerEffectiveTime = Timestamp.assertFromInstant(
            Instant.ofEpochSecond(
              contract.getActiveAt.getSeconds,
              contract.getActiveAt.getNanos.toLong,
            )
          )
          val submissionSeed = crypto.Hash.assertHashContractInstance(
            contractInstance.template,
            contractInstance.arg.value,
          )

          val createTransaction = TransactionAccepted(
            optSubmitterInfo = None,
            transactionMeta = TransactionMeta(
              ledgerEffectiveTime = ledgerEffectiveTime,
              workflowId = None,
              submissionTime = ledgerEffectiveTime,
              submissionSeed = submissionSeed,
              optUsedPackages = None,
              optNodeSeeds = None,
              optByKeyNodes = None,
            ),
            transaction =
              transactionFromContractState(damlStateKey.getContractId, contract, contractInstance),
            transactionId = TransactionId.assertFromString(
              UUID.randomUUID().toString
            ), // TODO: Can be determined from state?
            recordTime = recordTime(),
            divulgedContracts = List.empty, // No divulged contracts at creation.
            blindingInfo = Some(blindingInfoFromContractState(contract, damlStateKey.getContractId)),
          )
          Some(createTransaction)
        }

      case DamlStateKey.KeyCase.COMMAND_DEDUP | DamlStateKey.KeyCase.SUBMISSION_DEDUP =>
        None

      case DamlStateKey.KeyCase.KEY_NOT_SET =>
        None
    }
  }

  private def transactionFromContractState(
      contractId: String,
      contract: DamlContractState,
      contractInstance: Value.ContractInst[Value.VersionedValue[Value.ContractId]],
  ): CommittedTransaction = {
    val transactionBuilder = TransactionBuilder()
    // FIXME: Map from state.
    val observers = contract.getLocallyDisclosedToList.asScala ++ contract.getDivulgedToList.asScala
    val createNode = transactionBuilder.create(
      id = contractId,
      template = contractInstance.template.toString,
      argument = contractInstance.arg.value,
      signatories = Seq.empty,
      observers = observers.sorted,
      key = None,
    )
    transactionBuilder.add(createNode)
    transactionBuilder.buildCommitted()
  }

  private def blindingInfoFromContractState(
      contractState: DamlContractState,
      contractId: String,
  ): BlindingInfo = {
    val disclosedTo =
      contractState.getLocallyDisclosedToList.asScala.map(Ref.Party.assertFromString).toSet
    val divulgedTo = contractState.getDivulgedToList.asScala.map(Ref.Party.assertFromString).toSet
    BlindingInfo(
      disclosure = Map(NodeId(0) -> disclosedTo),
      divulgence =
        Map(com.daml.lf.value.Value.ContractId.assertFromString(contractId) -> divulgedTo),
    )
  }

  private def recordTime(): Timestamp = Timestamp.Epoch
}
