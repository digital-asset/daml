// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.reindexfromstate

import java.time.Instant
import java.util.UUID

import com.daml.ledger.on.memory.InMemoryState.MutableState
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.Conversions.{contractIdToStateKey, packageStateKey}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractState,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err, Raw}
import com.daml.ledger.participant.state.v1.Update.{
  ConfigurationChanged,
  PartyAddedToParticipant,
  PublicPackageUpload,
  TransactionAccepted,
}
import com.daml.ledger.participant.state.v1._
import com.daml.lf.archive.Decode
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, VisibleByKey}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction, Node, NodeId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

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
            transaction = transactionFromContractState(
              damlStateKey.getContractId,
              contract,
              contractInstance,
              submissionSeed,
            ),
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

  private val engine = new Engine()

  private def transactionFromContractState(
      contractId: String,
      contract: DamlContractState,
      contractInstance: Value.ContractInst[Value.VersionedValue[Value.ContractId]],
      submissionSeed: crypto.Hash,
  ): CommittedTransaction = {
    // Submitters must be stakeholders.
    val locallyDisclosedTo = contract.getLocallyDisclosedToList.asScala
    val templateId = contractInstance.template.toString
    val fetchedContract =
      fetchContractViaEngine(locallyDisclosedTo, contractId, templateId, submissionSeed).get
    val signatories = fetchedContract.signatories
    val observers = fetchedContract.stakeholders

    val transactionBuilder = TransactionBuilder()
    val createNode = transactionBuilder.create(
      id = contractId,
      template = templateId,
      argument = contractInstance.arg.value,
      signatories = signatories.toSeq,
      observers = observers.toSeq,
      key = None,
    )
    transactionBuilder.add(createNode)
    transactionBuilder.buildCommitted()
  }

  private def fetchContractViaEngine(
      submitters: Iterable[String],
      contractId: String,
      templateId: String,
      submissionSeed: crypto.Hash,
  ): Option[Node.NodeFetch[ContractId]] = {
    val fetchResult = engine.submitFetch(
      submitters.map(Ref.Party.assertFromString).toSet,
      Identifier.assertFromString(templateId),
      ContractId.assertFromString(contractId),
      Timestamp.Epoch,
      ParticipantId.assertFromString("a participant"),
      submissionSeed,
    )
    fetchResult
      .consume(
        contractId => lookupContract(contractId),
        packageId => lookupPackage(packageId),
        // No key lookups.
        _ => None,
        // No visibility checks.
        _ => VisibleByKey.Visible,
      )
      .toOption
  }

  private def lookupPackage(pkgId: PackageId): Option[Ast.Package] = {
    val stateKey = packageStateKey(pkgId)
    for {
      rawValue <- state
        .get(Raw.StateKey(stateKey.toByteString))
        .orElse {
          throw Err.MissingInputState(stateKey)
        }
      value <- kvutils.Envelope.openStateValue(rawValue).toOption
      pkg <- value.getValueCase match {
        case DamlStateValue.ValueCase.ARCHIVE =>
          try {
            Some(Decode.decodeArchive(value.getArchive)._2)
          } catch {
            case ParseError(err) =>
              throw Err.DecodeError("Archive", err)
          }

        case _ =>
          val msg = "value is not a DAML-LF archive"
          throw Err.DecodeError("Archive", msg)
      }
    } yield pkg
  }

  private def lookupContract(
      coid: Value.ContractId
  ): Option[Value.ContractInst[Value.VersionedValue[Value.ContractId]]] = {
    val stateKey = contractIdToStateKey(coid)
    for {
      contractState <- state
        .get(Raw.StateKey(stateKey.toByteString))
        .flatMap(envelope => kvutils.Envelope.openStateValue(envelope).toOption)
        .map(_.getContractState)
      contract = Conversions.decodeContractInstance(contractState.getContractInstance)
    } yield contract
  }

//  private def lookupKey(key: GlobalKeyWithMaintainers): Option[Value.ContractId] = {
//    val stateKey = Conversions.globalKeyToStateKey(key.globalKey)
//    for {
//      rawStateValue <- state.get(Raw.StateKey(stateKey.toByteString))
//      stateValue <- kvutils.Envelope.openStateValue(rawStateValue).toOption
//      if stateValue.getContractKeyState.getContractId.nonEmpty
//    } yield decodeContractId(stateValue.getContractKeyState.getContractId)
//  }

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
