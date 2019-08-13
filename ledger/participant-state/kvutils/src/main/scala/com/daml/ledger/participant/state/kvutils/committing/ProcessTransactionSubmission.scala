// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.backport.TimeModelChecker
import com.daml.ledger.participant.state.kvutils.Conversions.{commandDedupKey, _}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting._
import com.daml.ledger.participant.state.kvutils.{Conversions, InputsAndEffects}
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReason}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{Blinding, Engine}
import com.digitalasset.daml.lf.transaction.Node.{GlobalKey, NodeCreate}
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  ContractInst,
  NodeId,
  VersionedValue
}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[kvutils] case class ProcessTransactionSubmission(
    engine: Engine,
    config: Configuration,
    entryId: DamlLogEntryId,
    recordTime: Timestamp,
    txEntry: DamlTransactionEntry,
    inputState: Map[DamlStateKey, Option[DamlStateValue]]) {

  import Common._

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    Commit.run(
      Commit.sequence(
        deduplicateCommand,
        validateLetAndTtl,
        validateContractKeyUniqueness,
        validateModelConformance,
        buildFinalResult
      )
    )

  // -------------------------------------------------------------------------------

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val commandId = txEntry.getSubmitterInfo.getCommandId
  private def tracelog(msg: String) =
    logger.trace(s"[entryId=${prettyEntryId(entryId)}, cmdId=$commandId]: $msg")

  private val txLet = parseTimestamp(txEntry.getLedgerEffectiveTime)
  private val submitterInfo = txEntry.getSubmitterInfo
  private val submitter = submitterInfo.getSubmitter
  private lazy val relTx = Conversions.decodeTransaction(txEntry.getTransaction)

  private def contractVisibleToSubmitter(contractState: DamlContractState): Boolean = {
    val locallyDisclosedTo = contractState.getLocallyDisclosedToList.asScala
    val divulgedTo = contractState.getDivulgedToList.asScala
    locallyDisclosedTo.contains(submitter) || divulgedTo.contains(submitter)
  }

  // Pull all keys from referenced contracts. We require this for 'fetchByKey' calls
  // which are not evidenced in the transaction itself and hence the contract key state is
  // not included in the inputs.
  private lazy val knownKeys: Map[GlobalKey, AbsoluteContractId] =
    inputState.collect {
      case (key, Some(value))
          if value.hasContractState
            && value.getContractState.hasContractKey
            && contractVisibleToSubmitter(value.getContractState) =>
        Conversions.decodeContractKey(value.getContractState.getContractKey) ->
          Conversions.stateKeyToContractId(key)
    }

  /** Deduplicate the submission. If the check passes we save the command deduplication
    * state.
    */
  private def deduplicateCommand(): Commit[Unit] = Commit.delay {
    val dedupKey = commandDedupKey(submitterInfo)
    val dedupEntry = inputState(dedupKey)
    if (dedupEntry.isEmpty) {
      Commit.set(
        dedupKey ->
          DamlStateValue.newBuilder
            .setCommandDedup(DamlCommandDedupValue.newBuilder.build)
            .build)
    } else
      reject(RejectionReason.DuplicateCommand)
  }

  /** Validate ledger effective time and the command's time-to-live. */
  private def validateLetAndTtl(): Commit[Unit] = Commit.delay {
    val timeModelChecker = TimeModelChecker(config.timeModel)
    val givenLET = txLet.toInstant
    val givenMRT = parseTimestamp(txEntry.getSubmitterInfo.getMaximumRecordTime).toInstant

    if (timeModelChecker.checkLet(
        currentTime = recordTime.toInstant,
        givenLedgerEffectiveTime = givenLET,
        givenMaximumRecordTime = givenMRT)
      &&
      timeModelChecker.checkTtl(givenLET, givenMRT))
      Commit.pass
    else
      reject(RejectionReason.MaximumRecordTimeExceeded)
  }

  /** Validate the submission's conformance to the DAML model */
  private def validateModelConformance(): Commit[Unit] = Commit.delay {
    engine
      .validate(relTx, txLet)
      .consume(lookupContract, lookupPackage, lookupKey)
      .fold(err => reject(RejectionReason.Disputed(err.msg)), _ => Commit.pass)
  }

  private def validateContractKeyUniqueness(): Commit[Unit] = Commit.delay {
    val allUnique = relTx.fold(GenTransaction.AnyOrder, true) {
      case (allUnique, (_nodeId, create: NodeCreate[_, VersionedValue[ContractId]]))
          if create.key.isDefined =>
        val stateKey = Conversions.contractKeyToStateKey(
          GlobalKey(
            create.coinst.template,
            Conversions.forceAbsoluteContractIds(create.key.get.key)))

        allUnique &&
        inputState
          .get(stateKey)
          .flatten
          .forall(!_.getContractKeyState.hasContractId)

      case (allUnique, _) => allUnique
    }
    if (allUnique)
      Commit.pass
    else
      reject(RejectionReason.Disputed("DuplicateKey: Contract Key not unique"))
  }

  /** All checks passed. Produce the log entry and contract state updates. */
  private def buildFinalResult(): Commit[Unit] = Commit.delay {
    import Commit._

    val effects = InputsAndEffects.computeEffects(entryId, relTx)
    val blindingInfo = Blinding.blind(relTx)

    sequence(
      // Update contract state entries to mark contracts as consumed (checked by 'validateModelConformance')
      set(effects.consumedContracts.map { key =>
        val cs =
          inputState(key).getOrElse(throw Err.MissingInputState(key)).getContractState.toBuilder
        cs.setArchivedAt(buildTimestamp(txLet))
        cs.setArchivedByEntry(entryId)
        key -> DamlStateValue.newBuilder.setContractState(cs).build
      }),
      // Add contract state entries to mark contract activeness (checked by 'validateModelConformance')
      set(effects.createdContracts.map {
        case (key, createNode) =>
          val cs = DamlContractState.newBuilder
          cs.setActiveAt(buildTimestamp(txLet))
          val localDisclosure =
            blindingInfo.localDisclosure(NodeId.unsafeFromIndex(key.getContractId.getNodeId.toInt))
          cs.addAllLocallyDisclosedTo((localDisclosure: Iterable[String]).asJava)
          val absCoInst =
            createNode.coinst.mapValue(_.mapContractId(Conversions.toAbsCoid(entryId, _)))
          cs.setContractInstance(
            Conversions.encodeContractInstance(absCoInst)
          )
          createNode.key.foreach { keyWithMaintainers =>
            cs.setContractKey(
              Conversions.encodeContractKey(
                GlobalKey(
                  createNode.coinst.template,
                  Conversions.forceAbsoluteContractIds(keyWithMaintainers.key)
                )
              ))
          }
          key -> DamlStateValue.newBuilder.setContractState(cs).build
      }),
      // Update contract state of divulged contracts
      set(blindingInfo.globalImplicitDisclosure.map {
        case (absCoid, parties) =>
          val key = absoluteContractIdToStateKey(absCoid)
          val cs =
            inputState(key).getOrElse(throw Err.MissingInputState(key)).getContractState.toBuilder
          val partiesCombined: Set[String] =
            parties.toSet[String] union cs.getDivulgedToList.asScala.toSet
          cs.clearDivulgedTo
          cs.addAllDivulgedTo(partiesCombined.asJava)
          key -> DamlStateValue.newBuilder.setContractState(cs).build
      }),
      // Update contract keys
      set(effects.updatedContractKeys.map {
        case (key, contractKeyState) =>
          key -> DamlStateValue.newBuilder
            .setContractKeyState(contractKeyState)
            .build
      }),
      done(
        DamlLogEntry.newBuilder
          .setRecordTime(buildTimestamp(recordTime))
          .setTransactionEntry(txEntry)
          .build
      )
    )
  }

  // Helper to lookup contract instances. We verify the activeness of
  // contract instances here. Since we look up every contract that was
  // an input to a transaction, we do not need to verify the inputs separately.
  private def lookupContract(coid: AbsoluteContractId) = {
    def isVisibleToSubmitter(cs: DamlContractState): Boolean =
      cs.getLocallyDisclosedToList.asScala.contains(submitter) || cs.getDivulgedToList.asScala
        .contains(submitter) || {
        logger.trace(s"lookupContract($coid): Contract state not found!")
        false
      }
    def isActive(cs: DamlContractState): Boolean = {
      val activeAt = Option(cs.getActiveAt).map(parseTimestamp)
      activeAt.exists(txLet >= _) || {
        val activeAtStr = activeAt.fold("<activeAt missing>")(_.toString)
        logger.trace(
          s"lookupContract($coid): Contract not active (let=$txLet, activeAt=$activeAtStr).")
        false
      }
    }
    val (eid, nid) = absoluteContractIdToLogEntryId(coid)
    val stateKey = absoluteContractIdToStateKey(coid)
    for {
      // Fetch the state of the contract so that activeness and visibility can be checked.
      contractState <- inputState.get(stateKey).flatMap(_.map(_.getContractState)).orElse {
        logger.trace(s"lookupContract($coid): Contract state not found!")
        throw Err.MissingInputState(stateKey)
      }
      if isVisibleToSubmitter(contractState) && isActive(contractState)
      contract = Conversions.decodeContractInstance(contractState.getContractInstance)
    } yield contract
  }
  // Helper to lookup package from the state. The package contents
  // are stored in the [[DamlLogEntry]], which we find by looking up
  // the DAML state entry at `DamlStateKey(packageId = pkgId)`.
  private def lookupPackage(pkgId: PackageId) = {
    val stateKey = DamlStateKey.newBuilder.setPackageId(pkgId).build
    for {
      value <- inputState
        .get(stateKey)
        .flatten
        .orElse {
          throw Err.MissingInputState(stateKey)
        }
      pkg <- value.getValueCase match {
        case DamlStateValue.ValueCase.ARCHIVE =>
          // NOTE(JM): Engine only looks up packages once, compiles and caches,
          // provided that the engine instance is persisted.
          try {
            Some(Decode.decodeArchive(value.getArchive)._2)
          } catch {
            case ParseError(err) => throw Err.ArchiveDecodingFailed(pkgId, err)
          }
        case _ =>
          throw Err.InvalidPayload("lookupPackage($pkgId): value not a DAML-LF archive!")
      }
    } yield pkg
  }

  private def lookupKey(key: GlobalKey): Option[AbsoluteContractId] = {
    def isVisibleToSubmitter(cs: DamlContractState, coid: AbsoluteContractId): Boolean = {
      cs.getLocallyDisclosedToList.asScala.contains(submitter) || cs.getDivulgedToList.asScala
        .contains(submitter) || {
        logger.trace(s"lookupKey($key): Contract $coid not visible to submitter $submitter.")
        false
      }
    }
    inputState
      .get(Conversions.contractKeyToStateKey(key))
      .flatMap {
        _.flatMap { value =>
          for {
            contractId <- Option(value.getContractKeyState.getContractId).map(decodeContractId)
            contractStateKey = absoluteContractIdToStateKey(contractId)
            contractState <- inputState.get(contractStateKey).flatMap(_.map(_.getContractState))
            if isVisibleToSubmitter(contractState, contractId)
          } yield contractId
        }
      }
      // If the key was not in state inputs, then we look whether any of the accessed contracts
      // has the key we're looking for. This happens with "fetchByKey" where the key lookup
      // is not evidenced in the transaction.
      .orElse(knownKeys.get(key))
  }

  /** Look up the contract instance from the log entry containing the transaction.
    *
    * This currently looks up the contract instance from the transaction stored
    * in the log entry, which is inefficient as it needs to decode the full transaction
    * to access a single contract instance.
    *
    * See issue https://github.com/digital-asset/daml/issues/734 for future work
    * to use a more efficient representation for transactions and contract instances.
    */
  private def lookupContractInstanceFromLogEntry(
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      nodeId: Int
  ): Option[ContractInst[Transaction.Value[AbsoluteContractId]]] = {
    val relTx = Conversions.decodeTransaction(entry.getTransactionEntry.getTransaction)
    relTx.nodes
      .get(NodeId.unsafeFromIndex(nodeId))
      .orElse {
        throw Err.NodeMissingFromLogEntry(entryId, nodeId)
      }
      .flatMap { node: Transaction.Node =>
        node match {
          case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
            // FixMe (RH) toAbsCoid can throw an IllegalArgumentException
            Some(
              create.coinst.mapValue(
                _.mapContractId(toAbsCoid(entryId, _))
              )
            )
          case n =>
            throw Err.NodeNotACreate(entryId, nodeId)
        }
      }
  }

  private def reject[A](reason: RejectionReason): Commit[A] = {

    val rejectionEntry = {
      val builder = DamlRejectionEntry.newBuilder
      builder
        .setSubmitterInfo(txEntry.getSubmitterInfo)

      reason match {
        case RejectionReason.Inconsistent =>
          builder.setInconsistent(DamlRejectionEntry.Inconsistent.newBuilder.setDetails(""))
        case RejectionReason.Disputed(disputeReason) =>
          builder.setDisputed(DamlRejectionEntry.Disputed.newBuilder.setDetails(disputeReason))
        case RejectionReason.ResourcesExhausted =>
          builder.setResourcesExhausted(
            DamlRejectionEntry.ResourcesExhausted.newBuilder.setDetails(""))
        case RejectionReason.MaximumRecordTimeExceeded =>
          builder.setMaximumRecordTimeExceeded(
            DamlRejectionEntry.MaximumRecordTimeExceeded.newBuilder.setDetails(""))
        case RejectionReason.DuplicateCommand =>
          builder.setDuplicateCommand(DamlRejectionEntry.DuplicateCommand.newBuilder.setDetails(""))
        case RejectionReason.PartyNotKnownOnLedger =>
          builder.setPartyNotKnownOnLedger(
            DamlRejectionEntry.PartyNotKnownOnLedger.newBuilder.setDetails(""))
        case RejectionReason.SubmitterCannotActViaParticipant(details) =>
          builder.setSubmitterCannotActViaParticipant(
            DamlRejectionEntry.SubmitterCannotActViaParticipant.newBuilder.setDetails(details))
      }
      builder
    }

    Commit.done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setRejectionEntry(rejectionEntry)
        .build,
    )
  }

}
