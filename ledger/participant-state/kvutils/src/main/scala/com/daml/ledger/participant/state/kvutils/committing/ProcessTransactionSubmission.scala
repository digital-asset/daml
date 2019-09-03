// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.backport.TimeModelChecker
import com.daml.ledger.participant.state.kvutils.Conversions.{buildTimestamp, commandDedupKey, _}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.{Conversions, Err, InputsAndEffects, Pretty}
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, RejectionReason}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{Blinding, Engine}
import com.digitalasset.daml.lf.transaction.{BlindingInfo, GenTransaction}
import com.digitalasset.daml.lf.transaction.Node.{GlobalKey, NodeCreate}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId, NodeId, VersionedValue}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[kvutils] case class ProcessTransactionSubmission(
    engine: Engine,
    entryId: DamlLogEntryId,
    recordTime: Timestamp,
    defaultConfig: Configuration,
    participantId: ParticipantId,
    txEntry: DamlTransactionEntry,
    // FIXME(JM): remove inputState as a global to avoid accessing it when the intermediate
    // state should be used.
    inputState: Map[DamlStateKey, Option[DamlStateValue]]) {

  import Common._
  import Commit._

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    runSequence(
      inputState = inputState.collect { case (k, Some(x)) => k -> x },
      authorizeSubmitter,
      deduplicateCommand,
      validateLetAndTtl,
      validateContractKeyUniqueness,
      validateModelConformance,
      authorizeAndBlind
        .flatMap(buildFinalResult)
    )

  // -------------------------------------------------------------------------------

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val config: Configuration =
    Common.getCurrentConfiguration(defaultConfig, inputState, logger)

  private val commandId = txEntry.getSubmitterInfo.getCommandId
  private def tracelog(msg: String): Unit =
    logger.trace(s"[entryId=${Pretty.prettyEntryId(entryId)}, cmdId=$commandId]: $msg")

  private val txLet = parseTimestamp(txEntry.getLedgerEffectiveTime)
  private val submitterInfo = txEntry.getSubmitterInfo
  private val submitter = Party.assertFromString(submitterInfo.getSubmitter)
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
  private def deduplicateCommand: Commit[Unit] = {
    val dedupKey = commandDedupKey(submitterInfo)
    get(dedupKey).flatMap { dedupEntry =>
      if (dedupEntry.isEmpty) {
        Commit.set(
          dedupKey ->
            DamlStateValue.newBuilder
              .setCommandDedup(DamlCommandDedupValue.newBuilder.build)
              .build)
      } else
        reject(RejectionReason.DuplicateCommand)
    }
  }

  /** Authorize the submission by looking up the party allocation and verifying
    * that the submitting party is indeed hosted by the submitting participant.
    *
    * If the "open world" setting is enabled we allow the submission even if the
    * party is unallocated.
    */
  private def authorizeSubmitter: Commit[Unit] =
    get(partyStateKey(submitter)).flatMap {
      case Some(partyAllocation) =>
        if (partyAllocation.getParty.getParticipantId == participantId)
          pass
        else
          reject(
            RejectionReason.SubmitterCannotActViaParticipant(
              s"Party '$submitter' not hosted by participant $participantId"))
      case None =>
        if (config.openWorld)
          pass
        else
          reject(RejectionReason.PartyNotKnownOnLedger)
    }

  /** Validate ledger effective time and the command's time-to-live. */
  private def validateLetAndTtl: Commit[Unit] = delay {
    val timeModelChecker = TimeModelChecker(config.timeModel)
    val givenLET = txLet.toInstant
    val givenMRT = parseTimestamp(txEntry.getSubmitterInfo.getMaximumRecordTime).toInstant

    if (timeModelChecker.checkLet(
        currentTime = recordTime.toInstant,
        givenLedgerEffectiveTime = givenLET,
        givenMaximumRecordTime = givenMRT)
      /* NOTE(JM): This check has been disabled to be more lenient while
       * we're still in beta phase. Time model is being redesigned and
       * appropriate checks will be put back in place along with the new
       * implementation.
       *
       * && timeModelChecker.checkTtl(givenLET, givenMRT) */ )
      pass
    else
      reject(RejectionReason.MaximumRecordTimeExceeded)
  }

  /** Validate the submission's conformance to the DAML model */
  private def validateModelConformance: Commit[Unit] = delay {
    engine
      .validate(relTx, txLet)
      .consume(lookupContract, lookupPackage, lookupKey)
      .fold(err => reject(RejectionReason.Disputed(err.msg)), _ => pass)
  }

  /** Validate the submission's conformance to the DAML model */
  private def authorizeAndBlind: Commit[BlindingInfo] = delay {
    Blinding
      .checkAuthorizationAndBlind(relTx, initialAuthorizers = Set(submitter))
      .fold(err => reject(RejectionReason.Disputed(err.msg)), pure)
  }

  private def validateContractKeyUniqueness: Commit[Unit] =
    for {
      damlState <- getDamlState
      allUnique = relTx.fold(GenTransaction.AnyOrder, true) {
        case (allUnique, (_nodeId, create: NodeCreate[_, VersionedValue[ContractId]]))
            if create.key.isDefined =>
          val stateKey = Conversions.contractKeyToStateKey(
            GlobalKey(
              create.coinst.template,
              Conversions.forceAbsoluteContractIds(create.key.get.key)))

          allUnique &&
          damlState
            .get(stateKey)
            .forall(!_.getContractKeyState.hasContractId)

        case (allUnique, _) => allUnique
      }

      r <- if (allUnique)
        pass
      else
        reject(RejectionReason.Disputed("DuplicateKey: Contract Key not unique"))
    } yield r

  /** All checks passed. Produce the log entry and contract state updates. */
  private def buildFinalResult(blindingInfo: BlindingInfo): Commit[Unit] = delay {
    val effects = InputsAndEffects.computeEffects(entryId, relTx)

    // Helper to read the _current_ contract state.
    // NOTE(JM): Important to fetch from the state that is currently being built up since
    // we mark some contracts as archived and may later change their disclosure and do not
    // want to "unarchive" them.
    def getContractState(key: DamlStateKey): Commit[DamlContractState] =
      get(key).map {
        _.getOrElse(throw Err.MissingInputState(key)).getContractState
      }

    sequence(
      // Update contract state entries to mark contracts as consumed (checked by 'validateModelConformance')
      sequence(effects.consumedContracts.map { key =>
        for {
          cs <- getContractState(key).map { cs =>
            cs.toBuilder
              .setArchivedAt(buildTimestamp(txLet))
              .setArchivedByEntry(entryId)
          }
          r <- set(key -> DamlStateValue.newBuilder.setContractState(cs).build)
        } yield r
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
      sequence(blindingInfo.globalImplicitDisclosure.map {
        case (absCoid, parties) =>
          val key = absoluteContractIdToStateKey(absCoid)
          getContractState(key).flatMap { cs =>
            val divulged: Set[String] = cs.getDivulgedToList.asScala.toSet
            val newDivulgences: Set[String] = parties.toSet[String] -- divulged
            if (newDivulgences.isEmpty)
              pass
            else {
              val cs2 = cs.toBuilder
                .addAllDivulgedTo(newDivulgences.asJava)
              set(key -> DamlStateValue.newBuilder.setContractState(cs2).build)
            }
          }
      }.toList),
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
      !cs.hasArchivedAt && activeAt.exists(txLet >= _) || {
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
    val stateKey = packageStateKey(pkgId)
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
            case ParseError(err) => throw Err.DecodeError("Archive", err)
          }

        case _ =>
          throw Err.DecodeError("Archive", "lookupPackage($pkgId): value not a DAML-LF archive!")
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

  private def reject[A](reason: RejectionReason): Commit[A] = {

    val rejectionEntry = {
      val builder = DamlTransactionRejectionEntry.newBuilder
      builder
        .setSubmitterInfo(txEntry.getSubmitterInfo)

      reason match {
        case RejectionReason.Inconsistent =>
          builder.setInconsistent(
            DamlTransactionRejectionEntry.Inconsistent.newBuilder.setDetails(""))
        case RejectionReason.Disputed(disputeReason) =>
          builder.setDisputed(
            DamlTransactionRejectionEntry.Disputed.newBuilder.setDetails(disputeReason))
        case RejectionReason.ResourcesExhausted =>
          builder.setResourcesExhausted(
            DamlTransactionRejectionEntry.ResourcesExhausted.newBuilder.setDetails(""))
        case RejectionReason.MaximumRecordTimeExceeded =>
          builder.setMaximumRecordTimeExceeded(
            DamlTransactionRejectionEntry.MaximumRecordTimeExceeded.newBuilder.setDetails(""))
        case RejectionReason.DuplicateCommand =>
          builder.setDuplicateCommand(
            DamlTransactionRejectionEntry.DuplicateCommand.newBuilder.setDetails(""))
        case RejectionReason.PartyNotKnownOnLedger =>
          builder.setPartyNotKnownOnLedger(
            DamlTransactionRejectionEntry.PartyNotKnownOnLedger.newBuilder.setDetails(""))
        case RejectionReason.SubmitterCannotActViaParticipant(details) =>
          builder.setSubmitterCannotActViaParticipant(
            DamlTransactionRejectionEntry.SubmitterCannotActViaParticipant.newBuilder
              .setDetails(details))
      }
      builder
    }

    Commit.done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setTransactionRejectionEntry(rejectionEntry)
        .build,
    )
  }

}
