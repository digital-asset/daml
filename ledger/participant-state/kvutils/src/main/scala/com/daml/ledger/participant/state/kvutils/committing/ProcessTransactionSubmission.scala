// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import java.time.Instant

import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committing.Common.Commit._
import com.daml.ledger.participant.state.kvutils.committing.Common._
import com.daml.ledger.participant.state.kvutils.committing.ProcessTransactionSubmission._
import com.daml.ledger.participant.state.kvutils.{Conversions, DamlStateMap, Err, InputsAndEffects}
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, RejectionReason}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{IdString, PackageId, Party}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{Blinding, Engine}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Transaction.AbsTransaction
import com.digitalasset.daml.lf.transaction.{BlindingInfo, GenTransaction, Node}
import com.digitalasset.daml.lf.value.Value
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

private[kvutils] class ProcessTransactionSubmission(
    defaultConfig: Configuration,
    engine: Engine,
    metricRegistry: MetricRegistry,
) {

  private implicit val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def run(
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      participantId: ParticipantId,
      txEntry: DamlTransactionEntry,
      inputState: DamlStateMap,
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = Metrics.runTimer.time { () =>
    val transactionEntry = TransactionEntry(txEntry)
    runSequence(
      inputState = inputState,
      "Authorize submitter" -> authorizeSubmitter(recordTime, participantId, transactionEntry),
      "Check Informee Parties Allocation" ->
        checkInformeePartiesAllocation(recordTime, transactionEntry),
      "Deduplicate" -> deduplicateCommand(recordTime, transactionEntry),
      "Validate Ledger Time" -> validateLedgerTime(recordTime, transactionEntry, inputState),
      "Validate Contract Key Uniqueness" ->
        validateContractKeyUniqueness(recordTime, transactionEntry),
      "Validate Model Conformance" -> timed(
        Metrics.interpretTimer,
        validateModelConformance(engine, recordTime, participantId, transactionEntry, inputState),
      ),
      "Authorize and build result" ->
        authorizeAndBlind(recordTime, transactionEntry).flatMap(
          buildFinalResult(entryId, recordTime, transactionEntry))
    )
  }

  // -------------------------------------------------------------------------------

  private def contractIsActiveAndVisibleToSubmitter(
      transactionEntry: TransactionEntry,
      contractState: DamlContractState,
  ): Boolean = {
    val locallyDisclosedTo = contractState.getLocallyDisclosedToList.asScala
    val divulgedTo = contractState.getDivulgedToList.asScala
    val isVisible = locallyDisclosedTo.contains(transactionEntry.submitter) || divulgedTo.contains(
      transactionEntry.submitter)
    val isActive = {
      val activeAt = Option(contractState.getActiveAt).map(parseTimestamp)
      !contractState.hasArchivedAt && activeAt.exists(transactionEntry.ledgerEffectiveTime >= _)
    }
    isVisible && isActive
  }

  /** Deduplicate the submission. If the check passes we save the command deduplication
    * state.
    */
  private def deduplicateCommand(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
  ): Commit[Unit] = {
    val dedupKey = commandDedupKey(transactionEntry.submitterInfo)
    get(dedupKey).flatMap { dedupEntry =>
      def isAfterDeduplicationTime(stateValue: DamlStateValue): Boolean = {
        lazy val cmdDedup = stateValue.getCommandDedup
        lazy val dedupTime = parseTimestamp(cmdDedup.getDeduplicatedUntil).toInstant
        !stateValue.hasCommandDedup || !cmdDedup.hasDeduplicatedUntil || dedupTime.isBefore(
          recordTime.toInstant)
      }
      if (dedupEntry.forall(isAfterDeduplicationTime)) {
        Commit.set(
          dedupKey ->
            DamlStateValue.newBuilder
              .setCommandDedup(
                DamlCommandDedupValue.newBuilder
                  .setRecordTime(buildTimestamp(recordTime))
                  .setDeduplicatedUntil(transactionEntry.submitterInfo.getDeduplicateUntil)
                  .build)
              .build)
      } else {
        logger.trace(
          s"Transaction rejected, duplicate command, correlationId=${transactionEntry.commandId}")
        reject(
          recordTime,
          DamlTransactionRejectionEntry.newBuilder
            .setSubmitterInfo(transactionEntry.submitterInfo)
            .setDuplicateCommand(Duplicate.newBuilder.setDetails(""))
        )
      }
    }
  }

  /** Authorize the submission by looking up the party allocation and verifying
    * that the submitting party is indeed hosted by the submitting participant.
    *
    * If the "open world" setting is enabled we allow the submission even if the
    * party is unallocated.
    */
  private def authorizeSubmitter(
      recordTime: Timestamp,
      participantId: ParticipantId,
      transactionEntry: TransactionEntry,
  ): Commit[Unit] =
    get(partyStateKey(transactionEntry.submitter)).flatMap {
      case Some(partyAllocation) =>
        if (partyAllocation.getParty.getParticipantId == participantId)
          pass
        else
          reject(
            recordTime,
            buildRejectionLogEntry(
              transactionEntry,
              RejectionReason.SubmitterCannotActViaParticipant(
                s"Party '${transactionEntry.submitter}' not hosted by participant $participantId"))
          )
      case None =>
        reject(
          recordTime,
          buildRejectionLogEntry(transactionEntry, RejectionReason.PartyNotKnownOnLedger))
    }

  /** Validate ledger effective time and the command's time-to-live. */
  private def validateLedgerTime(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
      inputState: DamlStateMap,
  ): Commit[Unit] = delay {
    val (_, config) = Common.getCurrentConfiguration(defaultConfig, inputState, logger)
    val timeModel = config.timeModel
    val givenLedgerTime = transactionEntry.ledgerEffectiveTime.toInstant

    timeModel
      .checkTime(ledgerTime = givenLedgerTime, recordTime = recordTime.toInstant)
      .fold(
        reason =>
          reject(
            recordTime,
            buildRejectionLogEntry(transactionEntry, RejectionReason.InvalidLedgerTime(reason))),
        _ => pass)
  }

  /** Validate the submission's conformance to the DAML model */
  private def validateModelConformance(
      engine: Engine,
      recordTime: Timestamp,
      participantId: ParticipantId,
      transactionEntry: TransactionEntry,
      inputState: DamlStateMap,
  ): Commit[Unit] = delay {
    // Pull all keys from referenced contracts. We require this for 'fetchByKey' calls
    // which are not evidenced in the transaction itself and hence the contract key state is
    // not included in the inputs.
    lazy val knownKeys: Map[DamlContractKey, Value.AbsoluteContractId] =
      inputState.collect {
        case (key, Some(value))
            if value.hasContractState
              && value.getContractState.hasContractKey
              && contractIsActiveAndVisibleToSubmitter(transactionEntry, value.getContractState) =>
          value.getContractState.getContractKey -> Conversions.stateKeyToContractId(key)
      }

    val ctx = Metrics.interpretTimer.time()
    try {
      engine
        .validate(
          transactionEntry.abs,
          transactionEntry.ledgerEffectiveTime,
          participantId,
          transactionEntry.submissionSeedAndTime,
        )
        .consume(
          lookupContract(transactionEntry, inputState),
          lookupPackage(transactionEntry, inputState),
          lookupKey(transactionEntry, inputState, knownKeys),
        )
        .fold(
          err =>
            reject(
              recordTime,
              buildRejectionLogEntry(transactionEntry, RejectionReason.Disputed(err.msg))),
          _ => pass)
    } finally {
      val _ = ctx.stop()
    }
  }

  /** Validate the submission's conformance to the DAML model */
  private def authorizeAndBlind(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
  ): Commit[BlindingInfo] = delay {
    Blinding
      .checkAuthorizationAndBlind(
        transactionEntry.abs,
        initialAuthorizers = Set(transactionEntry.submitter),
      )
      .fold(
        err =>
          reject(
            recordTime,
            buildRejectionLogEntry(transactionEntry, RejectionReason.Disputed(err.msg))),
        pure)
  }

  private def validateContractKeyUniqueness(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
  ): Commit[Unit] =
    for {
      damlState <- getDamlState
      startingKeys = damlState.collect {
        case (k, v) if k.hasContractKey && v.getContractKeyState.getContractId.nonEmpty => k
      }.toSet

      allUnique = transactionEntry.abs
        .fold((true, startingKeys)) {
          case (
              (allUnique, existingKeys),
              (_, exe @ Node.NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _, _, _)))
              if exe.key.isDefined && exe.consuming =>
            val stateKey = Conversions.globalKeyToStateKey(
              Node.GlobalKey(exe.templateId, Conversions.forceNoContractIds(exe.key.get.key.value)))
            (allUnique, existingKeys - stateKey)

          case ((allUnique, existingKeys), (_, create @ Node.NodeCreate(_, _, _, _, _, _, _)))
              if create.key.isDefined =>
            val stateKey = Conversions.globalKeyToStateKey(
              Node.GlobalKey(
                create.coinst.template,
                Conversions.forceNoContractIds(create.key.get.key.value)))

            (allUnique && !existingKeys.contains(stateKey), existingKeys + stateKey)

          case (accum, _) => accum
        }
        ._1

      r <- if (allUnique)
        pass
      else
        reject(
          recordTime,
          buildRejectionLogEntry(
            transactionEntry,
            RejectionReason.Disputed("DuplicateKey: Contract Key not unique")))
    } yield r

  /** Check that all informee parties mentioned of a transaction are allocated. */
  private def checkInformeePartiesAllocation(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
  ): Commit[Unit] = {
    def foldInformeeParties[T](tx: GenTransaction.WithTxValue[_, _], init: T)(
        f: (T, String) => T
    ): T =
      tx.fold(init) {
        case (accum, (_, node)) =>
          node.informeesOfNode.foldLeft(accum)(f)
      }

    for {
      allExist <- foldInformeeParties(transactionEntry.abs, pure(true)) { (accum, party) =>
        get(partyStateKey(party)).flatMap(_.fold(pure(false))(_ => accum))
      }

      result <- if (allExist)
        pass
      else
        reject(
          recordTime,
          buildRejectionLogEntry(transactionEntry, RejectionReason.PartyNotKnownOnLedger)
        )
    } yield result
  }

  /** All checks passed. Produce the log entry and contract state updates. */
  private def buildFinalResult(
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
  )(blindingInfo: BlindingInfo): Commit[Unit] = delay {
    val effects = InputsAndEffects.computeEffects(transactionEntry.abs)

    val cid2nid: Value.AbsoluteContractId => Value.NodeId = transactionEntry.abs.localContracts

    // Helper to read the _current_ contract state.
    // NOTE(JM): Important to fetch from the state that is currently being built up since
    // we mark some contracts as archived and may later change their disclosure and do not
    // want to "unarchive" them.
    def getContractState(key: DamlStateKey): Commit[DamlContractState] =
      get(key).map {
        _.getOrElse(throw Err.MissingInputState(key)).getContractState
      }

    sequence2(
      // Add contract state entries to mark contract activeness (checked by 'validateModelConformance')
      set(effects.createdContracts.map {
        case (key, createNode) =>
          val cs = DamlContractState.newBuilder
          cs.setActiveAt(buildTimestamp(transactionEntry.ledgerEffectiveTime))
          val localDisclosure =
            blindingInfo.localDisclosure(cid2nid(decodeContractId(key.getContractId)))
          cs.addAllLocallyDisclosedTo((localDisclosure: Iterable[String]).asJava)
          cs.setContractInstance(
            Conversions.encodeContractInstance(createNode.coinst)
          )
          createNode.key.foreach { keyWithMaintainers =>
            cs.setContractKey(
              Conversions.encodeGlobalKey(
                Node.GlobalKey
                  .build(
                    createNode.coinst.template,
                    keyWithMaintainers.key.value
                  )
                  .fold(
                    _ => throw Err.InvalidSubmission("Unexpected contract id in contract key."),
                    identity))
            )
          }
          key -> DamlStateValue.newBuilder.setContractState(cs).build
      }),
      // Update contract state entries to mark contracts as consumed (checked by 'validateModelConformance')
      sequence2(effects.consumedContracts.map { key =>
        for {
          cs <- getContractState(key).map { cs =>
            cs.toBuilder
              .setArchivedAt(buildTimestamp(transactionEntry.ledgerEffectiveTime))
              .setArchivedByEntry(entryId)
          }
          r <- set(key -> DamlStateValue.newBuilder.setContractState(cs).build)
        } yield r
      }: _*),
      // Update contract state of divulged contracts
      sequence2(blindingInfo.globalDivulgence.map {
        case (coid, parties) =>
          val key = contractIdToStateKey(coid)
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
      }.toList: _*),
      // Update contract keys
      set(effects.updatedContractKeys.map {
        case (key, contractKeyState) =>
          logger.trace(s"updating contract key $key to $contractKeyState")
          key ->
            DamlStateValue.newBuilder
              .setContractKeyState(
                DamlContractKeyState.newBuilder.setContractId(contractKeyState.fold("")(_.coid))
              )
              .build
      }),
      delay {
        Metrics.accepts.inc()
        logger.trace(s"Transaction accepted, correlationId=${transactionEntry.commandId}")
        done(
          DamlLogEntry.newBuilder
            .setRecordTime(buildTimestamp(recordTime))
            .setTransactionEntry(transactionEntry.txEntry)
            .build
        )
      }
    )
  }

  // Helper to lookup contract instances. We verify the activeness of
  // contract instances here. Since we look up every contract that was
  // an input to a transaction, we do not need to verify the inputs separately.
  private def lookupContract(transactionEntry: TransactionEntry, inputState: DamlStateMap)(
      coid: Value.AbsoluteContractId,
  ): Option[Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]] = {
    val stateKey = contractIdToStateKey(coid)
    for {
      // Fetch the state of the contract so that activeness and visibility can be checked.
      contractState <- inputState.get(stateKey).flatMap(_.map(_.getContractState)).orElse {
        logger.warn(
          s"Lookup contract failed, contractId=$coid correlationId=${transactionEntry.commandId}")
        throw Err.MissingInputState(stateKey)
      }
      if contractIsActiveAndVisibleToSubmitter(transactionEntry, contractState)
      contract = Conversions.decodeContractInstance(contractState.getContractInstance)
    } yield contract
  }

  // Helper to lookup package from the state. The package contents
  // are stored in the [[DamlLogEntry]], which we find by looking up
  // the DAML state entry at `DamlStateKey(packageId = pkgId)`.
  private def lookupPackage(
      transactionEntry: TransactionEntry,
      inputState: DamlStateMap,
  )(pkgId: PackageId): Option[Ast.Package] = {
    val stateKey = packageStateKey(pkgId)
    for {
      value <- inputState
        .get(stateKey)
        .flatten
        .orElse {
          logger.warn(
            s"Lookup package failed, package not found, packageId=$pkgId correlationId=${transactionEntry.commandId}")
          throw Err.MissingInputState(stateKey)
        }
      pkg <- value.getValueCase match {
        case DamlStateValue.ValueCase.ARCHIVE =>
          // NOTE(JM): Engine only looks up packages once, compiles and caches,
          // provided that the engine instance is persisted.
          try {
            Some(Decode.decodeArchive(value.getArchive)._2)
          } catch {
            case ParseError(err) =>
              logger.warn(
                s"Decode archive failed, packageId=$pkgId correlationId=${transactionEntry.commandId}")
              throw Err.DecodeError("Archive", err)
          }

        case _ =>
          val msg = s"value not a DAML-LF archive"
          logger.warn(
            s"Lookup package failed, $msg, packageId=$pkgId correlationId=${transactionEntry.commandId}")
          throw Err.DecodeError("Archive", msg)
      }

    } yield pkg
  }

  private def lookupKey(
      transactionEntry: TransactionEntry,
      inputState: DamlStateMap,
      knownKeys: Map[DamlContractKey, Value.AbsoluteContractId],
  )(key: Node.GlobalKey): Option[Value.AbsoluteContractId] = {
    val stateKey = Conversions.globalKeyToStateKey(key)
    inputState
      .get(stateKey)
      .flatMap {
        _.flatMap { value =>
          for {
            contractId <- Some(value.getContractKeyState.getContractId)
              .filter(_.nonEmpty)
              .map(decodeContractId)
            contractStateKey = contractIdToStateKey(contractId)
            contractState <- inputState.get(contractStateKey).flatMap(_.map(_.getContractState))
            if contractIsActiveAndVisibleToSubmitter(transactionEntry, contractState)
          } yield contractId
        }
      }
      // If the key was not in state inputs, then we look whether any of the accessed contracts has
      // the key we're looking for. This happens with "fetchByKey" where the key lookup is not
      // evidenced in the transaction. The activeness of the contract is checked when it is fetched.
      .orElse {
        knownKeys.get(stateKey.getContractKey)
      }
  }

  private def buildRejectionLogEntry(
      transactionEntry: TransactionEntry,
      reason: RejectionReason,
  ): DamlTransactionRejectionEntry.Builder = {
    logger.trace(
      s"Transaction rejected, ${reason.description}, correlationId=${transactionEntry.commandId}")
    val builder = DamlTransactionRejectionEntry.newBuilder
    builder
      .setSubmitterInfo(transactionEntry.submitterInfo)

    reason match {
      case RejectionReason.Inconsistent =>
        builder.setInconsistent(Inconsistent.newBuilder.setDetails(""))
      case RejectionReason.Disputed(disputeReason) =>
        builder.setDisputed(Disputed.newBuilder.setDetails(disputeReason))
      case RejectionReason.ResourcesExhausted =>
        builder.setResourcesExhausted(ResourcesExhausted.newBuilder.setDetails(""))
      case RejectionReason.PartyNotKnownOnLedger =>
        builder.setPartyNotKnownOnLedger(PartyNotKnownOnLedger.newBuilder.setDetails(""))
      case RejectionReason.SubmitterCannotActViaParticipant(details) =>
        builder.setSubmitterCannotActViaParticipant(
          SubmitterCannotActViaParticipant.newBuilder
            .setDetails(details))
      case RejectionReason.InvalidLedgerTime(reason) =>
        builder.setInvalidLedgerTime(InvalidLedgerTime.newBuilder.setDetails(reason))
    }
    builder
  }

  private def reject[A](
      recordTime: Timestamp,
      rejectionEntry: DamlTransactionRejectionEntry.Builder,
  ): Commit[A] = {
    Metrics.rejections(rejectionEntry.getReasonCase.getNumber).inc()
    Commit.done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setTransactionRejectionEntry(rejectionEntry)
        .build,
    )
  }

  private object Metrics {
    private val prefix = kvutils.MetricPrefix :+ "committer" :+ "transaction"

    val runTimer: Timer = metricRegistry.timer(prefix :+ "run_timer")
    val interpretTimer: Timer = metricRegistry.timer(prefix :+ "interpret_timer")
    val accepts: Counter = metricRegistry.counter(prefix :+ "accepts")
    val rejections: Map[Int, Counter] =
      DamlTransactionRejectionEntry.ReasonCase.values
        .map(v => v.getNumber -> metricRegistry.counter(prefix :+ s"rejections_${v.name}"))
        .toMap
  }
}

object ProcessTransactionSubmission {

  private case class TransactionEntry(txEntry: DamlTransactionEntry) {
    val ledgerEffectiveTime: Timestamp = parseTimestamp(txEntry.getLedgerEffectiveTime)
    val submitterInfo: DamlSubmitterInfo = txEntry.getSubmitterInfo
    val commandId: String = submitterInfo.getCommandId
    val submitter: IdString.Party = Party.assertFromString(submitterInfo.getSubmitter)
    lazy val abs: AbsTransaction = Conversions.decodeTransaction(txEntry.getTransaction)
    val submissionSeedAndTime: Option[(Hash, Timestamp)] =
      Conversions
        .parseOptHash(txEntry.getSubmissionSeed)
        .map(_ -> Conversions.parseTimestamp(txEntry.getSubmissionTime))
  }

}
