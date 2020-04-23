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
import com.daml.lf.archive.Decode
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{IdString, PackageId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Blinding, Engine}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Transaction.AbsTransaction
import com.daml.lf.transaction.{BlindingInfo, GenTransaction, Node}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.AbsoluteContractId
import com.google.protobuf.{Timestamp => ProtoTimestamp}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

// The parameter inStaticTimeMode indicates that the ledger is running in static time mode.
//
// Command deduplication is always based on wall clock time and not ledger time. In static time mode,
// record time cannot be used for command deduplication. This flag indicates that the system clock should
// be used as submission time for commands instead of record time.
//
// Other possible solutions that we discarded:
// * Pass in an additional time provider, but this hides the intent
// * Adding and additional submission field commandDedupSubmissionTime field. While having participants
//   provide this field *could* lead to possible exploits, they are not exploits that could do any harm.
//   The bigger concern is adding a public API for the specific use case of Sandbox with static time.
private[kvutils] class ProcessTransactionSubmission(
    defaultConfig: Configuration,
    engine: Engine,
    metricRegistry: MetricRegistry,
    inStaticTimeMode: Boolean
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
      "Validate Contract Keys" ->
        validateContractKeys(recordTime, transactionEntry),
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

  /** Reject duplicate commands
    */
  private def deduplicateCommand(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
  ): Commit[Unit] = {
    val dedupKey = commandDedupKey(transactionEntry.submitterInfo)
    get(dedupKey).flatMap { dedupEntry =>
      val submissionTime = if (inStaticTimeMode) Instant.now() else recordTime.toInstant
      if (dedupEntry.forall(isAfterDeduplicationTime(submissionTime, _))) {
        pass
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

  // Checks that the submission time of the command is after the
  // deduplicationTime represented by stateValue
  private def isAfterDeduplicationTime(
      submissionTime: Instant,
      stateValue: DamlStateValue): Boolean = {
    val cmdDedup = stateValue.getCommandDedup
    if (stateValue.hasCommandDedup && cmdDedup.hasDeduplicatedUntil) {
      val dedupTime = parseTimestamp(cmdDedup.getDeduplicatedUntil).toInstant
      dedupTime.isBefore(submissionTime)
    } else {
      false
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

  private def validateContractKeys(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
  ): Commit[Unit] =
    for {
      damlState <- getDamlState
      startingKeys = damlState.collect {
        case (k, v) if k.hasContractKey && v.getContractKeyState.getContractId.nonEmpty => k
      }.toSet
      _ <- validateContractKeyUniqueness(recordTime, transactionEntry, startingKeys)
      _ <- validateContractKeyCausalMonotonicity(
        recordTime,
        transactionEntry,
        startingKeys,
        damlState)
    } yield ()

  private def validateContractKeyUniqueness(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
      keys: Set[DamlStateKey]) = {
    val allUnique = transactionEntry.abs
      .fold((true, keys)) {
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

    if (allUnique)
      pass
    else
      reject(
        recordTime,
        buildRejectionLogEntry(
          transactionEntry,
          RejectionReason.Disputed("DuplicateKey: Contract Key not unique")))

  }

  /** LookupByKey nodes themselves don't actually fetch the contract.
    * Therefore we need to do an additional check on all contract keys
    * to ensure the referred contract satisfies the causal monotonicity invariant.
    * This could be reduced to only validate this for keys referred to by
    * NodeLookupByKey.
    */
  private def validateContractKeyCausalMonotonicity(
      recordTime: Timestamp,
      transactionEntry: TransactionEntry,
      keys: Set[DamlStateKey],
      damlState: DamlOutputStateMap) = {
    val causalKeyMonotonicity = keys.forall { key =>
      val state = damlState(key)
      val keyActiveAt =
        Conversions.parseTimestamp(state.getContractKeyState.getActiveAt).toInstant
      !keyActiveAt.isAfter(transactionEntry.ledgerEffectiveTime.toInstant)
    }
    if (causalKeyMonotonicity)
      pass
    else
      reject(recordTime, buildRejectionLogEntry(transactionEntry, RejectionReason.Inconsistent))
  }

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

    val dedupKey = commandDedupKey(transactionEntry.submitterInfo)

    val ledgerEffectiveTime = transactionEntry.txEntry.getLedgerEffectiveTime

    // Helper to read the _current_ contract state.
    // NOTE(JM): Important to fetch from the state that is currently being built up since
    // we mark some contracts as archived and may later change their disclosure and do not
    // want to "unarchive" them.
    def getContractState(key: DamlStateKey): Commit[DamlContractState] =
      get(key).map {
        _.getOrElse(throw Err.MissingInputState(key)).getContractState
      }

    sequence2(
      // Set a deduplication entry
      set(
        dedupKey -> DamlStateValue.newBuilder
          .setCommandDedup(
            DamlCommandDedupValue.newBuilder
              .setRecordTime(buildTimestamp(recordTime))
              .setDeduplicatedUntil(transactionEntry.submitterInfo.getDeduplicateUntil)
              .build)
          .build),
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
          updateContractKeyWithContractKeyState(ledgerEffectiveTime, key, contractKeyState)
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

  private def updateContractKeyWithContractKeyState(
      ledgerEffectiveTime: ProtoTimestamp,
      key: DamlStateKey,
      contractKeyState: Option[AbsoluteContractId]): (DamlStateKey, DamlStateValue) = {
    logger.trace(s"updating contract key $key to $contractKeyState")
    key ->
      DamlStateValue.newBuilder
        .setContractKeyState(
          contractKeyState
            .map(
              coid =>
                DamlContractKeyState.newBuilder
                  .setContractId(coid.coid)
                  .setActiveAt(ledgerEffectiveTime))
            .getOrElse(DamlContractKeyState.newBuilder())
        )
        .build
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
      // There is the possibility that the reinterpretation of the transaction yields a different
      // result in a LookupByKey than the original transaction. This means that the contract state data for the
      // contractId pointed to by that contractKey might not have been preloaded into the input state map.
      // This is not a problem because after the transaction reinterpretation, we compare the original
      // transaction with the reintrepreted one, and the LookupByKey node will not match.
      // Additionally, all contract keys are checked to uphold causal monotonicity.
      contractState <- inputState.get(stateKey).flatMap(_.map(_.getContractState))
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
    // we don't check whether the contract is active or not, because in we might not have loaded it earlier.
    // this is not a problem, because:
    // a) if the lookup was negative and we actually found a contract,
    //    the transaction validation will fail.
    // b) if the lookup was positive and its result is a different contract,
    //    the transaction validation will fail.
    // c) if the lookup was positive and its result is the same contract,
    //    - the authorization check ensures that the submitter is in fact allowed
    //      to lookup the contract
    //    - the separate contract keys check ensures that all contracts pointed to by
    //    contract keys respect causal monotonicity.
    val stateKey = Conversions.globalKeyToStateKey(key)
    val contractId = for {
      stateValue <- inputState.get(stateKey).flatten
      if stateValue.getContractKeyState.getContractId.nonEmpty
    } yield decodeContractId(stateValue.getContractKeyState.getContractId)

    // If the key was not in state inputs, then we look whether any of the accessed contracts has
    // the key we're looking for. This happens with "fetchByKey" where the key lookup is not
    // evidenced in the transaction. The activeness of the contract is checked when it is fetched.
    contractId.orElse {
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
