// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.codahale.metrics.Counter
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.kvutils.{Conversions, DamlStateMap, Err, InputsAndEffects}
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReason}
import com.daml.lf.archive.Decode
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.crypto
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Blinding, Engine}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{BlindingInfo, GenTransaction, Node}
import com.daml.lf.transaction.Transaction.AbsTransaction
import com.daml.lf.value.Value
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.metrics.Metrics
import com.google.protobuf.{Timestamp => ProtoTimestamp}

import scala.collection.JavaConverters._

private[kvutils] object TransactionCommitter {

  case class Digest(submission: DamlTransactionEntry) {
    val ledgerEffectiveTime: Timestamp = parseTimestamp(submission.getLedgerEffectiveTime)
    val submitterInfo: DamlSubmitterInfo = submission.getSubmitterInfo
    val commandId: String = submitterInfo.getCommandId
    val submitter: Party = Party.assertFromString(submitterInfo.getSubmitter)
    lazy val abs: AbsTransaction = Conversions.decodeTransaction(submission.getTransaction)
    val submissionTime: Timestamp = Conversions.parseTimestamp(submission.getSubmissionTime)
    val submissionSeed: Option[crypto.Hash] = Conversions.parseOptHash(submission.getSubmissionSeed)
  }

}

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
private[kvutils] class TransactionCommitter(
    defaultConfig: Configuration,
    engine: Engine,
    override protected val metrics: Metrics,
    inStaticTimeMode: Boolean
) extends Committer[DamlTransactionEntry, TransactionCommitter.Digest] {
  override protected val committerName = "transaction"

  override protected def init(
      ctx: CommitContext,
      transactionEntry: DamlTransactionEntry,
  ): TransactionCommitter.Digest =
    TransactionCommitter.Digest(transactionEntry)

  override protected val steps: Iterable[(StepInfo, Step)] = Iterable(
    "authorize_submitter" -> authorizeSubmitter,
    "check_informee_parties_allocation" -> checkInformeePartiesAllocation,
    "deduplicate" -> deduplicateCommand,
    "validate_ledger_time" -> validateLedgerTime,
    "validate_contract_keys" -> validateContractKeys,
    "validate_model_conformance" -> validateModelConformance,
    "authorize_and_build_result" -> authorizeAndBlind
  )

  // -------------------------------------------------------------------------------

  private def contractIsActiveAndVisibleToSubmitter(
      digest: TransactionCommitter.Digest,
      contractState: DamlContractState,
  ): Boolean = {
    val locallyDisclosedTo = contractState.getLocallyDisclosedToList.asScala
    val divulgedTo = contractState.getDivulgedToList.asScala
    val isVisible = locallyDisclosedTo.contains(digest.submitter) || divulgedTo.contains(
      digest.submitter)
    val isActive = {
      val activeAt = Option(contractState.getActiveAt).map(parseTimestamp)
      !contractState.hasArchivedAt && activeAt.exists(digest.ledgerEffectiveTime >= _)
    }
    isVisible && isActive
  }

  /** Reject duplicate commands
    */
  private def deduplicateCommand: Step = (ctx, digest) => {
    val dedupKey = commandDedupKey(digest.submitterInfo)
    val dedupEntry = ctx.get(dedupKey)
    val submissionTime = if (inStaticTimeMode) Instant.now() else ctx.getRecordTime.toInstant
    if (dedupEntry.forall(isAfterDeduplicationTime(submissionTime, _))) {
      StepContinue(digest)
    } else {
      logger.trace(s"Transaction rejected, duplicate command, correlationId=${digest.commandId}")
      reject(
        ctx.getRecordTime,
        DamlTransactionRejectionEntry.newBuilder
          .setSubmitterInfo(digest.submitterInfo)
          .setDuplicateCommand(Duplicate.newBuilder.setDetails(""))
      )
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
  private def authorizeSubmitter: Step = (ctx, digest) => {
    ctx.get(partyStateKey(digest.submitter)) match {
      case Some(partyAllocation) =>
        if (partyAllocation.getParty.getParticipantId == ctx.getParticipantId)
          StepContinue(digest)
        else
          reject(
            ctx.getRecordTime,
            buildRejectionLogEntry(
              digest,
              RejectionReason.SubmitterCannotActViaParticipant(
                s"Party '${digest.submitter}' not hosted by participant $ctx.getParticipantId"))
          )
      case None =>
        reject(
          ctx.getRecordTime,
          buildRejectionLogEntry(
            digest,
            RejectionReason.PartyNotKnownOnLedger(
              s"Submitting party '${digest.submitter}' not known"))
        )
    }
  }

  /** Validate ledger effective time and the command's time-to-live. */
  private def validateLedgerTime: Step = (ctx, digest) => {
    val (_, config) = getCurrentConfiguration(defaultConfig, ctx.inputs, logger)
    val timeModel = config.timeModel
    val givenLedgerTime = digest.ledgerEffectiveTime.toInstant

    timeModel
      .checkTime(ledgerTime = givenLedgerTime, recordTime = ctx.getRecordTime.toInstant)
      .fold(
        reason =>
          reject(
            ctx.getRecordTime,
            buildRejectionLogEntry(digest, RejectionReason.InvalidLedgerTime(reason))),
        _ => StepContinue(digest))
  }

  /** Validate the submission's conformance to the DAML model */
  private def validateModelConformance: Step =
    (ctx, digest) =>
      metrics.daml.kvutils.committer.transaction.interpretTimer.time(() => {
        // Pull all keys from referenced contracts. We require this for 'fetchByKey' calls
        // which are not evidenced in the transaction itself and hence the contract key state is
        // not included in the inputs.
        lazy val knownKeys: Map[DamlContractKey, Value.AbsoluteContractId] =
          ctx.inputs.collect {
            case (key, Some(value))
                if value.hasContractState
                  && value.getContractState.hasContractKey
                  && contractIsActiveAndVisibleToSubmitter(digest, value.getContractState) =>
              value.getContractState.getContractKey -> Conversions.stateKeyToContractId(key)
          }

        engine
          .validate(
            digest.abs,
            digest.ledgerEffectiveTime,
            ctx.getParticipantId,
            digest.submissionTime,
            digest.submissionSeed,
          )
          .consume(
            lookupContract(digest, ctx.inputs),
            lookupPackage(digest, ctx.inputs),
            lookupKey(digest, ctx.inputs, knownKeys),
          )
          .fold(
            err =>
              reject[TransactionCommitter.Digest](
                ctx.getRecordTime,
                buildRejectionLogEntry(digest, RejectionReason.Disputed(err.msg))),
            _ => StepContinue[TransactionCommitter.Digest](digest)
          )
      })

  /** Validate the submission's conformance to the DAML model */
  private def authorizeAndBlind: Step =
    (ctx, digest) =>
      Blinding
        .checkAuthorizationAndBlind(
          digest.abs,
          initialAuthorizers = Set(digest.submitter),
        )
        .fold(
          err =>
            reject(
              ctx.getRecordTime,
              buildRejectionLogEntry(digest, RejectionReason.Disputed(err.msg))),
          succ => buildFinalResult(ctx, digest, succ))

  private def validateContractKeys: Step = (ctx, digest) => {
    val damlState = ctx.inputs.collect { case (k, Some(v)) => k -> v } ++ ctx.getOutputs
    val startingKeys = damlState.collect {
      case (k, v) if k.hasContractKey && v.getContractKeyState.getContractId.nonEmpty => k
    }.toSet
    validateContractKeyUniqueness(ctx.getRecordTime, digest, startingKeys) match {
      case StepContinue(digest) =>
        validateContractKeyCausalMonotonicity(ctx.getRecordTime, digest, startingKeys, damlState)
      case err => err
    }

  }

  private def validateContractKeyUniqueness(
      recordTime: Timestamp,
      digest: TransactionCommitter.Digest,
      keys: Set[DamlStateKey]): StepResult[TransactionCommitter.Digest] = {
    val allUnique = digest.abs
      .fold((true, keys)) {
        case (
            (allUnique, existingKeys),
            (_, exe @ Node.NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _, _)))
            if exe.key.isDefined && exe.consuming =>
          val stateKey = Conversions.globalKeyToStateKey(
            Node.GlobalKey(exe.templateId, Conversions.forceNoContractIds(exe.key.get.key.value)))
          (allUnique, existingKeys - stateKey)

        case ((allUnique, existingKeys), (_, create @ Node.NodeCreate(_, _, _, _, _, _)))
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
      StepContinue(digest)
    else
      reject(
        recordTime,
        buildRejectionLogEntry(
          digest,
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
      digest: TransactionCommitter.Digest,
      keys: Set[DamlStateKey],
      damlState: Map[DamlStateKey, DamlStateValue]): StepResult[TransactionCommitter.Digest] = {
    val causalKeyMonotonicity = keys.forall { key =>
      val state = damlState(key)
      val keyActiveAt =
        Conversions.parseTimestamp(state.getContractKeyState.getActiveAt).toInstant
      !keyActiveAt.isAfter(digest.ledgerEffectiveTime.toInstant)
    }
    if (causalKeyMonotonicity)
      StepContinue(digest)
    else
      reject(
        recordTime,
        buildRejectionLogEntry(
          digest,
          RejectionReason.Inconsistent("Causal monotonicity violated")))
  }

  /** Check that all informee parties mentioned of a transaction are allocated. */
  private def checkInformeePartiesAllocation: Step = (ctx, digest) => {
    def foldInformeeParties(tx: GenTransaction.WithTxValue[_, _], init: Boolean)(
        f: (Boolean, String) => Boolean
    ): Boolean =
      tx.fold(init) {
        case (accum, (_, node)) =>
          node.informeesOfNode.foldLeft(accum)(f)
      }

    val allExist = foldInformeeParties(digest.abs, init = true) { (accum, party) =>
      ctx.get(partyStateKey(party)).fold(false)(_ => accum)
    }

    if (allExist)
      StepContinue(digest)
    else
      reject(
        ctx.getRecordTime,
        buildRejectionLogEntry(
          digest,
          RejectionReason.PartyNotKnownOnLedger("Not all parties known"))
      )
  }

  /** All checks passed. Produce the log entry and contract state updates. */
  private def buildFinalResult(
      ctx: CommitContext,
      digest: TransactionCommitter.Digest,
      blindingInfo: BlindingInfo
  ): StepResult[TransactionCommitter.Digest] = {
    val effects = InputsAndEffects.computeEffects(digest.abs)

    val cid2nid: Value.AbsoluteContractId => Value.NodeId = digest.abs.localContracts

    val dedupKey = commandDedupKey(digest.submitterInfo)

    val ledgerEffectiveTime = digest.submission.getLedgerEffectiveTime

    // Helper to read the _current_ contract state.
    // NOTE(JM): Important to fetch from the state that is currently being built up since
    // we mark some contracts as archived and may later change their disclosure and do not
    // want to "unarchive" them.
    def getContractState(key: DamlStateKey): DamlContractState =
      ctx.get(key).getOrElse(throw Err.MissingInputState(key)).getContractState

    // Set a deduplication entry
    ctx.set(
      dedupKey,
      DamlStateValue.newBuilder
        .setCommandDedup(
          DamlCommandDedupValue.newBuilder
            .setRecordTime(buildTimestamp(ctx.getRecordTime))
            .setDeduplicatedUntil(digest.submitterInfo.getDeduplicateUntil)
            .build)
        .build
    )

    // Add contract state entries to mark contract activeness (checked by 'validateModelConformance')
    effects.createdContracts.foreach {
      case (key, createNode) =>
        val cs = DamlContractState.newBuilder
        cs.setActiveAt(buildTimestamp(digest.ledgerEffectiveTime))
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
        ctx.set(key, DamlStateValue.newBuilder.setContractState(cs).build)
    }

    // Update contract state entries to mark contracts as consumed (checked by 'validateModelConformance')
    effects.consumedContracts.foreach { key =>
      val cs = getContractState(key)
      ctx.set(
        key,
        DamlStateValue.newBuilder
          .setContractState(
            cs.toBuilder
              .setArchivedAt(buildTimestamp(digest.ledgerEffectiveTime))
              .setArchivedByEntry(ctx.getEntryId)
          )
          .build
      )
    }

    // Update contract state of divulged contracts
    blindingInfo.globalDivulgence.foreach {
      case (coid, parties) =>
        val key = contractIdToStateKey(coid)
        val cs = getContractState(key)
        val divulged: Set[String] = cs.getDivulgedToList.asScala.toSet
        val newDivulgences: Set[String] = parties.toSet[String] -- divulged
        if (newDivulgences.nonEmpty) {
          val cs2 = cs.toBuilder
            .addAllDivulgedTo(newDivulgences.asJava)
          ctx.set(key, DamlStateValue.newBuilder.setContractState(cs2).build)
        }
    }

    // Update contract keys
    effects.updatedContractKeys.foreach {
      case (key, contractKeyState) =>
        val (k, v) =
          updateContractKeyWithContractKeyState(ledgerEffectiveTime, key, contractKeyState)
        ctx.set(k, v)
    }

    metrics.daml.kvutils.committer.transaction.accepts.inc()
    logger.trace(s"Transaction accepted, correlationId=${digest.commandId}")
    StepStop(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(ctx.getRecordTime))
        .setTransactionEntry(digest.submission)
        .build
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
  private def lookupContract(digest: TransactionCommitter.Digest, inputState: DamlStateMap)(
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
      if contractIsActiveAndVisibleToSubmitter(digest, contractState)
      contract = Conversions.decodeContractInstance(contractState.getContractInstance)
    } yield contract
  }

  // Helper to lookup package from the state. The package contents
  // are stored in the [[DamlLogEntry]], which we find by looking up
  // the DAML state entry at `DamlStateKey(packageId = pkgId)`.
  private def lookupPackage(
      digest: TransactionCommitter.Digest,
      inputState: DamlStateMap,
  )(pkgId: PackageId): Option[Ast.Package] = {
    val stateKey = packageStateKey(pkgId)
    for {
      value <- inputState
        .get(stateKey)
        .flatten
        .orElse {
          logger.warn(
            s"Lookup package failed, package not found, packageId=$pkgId correlationId=${digest.commandId}")
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
                s"Decode archive failed, packageId=$pkgId correlationId=${digest.commandId}")
              throw Err.DecodeError("Archive", err)
          }

        case _ =>
          val msg = s"value not a DAML-LF archive"
          logger.warn(
            s"Lookup package failed, $msg, packageId=$pkgId correlationId=${digest.commandId}")
          throw Err.DecodeError("Archive", msg)
      }

    } yield pkg
  }

  private def lookupKey(
      digest: TransactionCommitter.Digest,
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
      digest: TransactionCommitter.Digest,
      reason: RejectionReason,
  ): DamlTransactionRejectionEntry.Builder = {
    logger.trace(s"Transaction rejected, ${reason.description}, correlationId=${digest.commandId}")
    val builder = DamlTransactionRejectionEntry.newBuilder
    builder
      .setSubmitterInfo(digest.submitterInfo)

    reason match {
      case RejectionReason.Inconsistent(reason) =>
        builder.setInconsistent(Inconsistent.newBuilder.setDetails(reason))
      case RejectionReason.Disputed(reason) =>
        builder.setDisputed(Disputed.newBuilder.setDetails(reason))
      case RejectionReason.ResourcesExhausted(reason) =>
        builder.setResourcesExhausted(ResourcesExhausted.newBuilder.setDetails(reason))
      case RejectionReason.PartyNotKnownOnLedger(reason) =>
        builder.setPartyNotKnownOnLedger(PartyNotKnownOnLedger.newBuilder.setDetails(reason))
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
  ): StepResult[A] = {
    Metrics.rejections(rejectionEntry.getReasonCase.getNumber).inc()
    StepStop(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setTransactionRejectionEntry(rejectionEntry)
        .build,
    )
  }

  private object Metrics {
    val rejections: Map[Int, Counter] =
      DamlTransactionRejectionEntry.ReasonCase.values
        .map(v => v.getNumber -> metrics.daml.kvutils.committer.transaction.rejection(v.name()))
        .toMap
  }
}
