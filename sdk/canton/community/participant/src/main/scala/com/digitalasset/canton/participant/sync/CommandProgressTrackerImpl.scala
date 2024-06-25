// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.ledger.api.v2.admin.command_inspection_service.CommandState
import com.daml.ledger.api.v2.admin.command_inspection_service.GetCommandStatusResponse.CommandStatus.{
  CommandUpdates,
  RequestStatistics,
}
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.daml.lf.data.Ref.TypeConName
import com.digitalasset.daml.lf.transaction.Node.LeafOnlyAction
import com.digitalasset.daml.lf.transaction.Transaction.ChildrenRecursion
import com.digitalasset.daml.lf.transaction.{GlobalKeyWithMaintainers, Node}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.CommandProgressTrackerConfig.{
  defaultMaxFailed,
  defaultMaxPending,
  defaultMaxSucceeded,
}
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandProgressTracker,
  CommandResultHandle,
  CommandStatus,
}
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.protocol.LfSubmittedTransaction
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import io.grpc.StatusRuntimeException
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

final case class CommandProgressTrackerConfig(
    enabled: Boolean = true,
    maxFailed: NonNegativeInt = defaultMaxFailed,
    maxPending: NonNegativeInt = defaultMaxPending,
    maxSucceeded: NonNegativeInt = defaultMaxSucceeded,
)

object CommandProgressTrackerConfig {
  lazy val defaultMaxFailed: NonNegativeInt = NonNegativeInt.tryCreate(100)
  lazy val defaultMaxPending: NonNegativeInt = NonNegativeInt.tryCreate(1000)
  lazy val defaultMaxSucceeded: NonNegativeInt = NonNegativeInt.tryCreate(100)
}

@SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
class CommandProgressTrackerImpl(
    config: CommandProgressTrackerConfig,
    clock: Clock,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandProgressTracker
    with NamedLogging {

  private case class MyCommandResultHandle(key: CommandKey, initial: CommandStatus)
      extends CommandResultHandle {
    val ref = new AtomicReference[CommandStatus](initial)
    private def updateWithStatus(
        err: com.google.rpc.status.Status,
        state: CommandState,
    ): Unit =
      ref.updateAndGet { x =>
        x
          .focus(_.completion.status)
          .replace(Some(err))
          .copy(
            state = state,
            completed = Some(clock.now),
          )
      }.discard

    private def processSyncErr(err: com.google.rpc.status.Status): Unit = {
      // remove from pending
      lock.synchronized(pending.remove(key)).foreach { cur =>
        if (config.maxFailed.value > 0) {
          updateWithStatus(err, CommandState.COMMAND_STATE_FAILED)
          addToCollection(cur.ref.get(), failed, config.maxFailed.value)
        }
      }
    }

    def failedSync(err: StatusRuntimeException): Unit = {
      val tmp =
        com.google.rpc.status.Status.fromJavaProto(io.grpc.protobuf.StatusProto.fromThrowable(err))
      processSyncErr(tmp)
    }
    def internalErrorSync(err: Throwable): Unit =
      processSyncErr(encodeInternalError(err))
    private def encodeInternalError(err: Throwable): com.google.rpc.status.Status =
      com.google.rpc.status.Status
        .of(com.google.rpc.Code.INTERNAL_VALUE, err.getMessage, Seq.empty)

    def failedAsync(
        status: Option[com.google.rpc.status.Status]
    ): Unit =
      updateWithStatus(
        status.getOrElse(
          encodeInternalError(new IllegalStateException("Missing status upon failed completion"))
        ),
        CommandState.COMMAND_STATE_FAILED,
      )

    def succeeded(): Unit =
      updateWithStatus(CompletionFromTransaction.OkStatus, CommandState.COMMAND_STATE_SUCCEEDED)

    def recordEnvelopeSizes(requestSize: Int, numRecipients: Int, numEnvelopes: Int): Unit =
      ref
        .updateAndGet(
          _.copy(
            requestStatistics = RequestStatistics(
              requestSize = requestSize,
              recipients = numRecipients,
              envelopes = numEnvelopes,
            )
          )
        )
        .discard

    def recordTransactionImpact(
        transaction: com.digitalasset.daml.lf.transaction.SubmittedTransaction
    ): Unit = {
      val creates = mutable.ListBuffer.empty[CommandUpdates.Contract]
      val archives = mutable.ListBuffer.empty[CommandUpdates.Contract]
      final case class Stats(
          exercised: Int = 0,
          fetched: Int = 0,
          lookedUpByKey: Int = 0,
      )
      def mk(
          templateId: TypeConName,
          coid: String,
          keyOpt: Option[GlobalKeyWithMaintainers],
      ): CommandUpdates.Contract = {
        CommandUpdates.Contract(
          templateId = Some(
            Identifier(
              templateId.packageId,
              templateId.qualifiedName.module.toString,
              templateId.qualifiedName.name.toString,
            )
          ),
          contractId = coid,
          contractKey =
            keyOpt.flatMap(x => LfEngineToApi.lfValueToApiValue(verbose = false, x.value).toOption),
        )
      }
      def leaf(leafOnlyAction: LeafOnlyAction, stats: Stats): Stats =
        leafOnlyAction match {
          case c: Node.Create =>
            creates += mk(c.templateId, c.coid.coid, c.keyOpt)
            stats
          case _: Node.Fetch => stats.copy(fetched = stats.fetched + 1)
          case _: Node.LookupByKey => stats.copy(lookedUpByKey = stats.lookedUpByKey + 1)
        }
      val stats = transaction.foldInExecutionOrder(Stats())(
        exerciseBegin = (acc, _, exerciseNode) => {
          if (exerciseNode.consuming) {
            archives += mk(
              exerciseNode.templateId,
              exerciseNode.targetCoid.coid,
              exerciseNode.keyOpt,
            )
          }
          (acc.copy(exercised = acc.exercised + 1), ChildrenRecursion.DoRecurse)
        },
        rollbackBegin = (acc, _, _) => {
          (acc, ChildrenRecursion.DoNotRecurse)
        },
        leaf = (acc, _, leafNode) => leaf(leafNode, acc),
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      ref
        .updateAndGet(
          _.copy(
            updates = CommandUpdates(
              created = creates.toList,
              archived = archives.toList,
              exercised = stats.exercised,
              fetched = stats.fetched,
              lookedUpByKey = stats.lookedUpByKey,
            )
          )
        )
        .discard
    }

  }

  // command key is (commandId, applicationId, actAs, submissionId)
  private type CommandKey = (String, String, Set[String], Option[String])

  private val pending = new mutable.LinkedHashMap[CommandKey, MyCommandResultHandle]()
  private val failed = new mutable.ArrayDeque[CommandStatus](config.maxFailed.value)
  private val succeeded = new mutable.ArrayDeque[CommandStatus](config.maxSucceeded.value)
  private val lock = new Object()

  private def findCommands(
      commandIdPrefix: String,
      limit: Int,
      collection: => Iterable[CommandStatus],
  ): Seq[CommandStatus] = {
    lock.synchronized {
      collection.filter(_.completion.commandId.startsWith(commandIdPrefix)).take(limit).toSeq
    }
  }

  override def findCommandStatus(
      commandIdPrefix: String,
      state: CommandState,
      limit: Int,
  ): Future[Seq[CommandStatus]] = Future {
    val pool = state match {
      case CommandState.COMMAND_STATE_UNSPECIFIED | CommandState.Unrecognized(_) =>
        findCommands(commandIdPrefix, limit, failed) ++
          findCommands(commandIdPrefix, limit, pending.values.map(_.ref.get())) ++
          findCommands(commandIdPrefix, limit, succeeded)
      case CommandState.COMMAND_STATE_FAILED => findCommands(commandIdPrefix, limit, failed)
      case CommandState.COMMAND_STATE_PENDING =>
        findCommands(commandIdPrefix, limit, pending.values.map(_.ref.get()))
      case CommandState.COMMAND_STATE_SUCCEEDED => findCommands(commandIdPrefix, limit, succeeded)
    }
    pool.take(limit)
  }

  override def registerCommand(
      commandId: String,
      submissionId: Option[String],
      applicationId: String,
      commands: Seq[Command],
      actAs: Set[String],
  )(implicit traceContext: TraceContext): CommandResultHandle = if (
    pending.size >= config.maxPending.value
  ) {
    CommandResultHandle.NoOp
  } else {
    val key = (commandId, applicationId, actAs, submissionId)
    logger.debug(s"Registering handle for $key")
    val status = CommandStatus(
      started = clock.now,
      completed = None,
      completion = CompletionFromTransaction.toApiCompletion(
        commandId = commandId,
        transactionId = "",
        applicationId = applicationId,
        traceContext = traceContext,
        optStatus = None,
        optSubmissionId = submissionId,
        optDeduplicationOffset = None,
        optDeduplicationDurationSeconds = None,
        optDeduplicationDurationNanos = None,
      ),
      state = CommandState.COMMAND_STATE_PENDING,
      commands = commands,
      requestStatistics = RequestStatistics(),
      updates = CommandUpdates(),
    )
    val handle = MyCommandResultHandle(key, status)
    val existing = lock.synchronized {
      pending.put(key, handle)
    }
    existing.foreach { prev =>
      // in theory, this can happen if an app sends the same command twice, so it's not
      // a warning ...
      logger.info(s"Duplicate command registration for ${prev.ref.get()}")
    }
    handle
  }

  override def findHandle(
      commandId: String,
      applicationId: String,
      actAs: Seq[String],
      submissionId: Option[String],
  ): CommandResultHandle = {
    lock.synchronized {
      pending.getOrElse(
        (commandId, applicationId, actAs.toSet, submissionId),
        CommandResultHandle.NoOp,
      )
    }
  }

  private def addToCollection(
      commandStatus: CommandStatus,
      collection: mutable.ArrayDeque[CommandStatus],
      maxSize: Int,
  ): Unit = {
    lock.synchronized {
      collection.prepend(commandStatus)
      if (collection.size > maxSize) {
        collection.removeLast().discard
      }
    }
  }

  override def processLedgerUpdate(update: Traced[TransactionLogUpdate]): Unit =
    update.value match {
      case TransactionLogUpdate.TransactionRejected(_offset, completionDetails) =>
        completionDetails.completionStreamResponse.completion.foreach { completionInfo =>
          val key = (
            completionInfo.commandId,
            completionInfo.applicationId,
            completionDetails.submitters,
            Option.when(completionInfo.submissionId.nonEmpty)(completionInfo.submissionId),
          )
          // remove from pending
          lock.synchronized(pending.remove(key)).foreach { cur =>
            if (config.maxFailed.value > 0) {
              cur.failedAsync(completionInfo.status)
              addToCollection(cur.ref.get(), failed, config.maxFailed.value)
            }
          }
        }
      case TransactionLogUpdate.TransactionAccepted(
            _transactionId,
            commandId,
            _workflowId,
            _effectiveAt,
            _offset,
            _events,
            Some(completionDetails),
            _domainId,
            _recordTime,
          ) =>
        completionDetails.completionStreamResponse.completion.foreach { completionInfo =>
          val key = (
            commandId,
            completionInfo.applicationId,
            completionDetails.submitters,
            Option.when(completionInfo.submissionId.nonEmpty)(completionInfo.submissionId),
          )
          // remove from pending
          lock.synchronized(pending.remove(key)).foreach { cur =>
            // mark as done
            cur.succeeded()
            addToCollection(cur.ref.get(), succeeded, config.maxSucceeded.value)
          }
        }
      case _ =>
    }

}
