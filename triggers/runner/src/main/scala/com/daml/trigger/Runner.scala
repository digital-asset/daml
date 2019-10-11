// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import scalaz.syntax.tag._
import scala.concurrent.{ExecutionContext, Future}

import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.{SExpr, Speedy, SValue}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement._

sealed trait TriggerMsg
final case class CompletionMsg(c: Completion) extends TriggerMsg
final case class TransactionMsg(t: Transaction) extends TriggerMsg

class Runner(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
    party: String,
    dar: Dar[(PackageId, Package)],
    submit: SubmitRequest => Unit) {

  private val converter = Converter.fromDar(dar)
  private val triggerIds = TriggerIds.fromDar(dar)
  private val darMap: Map[PackageId, Package] = dar.all.toMap
  private val compiler = Compiler(darMap)
  private val compiledPackages =
    PureCompiledPackages(darMap, compiler.compilePackages(darMap.keys)).right.get

  // Handles the result of initialState or update, i.e., (s, [Commands], Text)
  // by submitting the commands, printing the log message and returning
  // the new state
  def handleStepResult(v: SValue): SValue =
    v match {
      case SRecord(recordId, _, values)
          if recordId.qualifiedName ==
            QualifiedName(
              DottedName.assertFromString("DA.Types"),
              DottedName.assertFromString("Tuple3")) => {
        val newState = values.get(0)
        val commandVal = values.get(1)
        val logMessage = values.get(2) match {
          case SText(t) => t
          case _ =>
            throw new RuntimeException(s"Log message should be text but was ${values.get(2)}")
        }
        println(s"New state: $newState")
        println(s"Emitted log message: ${logMessage}")
        commandVal match {
          case SList(transactions) =>
            // Each transaction is a list of commands
            for (commands <- transactions) {
              converter.toCommands(commands) match {
                case Left(err) => throw new RuntimeException(err)
                case Right((commandId, commands)) => {
                  val commandsArg = Commands(
                    ledgerId = ledgerId.unwrap,
                    applicationId = applicationId.unwrap,
                    commandId = commandId,
                    party = party,
                    ledgerEffectiveTime = Some(fromInstant(Instant.EPOCH)),
                    maximumRecordTime = Some(fromInstant(Instant.EPOCH.plusSeconds(5))),
                    commands = commands
                  )
                  submit(SubmitRequest(commands = Some(commandsArg)))
                }
              }
            }
          case _ => {}
        }
        newState
      }
      case v => {
        throw new RuntimeException(s"Expected Tuple3 but got $v")
      }
    }

  def getTriggerSink(
      triggerId: Identifier,
      acs: Seq[CreatedEvent]): Sink[TriggerMsg, Future[SExpr]] = {
    val triggerExpr = EVal(triggerId)
    val (tyCon: TypeConName, stateTy) =
      dar.main._2.lookupIdentifier(triggerId.qualifiedName).toOption match {
        case Some(DValue(TApp(TTyCon(tcon), stateTy), _, _, _)) => (tcon, stateTy)
        case _ => {
          throw new RuntimeException(
            s"Identifier ${triggerId.qualifiedName} does not point to trigger")
        }
      }
    val triggerTy: TypeConApp = TypeConApp(tyCon, ImmArray(stateTy))
    val update = compiler.compile(ERecProj(triggerTy, Name.assertFromString("update"), triggerExpr))
    val getInitialState =
      compiler.compile(ERecProj(triggerTy, Name.assertFromString("initialState"), triggerExpr))

    val machine = Speedy.Machine.fromSExpr(null, false, compiledPackages)
    val createdExpr: SExpr = SEValue(converter.fromACS(acs))
    val initialState =
      SEApp(getInitialState, Array(SEValue(SParty(Party.assertFromString(party))), createdExpr))
    machine.ctrl = Speedy.CtrlExpr(initialState)
    while (!machine.isFinal) {
      machine.step() match {
        case SResultContinue => ()
        case SResultError(err) => {
          throw new RuntimeException(err)
        }
        case res => {
          throw new RuntimeException(s"Unexpected speedy result $res")
        }
      }
    }
    val evaluatedInitialState = handleStepResult(machine.toSValue)
    println(s"Initial state: $evaluatedInitialState")
    Sink.fold[SExpr, TriggerMsg](SEValue(evaluatedInitialState))((state, message) => {
      val messageVal = message match {
        case TransactionMsg(transaction) => {
          converter.fromTransaction(transaction)
        }
        case CompletionMsg(completion) => {
          converter.fromCompletion(completion)
        }
      }
      machine.ctrl = Speedy.CtrlExpr(SEApp(update, Array(SEValue(messageVal), state)))
      while (!machine.isFinal) {
        machine.step() match {
          case SResultContinue => ()
          case SResultError(err) => {
            throw new RuntimeException(err)
          }
          case res => {
            throw new RuntimeException(s"Unexpected speed result $res")
          }
        }
      }
      val newState = handleStepResult(machine.toSValue)
      SEValue(newState)
    })
  }
}

object Runner {
  def run(
      dar: Dar[(PackageId, Package)],
      triggerId: Identifier,
      client: LedgerClient,
      applicationId: ApplicationId,
      party: String,
      msgFlow: Flow[TriggerMsg, TriggerMsg, NotUsed] = Flow[TriggerMsg])(
      implicit materializer: Materializer,
      executionContext: ExecutionContext): Future[SExpr] = {
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    for {
      acsResponses <- client.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
      offset = Array(acsResponses: _*).lastOption
        .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
          LedgerOffset().withAbsolute(resp.offset))
      (msgSource, postFailure) = Runner.msgSource(client, offset, party)
      runner = new Runner(
        client.ledgerId,
        applicationId,
        party,
        dar,
        submitRequest => {
          val f = client.commandClient.submitSingleCommand(submitRequest)
          f.failed.foreach({
            case s: StatusRuntimeException =>
              postFailure(submitRequest.getCommands.commandId, s)
            case e => println(s"ERROR: Unexpected exception: $e")
          })
        }
      )
      finalState <- msgSource
        .via(msgFlow)
        .runWith(runner.getTriggerSink(triggerId, acsResponses.flatMap(x => x.activeContracts)))
    } yield finalState
  }
  def msgSource(client: LedgerClient, offset: LedgerOffset, party: String)(
      implicit materializer: Materializer)
    : (Source[TriggerMsg, NotUsed], (String, StatusRuntimeException) => Unit) = {
    // We use the queue to post failures that occur directly on command submission as opposed to
    // appearing asynchronously on the completion stream
    val (completionQueue, completionQueueSource) =
      Source.queue[Completion](10, OverflowStrategy.backpressure).preMaterialize()
    val transactionSource =
      client.transactionClient
        .getTransactions(
          offset,
          None,
          TransactionFilter(List((party, Filters.defaultInstance)).toMap),
          verbose = true)
        .map[TriggerMsg](TransactionMsg)
    val completionSource =
      client.commandClient
        .completionSource(List(party), offset)
        .mapConcat({
          case CheckpointElement(_) => List()
          case CompletionElement(c) => List(c)
        })
        .merge(completionQueueSource)
        .map[TriggerMsg](CompletionMsg)
    def postSubmitFailure(commandId: String, s: StatusRuntimeException) = {
      val _ = completionQueue.offer(
        Completion(
          commandId,
          Some(Status(s.getStatus().getCode().value(), s.getStatus().getDescription()))))
    }
    (transactionSource.merge(completionSource), postSubmitFailure)
  }
}
