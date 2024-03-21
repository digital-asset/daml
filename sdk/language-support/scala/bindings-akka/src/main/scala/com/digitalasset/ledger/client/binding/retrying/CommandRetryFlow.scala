// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.retrying

import java.time.temporal.TemporalAmount

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.ledger.client.services.commands.{CommandClient, CommandSubmission}
import com.daml.util.Ctx
import com.google.rpc.Code
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

object CommandRetryFlow {

  type In[C] = Ctx[C, CommandSubmission]
  type Out[C] = Ctx[C, Either[CompletionFailure, CompletionSuccess]]
  type SubmissionFlowType[C] = Flow[In[C], Out[C], NotUsed]

  private val RETRY_PORT = 0
  private val PROPAGATE_PORT = 1

  def apply[C](
      party: Party,
      commandClient: CommandClient,
      timeProvider: TimeProvider,
      maxRetryTime: TemporalAmount,
  )(implicit ec: ExecutionContext): Future[SubmissionFlowType[C]] =
    for {
      submissionFlow <- commandClient
        .trackCommands[RetryInfo[C, CommandSubmission]](List(party.unwrap))
      submissionFlowWithoutMat = submissionFlow.mapMaterializedValue(_ => NotUsed)
      graph = createGraph(submissionFlowWithoutMat, timeProvider, maxRetryTime)
    } yield wrapGraph(graph, timeProvider)

  def wrapGraph[C](
      graph: SubmissionFlowType[RetryInfo[C, CommandSubmission]],
      timeProvider: TimeProvider,
  ): SubmissionFlowType[C] =
    Flow[In[C]]
      .map(RetryInfo.wrap(timeProvider))
      .via(graph)
      .map(RetryInfo.unwrap)

  def createGraph[C](
      commandSubmissionFlow: SubmissionFlowType[RetryInfo[C, CommandSubmission]],
      timeProvider: TimeProvider,
      maxRetryTime: TemporalAmount,
  ): SubmissionFlowType[RetryInfo[C, CommandSubmission]] =
    Flow
      .fromGraph(GraphDSL.createGraph(commandSubmissionFlow) { implicit b => commandSubmission =>
        import GraphDSL.Implicits._

        val merge =
          b.add(Merge[In[RetryInfo[C, CommandSubmission]]](inputPorts = 2, eagerComplete = true))

        val retryDecider = b.add(
          Partition[Out[RetryInfo[C, CommandSubmission]]](
            outputPorts = 2,
            {
              case Ctx(
                    RetryInfo(value, nrOfRetries, firstSubmissionTime, _),
                    Left(notOk: CompletionResponse.NotOkResponse),
                    _,
                  ) if RETRYABLE_ERROR_CODES.contains(notOk.grpcStatus.code) =>
                if ((firstSubmissionTime plus maxRetryTime) isBefore timeProvider.getCurrentTime) {
                  RetryLogger.logStopRetrying(
                    value,
                    notOk.grpcStatus,
                    nrOfRetries,
                    firstSubmissionTime,
                  )
                  PROPAGATE_PORT
                } else {
                  RetryLogger.logNonFatal(value, notOk.grpcStatus, nrOfRetries)
                  RETRY_PORT
                }
              case Ctx(
                    RetryInfo(value, nrOfRetries, _, _),
                    Left(notOk: CompletionResponse.NotOkResponse),
                    _,
                  ) =>
                RetryLogger.logFatal(value, notOk.grpcStatus, nrOfRetries)
                PROPAGATE_PORT
              case Ctx(_, Left(CompletionResponse.TimeoutResponse(_)), _) =>
                PROPAGATE_PORT
              case Ctx(_, Left(statusNotFound: CompletionResponse.NoStatusInResponse), _) =>
                statusNotFoundError(statusNotFound.commandId)
              case Ctx(_, Right(_), _) =>
                PROPAGATE_PORT
            },
          )
        )

        val convertToRetry = b.add(Flow[Out[RetryInfo[C, CommandSubmission]]].map {
          case Ctx(retryInfo, _, telemetryContext) =>
            Ctx(retryInfo.newRetry, retryInfo.value, telemetryContext)
        })

        // format: off
        merge.out            ~> commandSubmission ~> retryDecider.in
        merge.in(RETRY_PORT) <~  convertToRetry   <~ retryDecider.out(RETRY_PORT)
        // format: on

        FlowShape(merge.in(PROPAGATE_PORT), retryDecider.out(PROPAGATE_PORT))
      })

  private[retrying] val RETRYABLE_ERROR_CODES =
    Set(Code.RESOURCE_EXHAUSTED_VALUE, Code.UNAVAILABLE_VALUE)

  private def statusNotFoundError(commandId: String): Int =
    throw new RuntimeException(s"Status for command $commandId is missing.")

}
