// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultBoundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Context, ManagedChannel, Status, StatusException, StatusRuntimeException}

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise, blocking}

trait AdminCommand[Req, Res, Result] {

  /** Create the request from configured options
    */
  def createRequest(): Either[String, Req]

  /** Handle the response the service has provided
    */
  def handleResponse(response: Res): Either[String, Result]

  /** Determines within which time frame the request should complete
    *
    * Some requests can run for a very long time. In this case, they should be "unbounded".
    * For other requests, you will want to set a custom timeout apart from the global default bounded timeout
    */
  def timeoutType: TimeoutType = DefaultBoundedTimeout

  /** Command's full name used to identify command in logging and span reporting
    */
  def fullName: String =
    // not using getClass.getSimpleName because it ignores the hierarchy of nested classes, and it also throws unexpected exceptions
    getClass.getName.split('.').last.replace("$", ".")
}

/** cantonctl GRPC Command
  */
trait GrpcAdminCommand[Req, Res, Result] extends AdminCommand[Req, Res, Result] {

  type Svc <: AbstractStub[Svc]

  /** Create the GRPC service to call
    */
  def createService(channel: ManagedChannel): Svc

  /** Submit the created request to our service
    */
  def submitRequest(service: Svc, request: Req): Future[Res]

}

object GrpcAdminCommand {
  sealed trait TimeoutType extends Product with Serializable

  /** Custom timeout triggered by the client */
  final case class CustomClientTimeout(timeout: NonNegativeDuration) extends TimeoutType

  /** The Server will ensure the operation is timed out so the client timeout is set to an infinite value */
  case object ServerEnforcedTimeout extends TimeoutType
  case object DefaultBoundedTimeout extends TimeoutType
  case object DefaultUnboundedTimeout extends TimeoutType

  object GrpcErrorStatus {
    def unapply(ex: Throwable): Option[Status] = ex match {
      case e: StatusException => Some(e.getStatus)
      case re: StatusRuntimeException => Some(re.getStatus)
      case _ => None
    }
  }

  private[digitalasset] def streamedResponse[Request, Response, Result](
      service: (Request, StreamObserver[Response]) => Unit,
      extract: Response => Seq[Result],
      request: Request,
      expected: Int,
      timeout: FiniteDuration,
      scheduler: ScheduledExecutorService,
  ): Future[Seq[Result]] = {
    val promise = Promise[Seq[Result]]()
    val buffer = ListBuffer[Result]()
    val context = Context.ROOT.withCancellation()

    def success(): Unit = blocking(buffer.synchronized {
      context.close()
      promise.trySuccess(buffer.toList).discard[Boolean]
    })

    context.run(() =>
      service(
        request,
        new StreamObserver[Response]() {
          override def onNext(value: Response): Unit = {
            val extracted = extract(value)
            blocking(buffer.synchronized {
              if (buffer.lengthCompare(expected) < 0) {
                buffer ++= extracted
                if (buffer.lengthCompare(expected) >= 0) {
                  success()
                }
              }
            })
          }

          override def onError(t: Throwable): Unit = {
            t match {
              case GrpcErrorStatus(status) if status.getCode == Status.CANCELLED.getCode =>
                success()
              case _ =>
                val _ = promise.tryFailure(t)
            }
          }

          override def onCompleted(): Unit = {
            success()
          }
        },
      )
    )
    scheduler.schedule(
      new Runnable() {
        override def run(): Unit = {
          val _ = context.cancel(Status.CANCELLED.asException())
        }
      },
      timeout.toMillis,
      TimeUnit.MILLISECONDS,
    )
    promise.future
  }
}
