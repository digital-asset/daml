// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import io.grpc.health.v1.health.HealthCheckResponse
import scalaz.Scalaz._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

final class HealthService(
    getLedgerHealth: HealthService.GetHealthCheckResponse,
    contractDao: Option[dbbackend.ContractDao],
    timeoutSeconds: Int,
) {

  import HealthService._
  def ready()(implicit ec: ExecutionContext): Future[ReadyResponse] =
    for {
      ledger <- getLedgerHealth().transform {
        case Failure(err) => Success((false, Some(err.toString)))
        case Success(resp) =>
          Success(
            (resp.status == HealthCheckResponse.ServingStatus.SERVING, Some(resp.status.toString))
          )
      }
      optDb <- contractDao.traverse(opt =>
        opt.isValid(timeoutSeconds).unsafeToFuture().recover { case NonFatal(_) =>
          false
        }
      )
    } yield ReadyResponse(
      Seq(Check("ledger", ledger._1, ledger._2)) ++ optDb.toList.map(Check("database", _, None))
    )
}

object HealthService {
  case class Check(name: String, result: Boolean, details: Option[String])
  case class ReadyResponse(checks: Seq[Check]) {
    val ok = checks.forall(_.result)
    private def check(c: Check) = {
      val (checkBox, result) = if (c.result) { ("+", "ok") }
      else { ("-", "failed") }
      val output = s"[$checkBox] ${c.name} $result"
      c.details.fold(output)(d => s"$output ($d)")
    }

    // Format modeled after k8s’ own healthchecks
    private def render(): String =
      (checks.map(check(_)) ++ Seq(s"readyz check ${if (ok) "passed" else "failed"}", ""))
        .mkString("\n")

    def toHttpResponse: HttpResponse =
      HttpResponse(
        status = if (ok) StatusCodes.OK else StatusCodes.ServiceUnavailable,
        entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, render()),
      )
  }
  type GetHealthCheckResponse = () => Future[HealthCheckResponse]
}
