// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import io.grpc.health.v1.health.HealthCheckResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class HealthService(
    getLedgerHealth: HealthService.GetHealthCheckResponse,
) {

  import HealthService.*
  def ready()(implicit ec: ExecutionContext): Future[ReadyResponse] =
    for {
      ledger <- getLedgerHealth().transform {
        case Failure(err) => Success((false, Some(err.toString)))
        case Success(resp) =>
          Success(
            (resp.status == HealthCheckResponse.ServingStatus.SERVING, Some(resp.status.toString))
          )
      }
    } yield ReadyResponse(
      Seq(Check("ledger", ledger._1, ledger._2))
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
