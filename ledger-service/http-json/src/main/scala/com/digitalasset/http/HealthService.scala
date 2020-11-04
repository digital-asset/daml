// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import scalaz.Scalaz._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

final class HealthService(
    getLedgerEnd: HealthService.GetLedgerEnd,
    contractDao: Option[dbbackend.ContractDao],
    timeoutSeconds: Int) {
  import HealthService._
  def ready()(implicit ec: ExecutionContext): Future[ReadyResponse] =
    for {
      ledger <- getLedgerEnd().transform(r => Try(r.isSuccess))
      optDb <- contractDao.traverse(opt =>
        opt.isValid(timeoutSeconds).unsafeToFuture().recover {
          case NonFatal(_) => false
      })
    } yield ReadyResponse(Seq(Check("ledger", ledger)) ++ optDb.toList.map(Check("database", _)))
}

object HealthService {
  case class Check(name: String, result: Boolean)
  case class ReadyResponse(checks: Seq[Check]) {
    val ok = checks.forall(_.result)
    private def check(c: Check) = {
      val (checkBox, result) = if (c.result) { ("+", "ok") } else { ("-", "failed") }
      s"[$checkBox] ${c.name} $result"
    }

    // Format modeled after k8s’ own healthchecks
    private def render(): String =
      (checks.map(check(_)) ++ Seq(s"readyz check ${if (ok) "passed" else "failed"}", ""))
        .mkString("\n")

    def toHttpResponse: HttpResponse =
      HttpResponse(
        status = if (ok) StatusCodes.OK else StatusCodes.ServiceUnavailable,
        entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, render())
      )
  }
  // We only check health so we don’t care about the offset
  type GetLedgerEnd = () => Future[Unit]
}
