// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.txncounts

import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.jwt
import com.daml.ledger.{client => dlc}
import dlc.{LedgerClient => DamlLedgerClient}
import dlc.configuration.{
  LedgerClientConfiguration,
  LedgerIdRequirement,
  CommandClientConfiguration,
}
import com.daml.ledger.api.auth.AuthServiceJWTCodec.{readFromString => parseJWT}
import com.daml.ledger.api.{v1 => lav1}
import lav1.ledger_offset.LedgerOffset
import lav1.value.Identifier
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.transaction_filter.Filters
import scalaz.\/-
import scalaz.syntax.std.option._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext => EC}

object Main extends App {
  private val (host, port, rawJwt, startOffset) = args match {
    case Array(host, port, jwt, rest @ _*) =>
      (
        host,
        port.toInt,
        jwt,
        rest match {
          case Seq(startOffset) => Some(startOffset)
          case Seq() => None
        },
      )
    case _ => sys.error("Usage: LEDGER_HOST LEDGER_PORT JWT [START_OFFSET]")
  }
  private val asys = ActorSystem()
  private implicit val amat: Materializer = Materializer(asys)
  private implicit val aesf: AkkaExecutionSequencerPool = new AkkaExecutionSequencerPool(
    "clientPool"
  )(asys)

  private def shutdown(): Unit = {
    Await.result(execution, 60.minutes)
    Await.result(asys.terminate(), 10.seconds)
    ()
  }

  private implicit val ec: EC = asys.dispatcher

  private lazy val jwtPayload = {
    val \/-(djwt) = jwt.JwtDecoder.decode(jwt.domain.Jwt(rawJwt))
    parseJWT(djwt.payload).get
  }

  private lazy val parties =
    jwtPayload.actAs.toSet ++ jwtPayload.readAs

  private val lcc = LedgerClientConfiguration(
    applicationId = jwtPayload.applicationId getOrElse sys.error("application ID required"),
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    token = Some(rawJwt),
  )

  val execution = for {
    open <- it()
    (client, end) = open
    _ <- counts(client, end)
      .map((reportCounts _).tupled)
      .wireTap(jsv => println(jsv.compactPrint))
      .run()
  } yield ()

  private def it() = for {
    client <- DamlLedgerClient.singleHost(host, port, lcc)
    ler <- client.transactionClient.getLedgerEnd(Some(rawJwt))
  } yield (client, ler.offset)

  private def counts(
      client: DamlLedgerClient,
      end: Option[LedgerOffset],
  ): Source[(String, Counts), NotUsed] = {
    import lav1.transaction_filter.TransactionFilter
    import lav1.event.Event.Event
    client.transactionClient
      .getTransactions(
        start = LedgerOffset(
          startOffset.cata[LedgerOffset.Value](
            LedgerOffset.Value.Absolute,
            LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
          )
        ),
        end = end,
        transactionFilter =
          TransactionFilter(filtersByParty = parties.view.map((_, Filters.defaultInstance)).toMap),
        verbose = false,
        token = Some(rawJwt),
      )
      .map { txn =>
        val (created, archived) = txn.events.partitionMap {
          _.event match {
            case Event.Created(ce) => Left(ce.getTemplateId)
            case Event.Archived(ae) => Right(ae.getTemplateId)
            case Event.Empty => sys.error("nonexistent transaction event")
          }
        }
        def ct[A](m: Seq[A]): Map[A, Int] = m.groupBy(identity).transform((_, xs) => xs.size)
        (txn.transactionId, Counts(creates = ct(created), archives = ct(archived)))
      }
  }

  private def reportCounts(txnId: String, counts: Counts): JsValue = {
    import DefaultJsonProtocol._
    implicit val identifierFormat: JsonFormat[Identifier] = new JsonFormat[Identifier] {
      override def read(json: JsValue) = sys.error("never used")
      override def write(obj: Identifier) = JsString(
        s"${obj.packageId}:${obj.moduleName}:${obj.entityName}"
      )
    }
    Map(
      "transactionId" -> txnId.toJson,
      "creates" -> counts.creates.toJson,
      "archives" -> counts.archives.toJson,
    ).toJson
  }

  shutdown()
}

final case class Counts(creates: Map[Identifier, Int], archives: Map[Identifier, Int])
