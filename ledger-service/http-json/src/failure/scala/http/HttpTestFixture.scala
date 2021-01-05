// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.Uri
import com.daml.bazeltools.BazelRunfiles
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.daml.ledger.client.LedgerClient
import com.daml.ports.LockedFreePort
import com.daml.testing.postgresql.PostgresAroundAll
import java.net.InetAddress
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}

trait HttpFailureTestFixture extends ToxicSandboxFixture with PostgresAroundAll { self: Suite =>

  private implicit val ec: ExecutionContext = system.dispatcher

  lazy val (dbProxy, dbProxyPort) = {
    val host = InetAddress.getLoopbackAddress
    val proxyPort = LockedFreePort.find()
    val proxy = proxyClient.createProxy(
      "database",
      s"${host.getHostName}:${proxyPort.port}",
      s"${host.getHostName}:${postgresDatabase.port}")
    proxyPort.unlock()
    (proxy, proxyPort.port)
  }

  // has to be lazy because postgresFixture is NOT initialized yet
  private lazy val jdbcConfig_ =
    JdbcConfig(
      driver = "org.postgresql.Driver",
      url =
        s"jdbc:postgresql://${postgresDatabase.hostName}:$dbProxyPort/${postgresDatabase.databaseName}?user=${postgresDatabase.userName}&password=${postgresDatabase.password}",
      user = "test",
      password = "",
      createSchema = true
    )

  override def packageFiles =
    List(
      BazelRunfiles.requiredResource("docs/quickstart-model.dar"),
      BazelRunfiles.requiredResource("ledger-service/http-json/Account.dar"))

  protected def allocateParty(client: LedgerClient, displayName: String): Future[domain.Party] =
    client.partyManagementClient
      .allocateParty(None, Some(displayName), None)
      .map(p => domain.Party(p.party))

  protected def withHttpService[A]
    : ((Uri, DomainJsonEncoder, DomainJsonDecoder, LedgerClient) => Future[A]) => Future[A] = {
    println(proxy.getUpstream())
    HttpServiceTestFixture.withHttpService(
      this.getClass.getSimpleName,
      proxiedPort,
      Some(jdbcConfig_),
      None,
      wsConfig = Some(Config.DefaultWsConfig))
  }
}
