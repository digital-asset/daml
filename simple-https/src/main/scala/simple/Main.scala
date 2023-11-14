// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package simple

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.server.{Directives, Route}
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.tls.TlsConfiguration
import io.netty.handler.ssl.ClientAuth
import java.nio.file.Paths

object Main extends Directives {

  def main(args: Array[String]): Unit = {
    val _ = System.setProperty("javax.net.debug", "all")

    val tlsConfig = TlsConfiguration(
      enabled = true,
      certChainFile = Some(testCerts("server.crt")),
      privateKeyFile = Some(testCerts("server.pem")),
      trustCollectionFile = Some(testCerts("ca.crt")),
      clientAuth = ClientAuth.NONE,
      enableCertRevocationChecking = false,
      minimumServerProtocolVersion = None,
    )
    val https: HttpsConnectionContext = httpsConnectionContext(tlsConfig)

    val routes: Route = get { complete("Hello world!") }

    implicit val system: ActorSystem = ActorSystem()
    val _ = Http().newServerAt("127.0.0.1", 1235).enableHttps(https).bind(routes)
  }

  // copied from ledger-service/http-json/src/main/scala/com/digitalasset/http/HttpService.scala
  private def httpsConnectionContext(config: TlsConfiguration): HttpsConnectionContext = {
    import io.netty.buffer.ByteBufAllocator
    val sslContext =
      config.server
        .getOrElse(
          throw new IllegalArgumentException(s"$config could not be built as a server ssl context")
        )
    ConnectionContext.httpsServer(() => {
      println("now creating the engine")
      val engine: javax.net.ssl.SSLEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT)
      engine.setUseClientMode(false)
      engine.setWantClientAuth(false)
      println(s"created the engine: $engine")
      engine
    })
  }

  private def testCerts(basename: String) =
    BazelRunfiles.rlocation(Paths.get(s"test-common/test-certificates/${basename}")).toFile
}
