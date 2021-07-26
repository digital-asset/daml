// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.cliopts.Logging.LogEncoder
import com.daml.ledger.api.tls.TlsConfiguration
import shapeless.{FieldPoly, LabelledGeneric}

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration
import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.metrics.MetricsReporter
import shapeless.labelled.FieldType
import scalaz.syntax.show._
import scalaz.std.anyVal._
import scalaz.std.option._

// This compile time checks that all fields of the Config case class
// have a fitting implementation for displaying it on the command line.
object ConfigOps {

  // The signatures would make this code very unreadable
  @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
  private object matcher extends FieldPoly {
    type Field[Name, Typ] = Typ with FieldType[Symbol with shapeless.tag.Tagged[Name], Typ]
    val W = shapeless.Witness

    implicit def ledgerHost = at[Field[W.`"ledgerHost"`.T, String]](value => s"ledgerHost=$value")

    implicit def ledgerPort = at[Field[W.`"ledgerPort"`.T, Int]](value => s"ledgerPort=$value")

    implicit def address = at[Field[W.`"address"`.T, String]](value => s"address=$value")

    implicit def httpPort = at[Field[W.`"httpPort"`.T, Int]](value => s"httpPort=$value")

    implicit def portFile = at[Field[W.`"portFile"`.T, Option[Path]]](value => s"portFile=$value")

    implicit def packageReloadInterval =
      at[Field[W.`"packageReloadInterval"`.T, FiniteDuration]](value =>
        s"packageReloadInterval=$value"
      )

    implicit def packageMaxInboundMessageSize =
      at[Field[W.`"packageMaxInboundMessageSize"`.T, Option[Int]]](value =>
        s"packageMaxInboundMessageSize=$value"
      )

    implicit def maxInboundMessageSize =
      at[Field[W.`"maxInboundMessageSize"`.T, Int]](value => s"maxInboundMessageSize=$value")

    implicit def healthTimeoutSeconds =
      at[Field[W.`"healthTimeoutSeconds"`.T, Int]](value => s"healthTimeoutSeconds=$value")

    implicit def tlsConfig =
      at[Field[W.`"tlsConfig"`.T, TlsConfiguration]](value => s"tlsConfig=$value")

    implicit def jdbcConfig =
      at[Field[W.`"jdbcConfig"`.T, Option[JdbcConfig]]](value =>
        s"jdbcConfig=${(value: Option[JdbcConfig]).shows}"
      )

    implicit def staticContentConfig =
      at[Field[W.`"staticContentConfig"`.T, Option[StaticContentConfig]]](value =>
        s"staticContentConfig=${(value: Option[StaticContentConfig]).shows}"
      )

    implicit def allowNonHttps =
      at[Field[W.`"allowNonHttps"`.T, Boolean]](value => s"allowNonHttps=${(value: Boolean).shows}")

    implicit def accessTokenFile =
      at[Field[W.`"accessTokenFile"`.T, Option[Path]]](value => s"accessTokenFile=$value")

    implicit def wsConfig =
      at[Field[W.`"wsConfig"`.T, Option[WebsocketConfig]]](value =>
        s"wsConfig=${(value: Option[WebsocketConfig]).shows}"
      )

    implicit def nonRepudiation =
      at[Field[W.`"nonRepudiation"`.T, nonrepudiation.Configuration.Cli]](value =>
        List(
          s"nonRepudiationCertificateFile=${value.certificateFile: Option[Path]}",
          s"nonRepudiationPrivateKeyFile=${value.privateKeyFile: Option[Path]}",
          s"nonRepudiationPrivateKeyAlgorithm=${value.privateKeyAlgorithm: Option[String]}",
        ).mkString(", ")
      )
    implicit def logLevel =
      at[Field[W.`"logLevel"`.T, Option[LogLevel]]](value => s"logLevel=$value")

    implicit def logEncoder =
      at[Field[W.`"logEncoder"`.T, LogEncoder]](value => s"logEncoder=$value")

    implicit def metricsReporter =
      at[Field[W.`"metricsReporter"`.T, Option[MetricsReporter]]](value =>
        s"metricsReporter=$value"
      )

    implicit def metricsReportingInterval =
      at[Field[W.`"metricsReportingInterval"`.T, FiniteDuration]](value =>
        s"metricsReportingInterval=$value"
      )
  }

  def cliString(config: Config) = {
    val genericConfig = LabelledGeneric[Config]
    val it = genericConfig.to(config)
    it.map(matcher).mkString("Config(", ", ", ")")
  }
}
