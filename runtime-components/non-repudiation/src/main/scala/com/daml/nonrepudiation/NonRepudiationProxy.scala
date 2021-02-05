// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.time.Clock

import com.codahale.metrics.MetricRegistry
import com.daml.grpc.ReverseProxy
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext}
import io.grpc.{Channel, Server, ServerBuilder}

object NonRepudiationProxy {

  def owner[Context](
      participant: Channel,
      serverBuilder: ServerBuilder[_],
      certificateRepository: CertificateRepository.Read,
      signedPayloadRepository: SignedPayloadRepository.Write,
      metricsReporterProvider: MetricRegistry => MetricsReporterOwner[Context],
      timestampProvider: Clock,
      serviceName: String,
      serviceNames: String*
  )(implicit context: HasExecutionContext[Context]): AbstractResourceOwner[Context, Server] = {

    val metricsRegistry = new MetricRegistry
    val metrics = new Metrics(metricsRegistry)

    val signatureVerification =
      new SignatureVerificationInterceptor(
        certificateRepository,
        signedPayloadRepository,
        timestampProvider,
        metrics,
      )

    val serverOwner =
      ReverseProxy.owner(
        backend = participant,
        serverBuilder = serverBuilder,
        interceptors = Iterator(serviceName +: serviceNames: _*)
          .map(service => service -> Seq(signatureVerification))
          .toMap,
      )

    for {
      _ <- metricsReporterProvider(metricsRegistry)
      server <- serverOwner
    } yield server

  }

}
