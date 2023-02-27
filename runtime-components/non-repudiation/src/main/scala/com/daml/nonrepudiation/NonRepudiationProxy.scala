// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.time.Clock

import com.daml.grpc.ReverseProxy
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext}
import io.grpc.{Channel, Server, ServerBuilder}

object NonRepudiationProxy {

  def owner[Context](
      participant: Channel,
      serverBuilder: ServerBuilder[_],
      certificateRepository: CertificateRepository.Read,
      signedPayloadRepository: SignedPayloadRepository.Write,
      timestampProvider: Clock,
      metrics: Metrics,
      serviceName: String,
      serviceNames: String*
  )(implicit context: HasExecutionContext[Context]): AbstractResourceOwner[Context, Server] = {

    val signatureVerification =
      new SignatureVerificationInterceptor(
        certificateRepository,
        signedPayloadRepository,
        timestampProvider,
        metrics,
      )

    ReverseProxy.owner(
      backend = participant,
      serverBuilder = serverBuilder,
      interceptors = Iterator(serviceName +: serviceNames: _*)
        .map(service => service -> Seq(signatureVerification))
        .toMap,
    )

  }

}
