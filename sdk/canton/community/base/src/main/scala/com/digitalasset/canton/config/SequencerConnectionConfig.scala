// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnection}

/** Definition provided by the synchronizer node to members with details on how to connect to the synchronizer sequencer. * */
sealed trait SequencerConnectionConfig {
  def toConnection: Either[String, SequencerConnection]
}

object SequencerConnectionConfig {

  /** Throws an exception if the file does not exist or cannot be loaded. */
  final case class CertificateFile(pemFile: ExistingFile) {
    val pem: X509CertificatePem = X509CertificatePem.tryFromFile(pemFile.unwrap.toScala)
  }

  /** Grpc connection using a real grpc channel.
    */
  final case class Grpc(
      address: String,
      port: Port,
      transportSecurity: Boolean = false,
      customTrustCertificates: Option[CertificateFile] = None,
  ) extends SequencerConnectionConfig {

    def toConnection: Either[String, GrpcSequencerConnection] =
      for {
        pem <- customTrustCertificates.traverse(file =>
          X509CertificatePem.fromFile(file.pemFile.unwrap.toScala)
        )
      } yield GrpcSequencerConnection(
        NonEmpty(Seq, Endpoint(address, port)),
        transportSecurity,
        pem.map(_.unwrap),
        SequencerAlias.Default,
      )
  }
}
