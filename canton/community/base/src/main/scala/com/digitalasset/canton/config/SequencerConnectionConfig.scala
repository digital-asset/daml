// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.google.protobuf.ByteString

/** Definition provided by the domain node to members with details on how to connect to the domain sequencer. * */
sealed trait SequencerConnectionConfig {
  def toConnection: Either[String, SequencerConnection]
}

object SequencerConnectionConfig {

  // TODO(i3804) consolidate with TlsClientCertificate
  sealed trait CertificateConfig extends Product with Serializable {
    def pem: X509CertificatePem
  }

  /** Throws an exception if the file does not exist or cannot be loaded. */
  final case class CertificateFile(pemFile: ExistingFile) extends CertificateConfig {
    override val pem: X509CertificatePem = X509CertificatePem.tryFromFile(pemFile.unwrap.toScala)
  }

  /** Throws an exception if the string containing the PEM certificate cannot be loaded. */
  final case class CertificateString(pemString: String) extends CertificateConfig {
    override val pem: X509CertificatePem = X509CertificatePem.tryFromString(pemString)
  }

  object CertificateConfig {
    def apply(bytes: ByteString): CertificateConfig =
      CertificateString(bytes.toStringUtf8)
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
