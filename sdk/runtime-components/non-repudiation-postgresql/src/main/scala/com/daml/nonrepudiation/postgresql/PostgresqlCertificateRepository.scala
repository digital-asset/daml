// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.postgresql

import java.security.cert.X509Certificate

import cats.effect.IO
import com.daml.nonrepudiation.{CertificateRepository, FingerprintBytes}
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor

final class PostgresqlCertificateRepository(transactor: Transactor[IO])(implicit
    logHandler: LogHandler
) extends CertificateRepository {

  private val table = Fragment.const(s"${Tables.Prefix}_certificates")

  private def getCertificate(fingerprint: FingerprintBytes): Fragment =
    fr"select certificate from" ++ table ++ fr"where fingerprint = $fingerprint"

  override def get(fingerprint: FingerprintBytes): Option[X509Certificate] =
    getCertificate(fingerprint).query[X509Certificate].option.transact(transactor).unsafeRunSync()

  private def putKey(fingerprint: FingerprintBytes, certificate: X509Certificate): Fragment =
    fr"insert into" ++ table ++ fr"""(fingerprint, certificate) values ($fingerprint, $certificate) on conflict do nothing"""

  override def put(certificate: X509Certificate): FingerprintBytes = {
    val fingerprint = FingerprintBytes.compute(certificate)
    putKey(fingerprint, certificate).update.run.transact(transactor).unsafeRunSync()
    fingerprint
  }

}
