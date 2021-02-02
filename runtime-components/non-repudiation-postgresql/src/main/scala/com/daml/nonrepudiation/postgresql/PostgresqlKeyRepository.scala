// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.postgresql

import java.security.PublicKey

import cats.effect.IO
import com.daml.nonrepudiation.{FingerprintBytes, KeyRepository}
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor

final class PostgresqlKeyRepository(transactor: Transactor[IO])(implicit
    logHandler: LogHandler
) extends KeyRepository {

  private val table = Fragment.const(s"${Tables.Prefix}_keys")

  private def getKey(fingerprint: FingerprintBytes): Fragment =
    fr"""select "key" from """ ++ table ++ fr"where fingerprint = $fingerprint"

  override def get(fingerprint: FingerprintBytes): Option[PublicKey] =
    getKey(fingerprint).query[PublicKey].option.transact(transactor).unsafeRunSync()

  private def putKey(fingerprint: FingerprintBytes, key: PublicKey): Fragment =
    fr"insert into " ++ table ++ fr"""(fingerprint, "key") values ($fingerprint, $key)"""

  override def put(key: PublicKey): FingerprintBytes = {
    val fingerprint = FingerprintBytes.compute(key)
    putKey(fingerprint, key).update.run.transact(transactor).unsafeRunSync()
    fingerprint
  }

}
