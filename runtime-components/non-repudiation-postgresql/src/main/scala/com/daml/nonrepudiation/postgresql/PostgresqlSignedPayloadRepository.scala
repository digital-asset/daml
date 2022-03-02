// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.postgresql

import cats.effect.IO
import com.daml.nonrepudiation.{CommandIdString, SignedPayload, SignedPayloadRepository}
import doobie.implicits._
import doobie.implicits.legacy.instant._
import doobie.util.fragment.Fragment
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor

final class PostgresqlSignedPayloadRepository(transactor: Transactor[IO])(implicit
    logHandler: LogHandler
) extends SignedPayloadRepository[CommandIdString] {

  private val table = Fragment.const(s"${Tables.Prefix}_signed_payloads")

  private def getSignedPayload(key: CommandIdString): Fragment =
    fr"select algorithm, fingerprint, payload, signature, timestamp from" ++ table ++ fr"where command_id = $key"

  override def get(key: CommandIdString): Iterable[SignedPayload] =
    getSignedPayload(key).query[SignedPayload].to[Iterable].transact(transactor).unsafeRunSync()

  private def putSignedPayload(key: CommandIdString, signedPayload: SignedPayload): Fragment = {
    import signedPayload._
    fr"insert into" ++ table ++ fr"""(command_id, algorithm, fingerprint, payload, signature, timestamp) values($key, $algorithm, $fingerprint, $payload, $signature, $timestamp)"""
  }

  override def put(signedPayload: SignedPayload): Unit = {
    val key = keyEncoder.encode(signedPayload.payload)
    val _ = putSignedPayload(key, signedPayload).update.run.transact(transactor).unsafeRunSync()
  }
}
