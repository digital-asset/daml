// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.admin

import com.google.protobuf
import com.daml.ledger.api.v1.package_service

sealed abstract class HashFunction extends Product with Serializable
case object SHA256 extends HashFunction
final case class Unrecognized(value: Int) extends HashFunction

object HashFunction {
  def fromLedgerApi(a: package_service.HashFunction): HashFunction = a match {
    case package_service.HashFunction.SHA256 => SHA256
    case package_service.HashFunction.Unrecognized(x) => Unrecognized(x)
  }
}

final case class GetPackageResponse(
    hashFunction: HashFunction,
    hash: String,
    archivePayload: protobuf.ByteString,
)

object GetPackageResponse {
  def fromLedgerApi(a: package_service.GetPackageResponse): GetPackageResponse = {
    GetPackageResponse(
      hashFunction = HashFunction.fromLedgerApi(a.hashFunction),
      archivePayload = a.archivePayload,
      hash = a.hash,
    )
  }
}
