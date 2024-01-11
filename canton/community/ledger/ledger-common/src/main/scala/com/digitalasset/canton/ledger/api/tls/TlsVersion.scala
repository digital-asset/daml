// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.tls

object TlsVersion {

  sealed abstract class TlsVersion(val version: String) {
    override def toString: String = version
  }

  case object V1 extends TlsVersion("TLSv1")

  case object V1_1 extends TlsVersion("TLSv1.1")

  case object V1_2 extends TlsVersion("TLSv1.2")

  case object V1_3 extends TlsVersion("TLSv1.3")

  val allVersions: Set[TlsVersion] = Set(
    V1,
    V1_1,
    V1_2,
    V1_3,
  )

}
