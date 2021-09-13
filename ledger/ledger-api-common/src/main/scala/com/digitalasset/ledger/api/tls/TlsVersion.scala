// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

object TlsVersion {

  sealed abstract class TlsVersion(val version: String) {
    override def toString: String = version
  }

  case object V1 extends TlsVersion("TLSv1")

  case object V1_1 extends TlsVersion("TLSv1.1")

  case object V1_2 extends TlsVersion("TLSv1.2")

  case object V1_3 extends TlsVersion("TLSv1.3")

}
