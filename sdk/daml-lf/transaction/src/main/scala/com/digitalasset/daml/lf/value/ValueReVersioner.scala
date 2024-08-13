// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value

import com.digitalasset.daml.lf.transaction.{TransactionVersion, Versioned}
import com.digitalasset.daml.lf.value.Value.VersionedValue

object ValueReVersioner {

  // Every time a new LF version is stamped that does not modify the LF value proto files it should
  // be added into this set. If the LF value proto is not identical then the conversion needs to be
  // manually handled in `reVersionValue`
  private val identicalLfValueVersions = Set(TransactionVersion.V31, TransactionVersion.VDev)

  private def versionsCompatible(
      source: TransactionVersion,
      target: TransactionVersion,
  ): Boolean = {
    identicalLfValueVersions.contains(source) && identicalLfValueVersions.contains(target)
  }

  def reVersionValue(
      value: VersionedValue,
      target: TransactionVersion,
  ): Either[String, VersionedValue] = {
    if (versionsCompatible(value.version, target)) Right(Versioned(target, value.unversioned))
    else Left(s"Version ${value.version} cannot be re-versioned to $target")
  }

}
