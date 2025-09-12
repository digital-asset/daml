// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.implicits.toBifunctorOps
import com.digitalasset.canton.protocol.{CantonContractIdV1Version, CantonContractIdVersion, LfHash}
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.digitalasset.daml.lf.value.Value.ThinContractInstance

import scala.util.Try

/** This class is used in the places where the hash has not been provided by the engine
  */
// TODO(#27344) - Future versions of contract hash will require a minimal type calculated by the engine
object LegacyContractHash {

  def tryThinContractHash(
      contractInstance: ThinContractInstance,
      upgradeFriendly: Boolean,
  ): LfHash =
    LfHash.assertHashContractInstance(
      contractInstance.template,
      contractInstance.arg,
      contractInstance.packageName,
      upgradeFriendly = upgradeFriendly,
    )

  def tryFatContractHash(contractInstance: FatContractInstance, upgradeFriendly: Boolean): LfHash =
    LfHash.assertHashContractInstance(
      contractInstance.templateId,
      contractInstance.createArg,
      contractInstance.packageName,
      upgradeFriendly = upgradeFriendly,
    )

  def fatContractHash(contractInstance: FatContractInstance): Either[String, LfHash] =
    for {
      idVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(contractInstance.contractId)
      idVersionV1 <- idVersion match {
        case v: CantonContractIdV1Version => Right(v)
        case other => Left(s"Unsupported contract authentication id version: $other")
      }
      result <- Try(
        tryFatContractHash(contractInstance, idVersionV1.useUpgradeFriendlyHashing)
      ).toEither.leftMap { e =>
        s"Failed to compute contract hash for contract id ${contractInstance.contractId}: $e"
      }
    } yield result

}
