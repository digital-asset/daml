// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

case class GlobalKey(hash: BigInt)

sealed abstract class ContractKeyUniquenessMode extends Product with Serializable

object ContractKeyUniquenessMode {
  case object Off extends ContractKeyUniquenessMode
  case object Strict extends ContractKeyUniquenessMode
}
