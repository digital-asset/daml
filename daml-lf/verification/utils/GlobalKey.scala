package lf.verified
package utils

case class GlobalKey(hash: BigInt)

sealed abstract class ContractKeyUniquenessMode extends Product with Serializable

object ContractKeyUniquenessMode {
  case object Off extends ContractKeyUniquenessMode
  case object Strict extends ContractKeyUniquenessMode
}