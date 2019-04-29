// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.daml.lf.iface
import com.digitalasset.extractor.ledger.types.Identifier
import doobie.util.fragment.Fragment

import scala.util.control.NoStackTrace
import scalaz._

object Types {
  final case class TypeDecls(
      templates: Map[Identifier, iface.Record.FWT] = Map.empty,
      records: Map[Identifier, iface.Record.FWT] = Map.empty,
      variants: Map[Identifier, iface.Variant.FWT] = Map.empty
  )

  /**
    * Like [[iface.Type]], without `TypeVar`s
    */
  sealed abstract class FullyAppliedType extends Product with Serializable

  object FullyAppliedType {
    final case class TypePrim(typ: iface.PrimType, typArgs: List[FullyAppliedType])
        extends FullyAppliedType
    final case class TypeCon(typ: iface.TypeConName, typArgs: List[FullyAppliedType])
        extends FullyAppliedType

    object ops {
      final implicit class GenTypeOps(val genType: iface.Type) extends AnyVal {
        def fat: FullyAppliedType = genType match {
          case iface.TypePrim(typ, typArgs) => TypePrim(typ, typArgs.toList.map(_.fat))
          case iface.TypeCon(typ, typArgs) => TypeCon(typ, typArgs.toList.map(_.fat))
          case iface.TypeVar(_) =>
            throw new IllegalArgumentException(
              s"A `TypeVar` (${genType}) cannot be converted to a `FullyAppliedType`"
            )
        }
      }
    }
  }

  final case class TargetOverLedgerEndError(target: String, end: String)
      extends Exception(
        s"The current latest transaction number (${end}) is lower than the requested latest transaction (${target}) to extract. " +
          s"Use `--to head` to extract until the current latest one, or a number that is not greater than ${end}."
      )
      with NoStackTrace

  final case class DataIntegrityError(message: String) extends Exception(message) with NoStackTrace

  /** Statement fragments form a monoid. */
  implicit val FragmentMonoid: Monoid[Fragment] =
    new Monoid[Fragment] {
      val zero: Fragment = Fragment.empty
      def append(a: Fragment, b: => Fragment): Fragment = a ++ b
    }
}
