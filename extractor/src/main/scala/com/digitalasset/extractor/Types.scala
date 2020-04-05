// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import com.daml.lf.iface
import com.daml.lf.iface.{Enum, TypeConName}
import com.daml.ledger.service.LedgerReader.PackageStore
import doobie.util.fragment.Fragment

import scala.util.control.NoStackTrace
import scalaz._

object Types {

  /**
    * Like `iface.Type`, without `TypeVar`s
    */
  sealed abstract class FullyAppliedType extends Product with Serializable

  object FullyAppliedType {
    final case class TypeNumeric(scale: Int) extends FullyAppliedType
    final case class TypePrim(typ: iface.PrimType, typArgs: List[FullyAppliedType])
        extends FullyAppliedType
    final case class TypeCon(
        typ: iface.TypeConName,
        typArgs: List[FullyAppliedType],
        isEnum: Boolean)
        extends FullyAppliedType

    object ops {
      final implicit class GenTypeOps(val genType: iface.Type) extends AnyVal {
        private def isEnum(typeCon: TypeConName, packageStore: PackageStore) = {
          val dataType = packageStore(typeCon.identifier.packageId)
            .typeDecls(typeCon.identifier.qualifiedName)
            .`type`
            .dataType
          dataType match {
            case Enum(_) => true
            case _ => false
          }
        }

        def fat(packageStore: PackageStore): FullyAppliedType = genType match {
          case iface.TypePrim(typ, typArgs) =>
            TypePrim(typ, typArgs.toList.map(_.fat(packageStore)))
          case iface.TypeCon(tyConName, typArgs) =>
            TypeCon(
              tyConName,
              typArgs.toList.map(_.fat(packageStore)),
              isEnum(tyConName, packageStore))
          case iface.TypeNumeric(scale) =>
            TypeNumeric(scale)
          case iface.TypeVar(_) =>
            throw new IllegalArgumentException(
              s"A `TypeVar` ($genType) cannot be converted to a `FullyAppliedType`"
            )
        }
      }
    }
  }

  final case class TargetOverLedgerEndError(target: String, end: String)
      extends Exception(
        s"The current latest transaction number ($end) is lower than the requested latest transaction ($target) to extract. " +
          s"Use `--to head` to extract until the current latest one, or a number that is not greater than $end."
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
