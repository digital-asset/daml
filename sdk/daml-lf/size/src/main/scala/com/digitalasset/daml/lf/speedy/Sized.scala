// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.Ref.{Identifier, Name, Party}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.speedy.SValue._

import scala.collection.immutable.TreeMap

object Sized {

  import com.digitalasset.daml.lf.Sized._
  import com.digitalasset.daml.lf.data.Sized._

  implicit lazy val SizedSUnit: SizedConstant[SUnit.type] =
    new SizedConstant(SUnit, OBJECT_HEADER_S)

  implicit object SizedSBool extends SizedFixSizeAtom[SBool](OBJECT_HEADER_S + LONG_S)

  implicit lazy val SizedSInt64: Sized[SInt64] =
    new SizedFixSizeAtom[SInt64](OBJECT_HEADER_S + LONG_S)

  implicit lazy val SizedSNumeric: SizedWrapper1[SNumeric, Numeric] =
    new SizedWrapper1[SNumeric, Numeric](SNumeric.apply, _.value)

  implicit lazy val SizedSText: SizedWrapper1[SText, String] =
    new SizedWrapper1[SText, String](SText.apply, _.value)

  implicit lazy val SizedSTimestamp: SizedWrapper1[STimestamp, Time.Timestamp] =
    new SizedWrapper1[STimestamp, Time.Timestamp](STimestamp.apply, _.value)

  implicit lazy val SizedSDate: SizedWrapper1[SDate, Time.Date] =
    new SizedWrapper1[SDate, Time.Date](SDate.apply, _.value)

  implicit lazy val SizedSContractId: SizedWrapper1[SContractId, ContractId] =
    new SizedWrapper1[SContractId, ContractId](SContractId.apply, _.value)

  implicit lazy val SizedSParty: SizedWrapper1[SParty, Party] =
    new SizedWrapper1[SParty, Ref.Party](SParty.apply, _.value)

  implicit lazy val SizedSRecord
      : SizedWrapper3[SRecord, Identifier, ImmArray[Name], Array[SValue]] =
    new SizedWrapper3[SRecord, Ref.Identifier, ImmArray[Ref.Name], Array[SValue]](
      SRecord.apply,
      (x: SRecord) => (x.id, x.fields, x.values),
    )

  implicit lazy val SizedSVariant: SizedWrapper3[SVariant, Identifier, Name, SValue] =
    new SizedWrapper3[SVariant, Ref.Identifier, Ref.Name, SValue](
      SVariant(_, _, 0, _),
      (x: SVariant) => (x.id, x.variant, x.value),
      48,
    )

  implicit lazy val SizedSEnum: SizedWrapper2[SEnum, Identifier, Name] =
    new SizedWrapper2[SEnum, Ref.Identifier, Ref.Name](
      SEnum(_, _, 0),
      (x: SEnum) => (x.id, x.constructor),
      40,
    )

  implicit lazy val SizedSList: SizedWrapper1[SList, FrontStack[SValue]] =
    new SizedWrapper1[SList, FrontStack[SValue]](SList(_), _.list)

  implicit lazy val SizedSOptional: SizedWrapper1[SOptional, Option[SValue]] =
    new SizedWrapper1[SOptional, Option[SValue]](SOptional(_), _.value)

  implicit lazy val SizedSMap: SizedWrapper1[SMap, TreeMap[SValue, SValue]] = {
    import SMap.`SMap Ordering`
    new SizedWrapper1[SMap, TreeMap[SValue, SValue]](SMap(isTextMap = false, _), _.entries, 32)
  }

  //  implicit lazy val SizedSAny: SizedWrapper2[SAny, Ast.Type, SValue] =
//    new SizedWrapper2[SAny, Ast.Type, SValue](SAny(_, _), (x: SAny) => (x.ty, x.value), 32)

  // implicit object SizedSStruct = new  SizedWrapper1[SStruct, Struct[Unit], Array[SValue]](, (x: SStruct) => x.)

//  implicit lazy val SizedSTypeRep: SizedWrapper1[STypeRep, Ast.Type] =
//    new SizedWrapper1[STypeRep, Ast.Type](STypeRep(_), _.ty, 32)

  implicit lazy val SizedSValue: Sized[SValue] = new Sized[SValue] {

    override def approximateFootprint(x: SValue): Long =
      x match {
        // case x: SPAP => SizedSPAP.approximateFootprint(x)
        case x: SRecord => SizedSRecord.approximateFootprint(x)
        // case x: SStruct => SizedSStruct.approximateFootprint(x)
        case x: SVariant => SizedSVariant.approximateFootprint(x)
        case x: SEnum => SizedSEnum.approximateFootprint(x)
        case x: SOptional => SizedSOptional.approximateFootprint(x)
        case x: SList => SizedSList.approximateFootprint(x)
        case x: SMap => SizedSMap.approximateFootprint(x)
        // case x: SAny => SizedSAny.approximateFootprint(x)
        case x: SInt64 => SizedSInt64.approximateFootprint(x)
        case x: SNumeric => SizedSNumeric.approximateFootprint(x)
        //  case x: SBigNumeric =>  SizedSBigNumeric.approximateFootprint(x)
        case x: SText => SizedSText.approximateFootprint(x)
        case x: STimestamp => SizedSTimestamp.approximateFootprint(x)
        case x: SParty => SizedSParty.approximateFootprint(x)
        case x: SBool => SizedSBool.approximateFootprint(x)
        case x: SDate => SizedSDate.approximateFootprint(x)
        case x: SContractId => SizedSContractId.approximateFootprint(x)
        // case x: STypeRep => ???
        // case x: SValue.SToken => ???
        case _ => throw new IllegalArgumentException(s"not implemente")
      }

    override def shallowFootprint(x: SValue): Long = footprint(x)

    override def approximateShallowFootprint(x: SValue): Long = approximateFootprint(x)
  }

}
