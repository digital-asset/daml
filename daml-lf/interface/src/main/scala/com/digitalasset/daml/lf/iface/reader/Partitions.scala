// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.iface
package reader

import ErrorFormatter._
import InterfaceReader.{InterfaceReaderError, dottedName}
import com.digitalasset.daml.lf.data.Ref.DottedName
import com.digitalasset.daml_lf.DamlLf1
import scalaz.{-\/, ==>>, Order, \/-}

import scala.collection.JavaConverters._
import scala.collection.breakOut

case class Partitions(
    templates: List[DamlLf1.DefTemplate] = List.empty,
    records: Map[DottedName, DamlLf1.DefDataType] = Map.empty,
    variants: Map[DottedName, DamlLf1.DefDataType] = Map.empty,
    enums: Map[DottedName, DamlLf1.DefDataType] = Map.empty,
    errors: List[(String, InterfaceReaderError)] = List.empty
) {
  def errorTree[Loc: Order](implicit kloc: String => Loc): Errors[Loc, InterfaceReaderError] =
    Errors(\/-(==>>(errors map {
      case (k, v) => (kloc(k), Errors.point[Loc, InterfaceReaderError](v))
    }: _*)))
}

object Partitions {
  def apply(m: DamlLf1.Module): Partitions = partition(m)

  private def partition(m: DamlLf1.Module): Partitions = {
    val templates = m.getTemplatesList.asScala.toList
    val dataTypes = m.getDataTypesList.asScala.filter(_.getSerializable)
    val (errors, recsVars) = partitionEithers(dataTypes map (partitionDDT(_)))
    val partitions = recsVars.groupBy(_._1)
    Partitions(
      templates = templates,
      records = partitions.getOrElse(DDT.RECORD, List.empty).map(_._2)(breakOut),
      variants = partitions.getOrElse(DDT.VARIANT, List.empty).map(_._2)(breakOut),
      enums = partitions.getOrElse(DDT.ENUM, List.empty).map(_._2)(breakOut),
      errors = errors.toList
    )
  }

  private sealed trait DDT
  private object DDT {
    case object RECORD extends DDT
    case object VARIANT extends DDT
    case object ENUM extends DDT
  }

  private def partitionDDT(a: DamlLf1.DefDataType)
    : (String, InterfaceReaderError) Either (DDT, (DottedName, DamlLf1.DefDataType)) = {
    import DamlLf1.DefDataType.{DataConsCase => DCC}
    (a.getDataConsCase match {
      case DCC.RECORD => dottedName(a.getName) map (x => (DDT.RECORD, (x, a)))
      case DCC.VARIANT => dottedName(a.getName) map (x => (DDT.VARIANT, (x, a)))
      case DCC.ENUM => dottedName(a.getName) map (x => (DDT.ENUM, (x, a)))
      case DCC.DATACONS_NOT_SET =>
        -\/(invalidDataTypeDefinition(a, "DamlLf1.DefDataType.DataConsCase.DATACONS_NOT_SET"))
    }).leftMap((a.toString, _)).toEither
  }
}
