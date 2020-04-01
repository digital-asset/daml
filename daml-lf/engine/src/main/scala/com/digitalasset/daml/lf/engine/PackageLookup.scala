// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._

object PackageLookup {
  def lookupDefinition(pkg: Package, identifier: QualifiedName): Either[Error, Definition] =
    pkg.lookupIdentifier(identifier).fold(err => Left(Error(err)), Right(_))

  def lookupDataType(pkg: Package, identifier: QualifiedName): Either[Error, DDataType] =
    for {
      defn <- lookupDefinition(pkg, identifier)
      dataTyp <- defn match {
        case dataType: DDataType => Right(dataType)
        case _: DValue =>
          Left(Error(s"Got value definition instead of datatype when looking up $identifier"))
        case _: DTypeSyn =>
          Left(
            Error(s"Got type synonym definition instead of datatype when looking up $identifier"))
      }
    } yield dataTyp

  def lookupRecord(
      pkg: Package,
      identifier: QualifiedName): Either[Error, (ImmArray[(TypeVarName, Kind)], DataRecord)] =
    lookupDataType(pkg, identifier).flatMap { dataTyp =>
      dataTyp.cons match {
        case rec: DataRecord =>
          Right((dataTyp.params, rec))
        case _: DataVariant =>
          Left(Error(s"Expecting record for identifier $identifier, got variant"))
        case _: DataEnum =>
          Left(Error(s"Expecting record for identifier $identifier, got enum"))
      }
    }

  def lookupVariant(
      pkg: Package,
      identifier: QualifiedName): Either[Error, (ImmArray[(TypeVarName, Kind)], DataVariant)] =
    lookupDataType(pkg, identifier).flatMap { dataTyp =>
      dataTyp.cons match {
        case v: DataVariant =>
          Right((dataTyp.params, v))
        case _: DataRecord =>
          Left(Error(s"Expecting variant for identifier $identifier, got record"))
        case _: DataEnum =>
          Left(Error(s"Expecting variant for identifier $identifier, got enum"))
      }
    }

  def lookupEnum(pkg: Package, identifier: QualifiedName): Either[Error, DataEnum] =
    lookupDataType(pkg, identifier).flatMap { dataTyp =>
      dataTyp.cons match {
        case v: DataEnum =>
          Right(v)
        case _: DataVariant =>
          Left(Error(s"Expecting enum for identifier $identifier, got variant"))
        case _: DataRecord =>
          Left(Error(s"Expecting enum for identifier $identifier, got record"))

      }
    }

  def lookupTemplate(pkg: Package, identifier: QualifiedName): Either[Error, Template] =
    for {
      dataTyp <- lookupDataType(pkg, identifier)
      tpl <- dataTyp.cons match {
        case DataRecord(_, Some(template)) => Right(template)
        case DataRecord(_, None) =>
          Left(Error(s"Got record with no template when looking up $identifier"))
        case _: DataVariant =>
          Left(Error(s"Expecting template for identifier $identifier, got variant"))
        case _: DataEnum =>
          Left(Error(s"Expecting template for identifier $identifier, got enum"))
      }
    } yield tpl
}
