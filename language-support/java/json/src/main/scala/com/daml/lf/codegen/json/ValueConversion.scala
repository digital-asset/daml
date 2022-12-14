// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.{data => JData}
import com.daml.lf.value.{Value => LfValue}

object ValueConversion {
  def toLfValue(v: JData.Value): LfValue = v match {
    case record: JData.DamlRecord => ???
    case variant: JData.Variant => ???
    case id: JData.ContractId => ???
    case list: JData.DamlList => ???
    case optional: JData.DamlOptional => ???
    case textMap: JData.DamlTextMap => ???
    case genMap: JData.DamlGenMap => ???
    case enum: JData.DamlEnum => ???
    case int64: JData.Int64 => ???
    case numeric: JData.Numeric => ???
    case text: JData.Text => ???
    case timestamp: JData.Timestamp => ???
    case date: JData.Date => ???
    case party: JData.Party => ???
    case bool: JData.Bool => ???
    case unit: JData.Unit => ???
  }

  def fromLfValue(lfV: LfValue): JData.Value = lfV match {
    case LfValue.ValueRecord(tycon, fields) => ???
    case LfValue.ValueVariant(tycon, variant, value) => ???
    case LfValue.ValueContractId(value) => ???
    case LfValue.ValueList(values) => ???
    case LfValue.ValueOptional(value) => ???
    case LfValue.ValueTextMap(value) => ???
    case LfValue.ValueGenMap(entries) => ???
    case LfValue.ValueEnum(tycon, value) => ???
    case LfValue.ValueInt64(value) => ???
    case LfValue.ValueNumeric(value) => ???
    case LfValue.ValueText(value) => ???
    case LfValue.ValueTimestamp(value) => ???
    case LfValue.ValueDate(value) => ???
    case LfValue.ValueParty(value) => ???
    case LfValue.ValueBool(value) => ???
    case LfValue.ValueUnit => ???
  }
}
