// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.{data => JData}
import com.daml.lf.value.{Value => LfValue}

object ValueConversion {
  def toLfValue(v: JData.Value): LfValue = v match {
    case bool: JData.Bool => ???
    case date: JData.Date => ???
    case text: JData.Text => ???
    case unit: JData.Unit => ???
    case int64: JData.Int64 => ???
    case party: JData.Party => ???
    case numeric: JData.Numeric => ???
    case variant: JData.Variant => ???
    case enum: JData.DamlEnum => ???
    case list: JData.DamlList => ???
    case timestamp: JData.Timestamp => ???
    case id: JData.ContractId => ???
    case map: JData.DamlGenMap => ???
    case record: JData.DamlRecord => ???
    case map: JData.DamlTextMap => ???
    case optional: JData.DamlOptional => ???
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
