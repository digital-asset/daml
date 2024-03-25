// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.DamlTextMap
import com.squareup.javapoet.{ClassName, TypeName}

object Types {
  // All the relevant Daml-LF primitives as they are represented in the API
  // ContractId is missing from the mapping because it's always used in its boxed form
  val apiBoolean = ClassName.get(classOf[javaapi.data.Bool])
  val apiInt64 = ClassName.get(classOf[javaapi.data.Int64])
  val apiNumeric = ClassName.get(classOf[javaapi.data.Numeric])
  val apiText = ClassName.get(classOf[javaapi.data.Text])
  val apiTimestamp = ClassName.get(classOf[javaapi.data.Timestamp])
  val apiParty = ClassName.get(classOf[javaapi.data.Party])
  val apiDate = ClassName.get(classOf[javaapi.data.Date])
  val apiList = ClassName.get(classOf[javaapi.data.DamlList])
  val apiOptional = ClassName.get(classOf[javaapi.data.DamlOptional])
  val apiUnit = ClassName.get(classOf[javaapi.data.Unit])
  val apiContractId = ClassName.get(classOf[javaapi.data.ContractId])
  val apiTextMap = ClassName.get(classOf[DamlTextMap])
  val apiGenMap = ClassName.get(classOf[javaapi.data.DamlGenMap])
  val apiCollectors = ClassName.get(classOf[javaapi.data.DamlCollectors])

  // All the types part of the Java platform to which API types will be mapped to
  val javaNativeBoolean = TypeName.get(java.lang.Boolean.TYPE)
  val javaBoxedBoolean = ClassName.get(classOf[java.lang.Boolean])
  val javaNativeLong = TypeName.get(java.lang.Long.TYPE)
  val javaBoxedLong = ClassName.get(classOf[java.lang.Long])
  val javaBigDecimal = ClassName.get(classOf[java.math.BigDecimal])
  val javaString = ClassName.get(classOf[java.lang.String])
  val javaInstant = ClassName.get(classOf[java.time.Instant])
  val javaLocalDate = ClassName.get(classOf[java.time.LocalDate])
  val javaList = ClassName.get(classOf[java.util.List[_]])
  val javaOptional = ClassName.get(classOf[java.util.Optional[_]])
  val javaMap = ClassName.get(classOf[java.util.Map[_, _]])
}
