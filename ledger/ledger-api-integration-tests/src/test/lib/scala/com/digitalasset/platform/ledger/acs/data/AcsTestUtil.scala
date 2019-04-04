// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.ledger.acs.data
import com.digitalasset.ledger.api.domain

trait AcsTestUtil {
  val parties = Set("harry", "ron", "hermione", "neville", "luna", "ginny", "voldemort")

  val templateId =
    domain.Identifier(
      moduleName = "module",
      entityName = "template",
      packageId = domain.PackageId("package-id"))
  val templateId2 =
    domain.Identifier(
      moduleName = "module",
      entityName = "template2",
      packageId = domain.PackageId("package-id2"))

  val offset1: BigInt = BigInt("1")
  val offset2: BigInt = BigInt("2")
  val offset3: BigInt = BigInt("3")
  val offsetLast: BigInt = BigInt("4")

  val workflowId = "workflow-id"
  val workflowId2 = "workflow-id-2"
}
