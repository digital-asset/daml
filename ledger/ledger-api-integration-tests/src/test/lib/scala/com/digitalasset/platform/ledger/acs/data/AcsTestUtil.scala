// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.ledger.acs.data
import com.digitalasset.daml.lf.data.Ref

trait AcsTestUtil {
  val parties = Set("harry", "ron", "hermione", "neville", "luna", "ginny", "voldemort")

  val templateId =
    Ref.Identifier(
      Ref.PackageId.assertFromString("package-id"),
      Ref.QualifiedName.assertFromString("module:template")
    )
  val templateId2 =
    Ref.Identifier(
      Ref.PackageId.assertFromString("package-id2"),
      Ref.QualifiedName.assertFromString("module:template2")
    )

  val offset1: BigInt = BigInt("1")
  val offset2: BigInt = BigInt("2")
  val offset3: BigInt = BigInt("3")
  val offsetLast: BigInt = BigInt("4")

  val workflowId = "workflow-id"
  val workflowId2 = "workflow-id-2"
}
