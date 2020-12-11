// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.transaction.TransactionOuterClass.Transaction

import scala.collection.mutable

final class SubmissionsBuilder(
    packages: mutable.Builder[(PackageId, Package), Map[PackageId, Package]],
    transactions: mutable.Builder[Transaction, Vector[Transaction]],
) {
  def +=(fullPackage: (PackageId, Package)): Unit = {
    packages += fullPackage
    ()
  }
  def +=(transaction: Transaction): Unit = {
    transactions += transaction
    ()
  }
  def result(): Submissions =
    Submissions(
      PureCompiledPackages(packages.result()).fold(sys.error, identity),
      transactions.result(),
    )
}
