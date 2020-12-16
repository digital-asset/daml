// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import com.daml.lf.CompiledPackages
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.lf.transaction.TransactionOuterClass.Transaction

private[lf] object Submissions {

  def newBuilder(): SubmissionsBuilder =
    new SubmissionsBuilder(Map.newBuilder[PackageId, Package], Vector.newBuilder[Transaction])

}

private[lf] final case class Submissions(
    compiledPackages: CompiledPackages,
    transactions: Vector[EncodedTransaction],
) {
  def values: Iterator[EncodedValueWithType] = {
    val extract = new TypedValueExtractor(compiledPackages.signatures)
    transactions.iterator.flatMap(extract.fromTransaction)
  }
}
