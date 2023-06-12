// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._

import com.daml.lf.speedy.SValue._
import com.daml.lf.language.Ast

object CheckUpgrade {

  private def mayUpgrade(from: TypeConName, into: TypeConName) = { // NICK: inline

    val fromP: PackageId = from.packageId
    val fromQ: QualifiedName = from.qualifiedName
    val intoP: PackageId = into.packageId
    val intoQ: QualifiedName = into.qualifiedName

    val _ = (fromP, intoP)

    // Check if qualified names are the same, ignore package-id
    val sameQ = (fromQ == intoQ)

    // println(s"mayUpgrade (sameQ=$sameQ): \n- from=${fromP}--$fromQ\n- into=${intoP}--$intoQ")

    // TODO NICK: may only upgrade when fields are compatible

    sameQ

  }

  def tryUpgradeOrDowngrade(
      from: TypeConName,
      into: TypeConName,
      lookup: TypeConName => Ast.Type,
      record: SRecord,
  ): Option[SRecord] = {

    val _ = lookup(_) // NICK,TODO

    if (mayUpgrade(from, into)) {

      record match {
        case SRecord(_, b, c) =>
          // NICK: TODO: adapt the record here: manufacturing new optional fields; removing old fields; updating the type
          // first step: match the tycon name match. use "into"
          Some(SRecord(into, b, c))
      }
    } else {
      None
    }
  }

}
