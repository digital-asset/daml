// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.lf.data.Ref.PackageId
import com.daml.navigator.console._

case object Package extends SimpleCommand {
  def name: String = "package"

  def description: String = "Print DAML-LF package details"

  def params: List[Parameter] = List(
    ParameterPackageId("id", "Package ID")
  )

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      arg1 <- args.headOption ~> "Missing <id> argument"
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      packId <- PackageId.fromString(arg1).toOption ~> s"Invalid <id> argument $arg1"
      pack <- ps.packageRegistry.pack(packId) ~> s"Unknown package $arg1"
    } yield {
      val header = List(
        "Module",
        "Type",
        "Name",
        "Size"
      )
      val templates = pack.templates.toList.map(
        t =>
          (
            t._1.qualifiedName.module.toString,
            "Template",
            t._1.qualifiedName.name.toString,
            t._2.toString.length.toString))
      val typeDefs = pack.typeDefs.toList.map(
        t =>
          (
            t._1.qualifiedName.module.toString,
            "TypeDef",
            t._1.qualifiedName.name.toString,
            t._2.toString.length.toString))
      val data = (templates ::: typeDefs)
        .sortBy(_._3)
        .sortBy(_._2)
        .sortBy(_._1)
        .map(r => List(r._1, r._2, r._3, r._4))
      (state, Pretty.asciiTable(state, header, data))
    }
  }

}
