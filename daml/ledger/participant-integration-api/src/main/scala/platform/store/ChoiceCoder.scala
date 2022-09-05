// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.lf.data.Ref

// TODO: https://github.com/digital-asset/daml/issues/12051
//  Drop this workaround.
@deprecated
private object ChoiceCoder {

  /*
     Inherited choices are ambiguous: one needs the ID of the interface where
     the choice is defined to disambiguate them.

     Until interfaces are made stable, we do not want to change/migrate the
     schema of the DBs, so we encode the choice identifier as follows:
       - If the choice is defined in the template we let it such as.
         This is backward compatible, simple, and cheap at runtime
       - If the choice is inherited from an interface, we mangle the
         tuple `(interfaceId, choiceName)` as follows:
           "#" + interfaceId.toString + "#" + choiceName
         Note that `#` is illegal within Identifiers and Names.

      Once we have decided to make interfaces stable, we will plan on how to
      handle more nicely interface IDs in the DBs.
   */

  def encode(mbInterfaceId: Option[Ref.Identifier], choiceName: String): String =
    mbInterfaceId match {
      case None => choiceName
      case Some(ifaceId) => "#" + ifaceId.toString + "#" + choiceName
    }

  def decode(s: String): (Option[Ref.Identifier], String) =
    if (s.startsWith("#"))
      s.split("#") match {
        case Array(_, ifaceId, chName) =>
          (Some(Ref.Identifier.assertFromString(ifaceId)), chName)
      }
    else
      (None, s)

}
