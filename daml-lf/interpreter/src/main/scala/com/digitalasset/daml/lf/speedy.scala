// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

package object speedy {

  def mylog(s: String): Unit = {
    println(s)
    /*import java.io.PrintWriter
    import java.io.FileWriter
    new PrintWriter(new FileWriter("/tmp/nick.log", true)) {
      write(s);
      write("\n");
      close
    }
    ()*/
  }

}
