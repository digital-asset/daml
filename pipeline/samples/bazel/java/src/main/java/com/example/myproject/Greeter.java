// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.example.myproject;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Scanner;

/**
 * Prints a greeting which can be customized by building with resources and/or passing in command-
 * line arguments.
 *
 * <p>Building and running this file will print "Hello world". Build and run the
 * //pipeline/samples/bazel/java:hello-world target to demonstrate this.</p>
 *
 * <p>If this is built with a greeting.txt resource included, it will replace "Hello" with the
 * contents of greeting.txt. The
 * //pipeline/samples/bazel/java:hello-resources target demonstrates this.</p>
 *
 * <p>If arguments are passed to the binary on the command line, the first argument will replace
 * "world" in the output. See //pipeline/samples/bazel/java:hello's argument test.</p>
 */
public class Greeter {
  static PrintStream out = System.out;

  public static String convertStreamToString(InputStream is) throws Exception {
    try (Scanner s = new Scanner(is)) {
      s.useDelimiter("\n");
      return s.hasNext() ? s.next() : "";
    }
  }

  public void hello(String obj) throws Exception {
    String greeting = "Hello";
    InputStream stream  = Greeter.class.getResourceAsStream("/greeting.txt");
    if (stream != null) {
      greeting = convertStreamToString(stream).trim();
    }
    out.println(greeting + " " + obj);
  }

  public static void main(String... args) throws Exception {
    Greeter g = new Greeter();
    String obj = args.length > 0 ? args[0] : "world";
    g.hello(obj);
  }
}
