package com.daml

package object cliopts {

  /** A lens-style setter. */
  type Setter[S, A] = (A => A, S) => S
}
