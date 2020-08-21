package com.daml

package object logging {
  type LoggingContextOf[+P] = LoggingContextOf.Module.Instance.T[P]
}
