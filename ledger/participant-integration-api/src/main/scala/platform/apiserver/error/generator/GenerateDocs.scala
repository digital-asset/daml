package com.daml.platform.apiserver.error.generator

import com.daml.error.generator.ErrorCodeDocumentationGenerator

case object GenerateDocs extends App {
  println(ErrorCodeDocumentationGenerator().getDocItemsAsJson) // TODO Tudor: support multiple scan paths
}
