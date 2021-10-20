# Security tests, by category

## Authorization:
- Engine level tests for _authorization_ check.: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L39)
- Exercise within exercise: No implicit authorization from outer exercise.: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L317)
- Unit test _authorization_ computations in: `CheckAuthorization`.: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L20)

## Privacy:
- Unit test _blinding_ computation: `Blinding.blind`.: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L14)

## Semantics:
- Exceptions, throw/catch.: [ExceptionTest.scala](daml-lf/interpreter/src/test/scala/com/digitalasset/daml/lf/speedy/ExceptionTest.scala#L24)

## Performance:
- Tail call optimization: Tail recursion does not blow the scala JVM stack.: [TailCallTest.scala](daml-lf/interpreter/src/test/scala/com/digitalasset/daml/lf/speedy/TailCallTest.scala#L18)


