# Security tests, by category

## Authorization:
- `create` with no signatories is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L50)
- `create` with non-signatory maintainers is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L72)
- `exercise` with no controllers is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L148)
- badly-authorized `create` is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L60)
- badly-authorized `exercise` is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L158)
- badly-authorized `exercise`/`create` (`create` is unauthorized) is rejected: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L265)
- badly-authorized `exercise`/`create` (`exercise` is unauthorized) is rejected: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L233)
- badly-authorized `exercise`/`exercise` (no implicit authority from outer `exercise`) is rejected: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L324)
- badly-authorized `fetch` is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L95)
- badly-authorized `lookup` is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L117)
- well-authorized `create` is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L43)
- well-authorized `exercise` is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L141)
- well-authorized `exercise`/`create` is accepted: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L211)
- well-authorized `exercise`/`exercise` is accepted: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L367)
- well-authorized `fetch` is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L89)
- well-authorized `lookup` is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L111)

## Privacy:
- ensure correct privacy for create node: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L32)
- ensure correct privacy for exercise node (consuming): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L43)
- ensure correct privacy for exercise node (non-consuming): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L62)
- ensure correct privacy for exercise subtree: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L137)
- ensure correct privacy for fetch node: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L82)
- ensure correct privacy for lookup node (found): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L94)
- ensure correct privacy for lookup node (not-found): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L115)
- ensure correct privacy for rollback subtree: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L199)

## Semantics:
- Exceptions, throw/catch.: [ExceptionTest.scala](daml-lf/interpreter/src/test/scala/com/digitalasset/daml/lf/speedy/ExceptionTest.scala#L24)

## Performance:
- Tail call optimization: Tail recursion does not blow the scala JVM stack.: [TailCallTest.scala](daml-lf/interpreter/src/test/scala/com/digitalasset/daml/lf/speedy/TailCallTest.scala#L18)

## Input Validation:
- ensure builtin operators have the correct type: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L47)
- ensure expression forms have the correct type: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L107)
- ill-formed exception definitions are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1078)
- ill-formed expressions are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L408)
- ill-formed kinds are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L19)
- ill-formed records are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1220)
- ill-formed templates are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L815)
- ill-formed type synonyms applications are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1199)
- ill-formed type synonyms definitions are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1266)
- ill-formed types are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L99)
- ill-formed variants are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1243)


