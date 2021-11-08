# Security tests, by category

## Authorization:
- badly-authorized create is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L61)
- badly-authorized exercise is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L159)
- badly-authorized exercise/create (create is unauthorized) is rejected: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L265)
- badly-authorized exercise/create (exercise is unauthorized) is rejected: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L233)
- badly-authorized exercise/exercise (no implicit authority from outer exercise) is rejected: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L324)
- badly-authorized fetch is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L96)
- badly-authorized lookup is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L118)
- create with no signatories is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L51)
- create with non-signatory maintainers is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L73)
- exercise with no controllers is rejected: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L149)
- well-authorized create is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L44)
- well-authorized exercise is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L142)
- well-authorized exercise/create is accepted: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L211)
- well-authorized exercise/exercise is accepted: [AuthPropagationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthPropagationSpec.scala#L367)
- well-authorized fetch is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L90)
- well-authorized lookup is accepted: [AuthorizationSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/AuthorizationSpec.scala#L112)

## Privacy:
- ensure correct privacy for create node: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L32)
- ensure correct privacy for exercise node (consuming): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L43)
- ensure correct privacy for exercise node (non-consuming): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L62)
- ensure correct privacy for exercise subtree: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L139)
- ensure correct privacy for fetch node: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L82)
- ensure correct privacy for lookup-by-key node (found): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L94)
- ensure correct privacy for lookup-by-key node (not-found): [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L116)
- ensure correct privacy for rollback subtree: [BlindingSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/BlindingSpec.scala#L201)

## Semantics:
- Exceptions, throw/catch.: [ExceptionTest.scala](daml-lf/interpreter/src/test/scala/com/digitalasset/daml/lf/speedy/ExceptionTest.scala#L24)
- contract key behaviour (non-unique mode): [ContractKeySpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/ContractKeySpec.scala#L383)
- contract key behaviour (unique mode): [ContractKeySpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/ContractKeySpec.scala#L389)
- contract keys must have a non-empty set of maintainers: [ContractKeySpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/ContractKeySpec.scala#L218)
- contract keys should be evaluated after ensure clause: [ContractKeySpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/ContractKeySpec.scala#L185)
- contract keys should be evaluated only when executing create: [ContractKeySpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/ContractKeySpec.scala#L146)

## Performance:
- Tail call optimization: Tail recursion does not blow the scala JVM stack.: [TailCallTest.scala](daml-lf/interpreter/src/test/scala/com/digitalasset/daml/lf/speedy/TailCallTest.scala#L18)

## Input Validation:
- ensure builtin operators have the correct type: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L47)
- ensure expression forms have the correct type: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L107)
- ill-formed create command is rejected: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L137)
- ill-formed create-and-exercise command is rejected: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L158)
- ill-formed exception definitions are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1078)
- ill-formed exercise command is rejected: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L142)
- ill-formed exercise-by-key command is rejected: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L149)
- ill-formed expressions are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L408)
- ill-formed fetch command is rejected: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L171)
- ill-formed fetch-by-key command is rejected: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L174)
- ill-formed kinds are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L19)
- ill-formed lookup command is rejected: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L179)
- ill-formed records are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1220)
- ill-formed templates are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L815)
- ill-formed type synonyms applications are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1199)
- ill-formed type synonyms definitions are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1266)
- ill-formed types are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L99)
- ill-formed variants are rejected: [TypingSpec.scala](daml-lf/validation/src/test/scala/com/digitalasset/daml/lf/validation/TypingSpec.scala#L1243)
- well formed create command is accepted: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L83)
- well formed create-and-exercise command is accepted: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L102)
- well formed exercise command is accepted: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L88)
- well formed exercise-by-key command is accepted: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L95)
- well formed fetch command is accepted: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L109)
- well formed fetch-by-key command is accepted: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L114)
- well formed lookup command is accepted: [CommandPreprocessorSpec.scala](daml-lf/engine/src/test/scala/com/digitalasset/daml/lf/engine/CommandPreprocessorSpec.scala#L119)


