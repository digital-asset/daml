# Generic templates

This presentation ignores a few things which are easy to implement but would add unnecessary complexity to the presentation:
- `observer`, `ensure` and `agreement` clauses in templates
- `preconsuming`, `postconsuming` and `nonconsuming` declarations
- the implicit `Archive` choice
- contract keys

We also assume that DAML-LF provides a function to coerce contract ids, which we expose as
```haskell
coerceContractId : ContractId a -> ContractId b
coerceContractId = primitive @"BECoerceContractId"
```

## Underlying type classes

We split the current `Template` type class into three type classes serving the following purpose:
1. `LfTemplate` indicates an "actual template" which will cause a template definition in DAML-LF. Our example above will yield `instance LfTemplate TA`. The `LfTemplate` type class serves the same purpose as the current `Template` type class but is not intended to be used in DAML. Instances of `LfTemplate` are generated for generic template instantiations and non-generic template definitions.
1. `Template` indicates a "logical template" which exists only in DAML but not in DAML-LF. Our example above will yield `instance D a => Template (T a)`. Instances of `Template` are generated for all template definitions, generic and non-generic ones.
1. `Creatable` links an instance of `Template` with an instance of `LfTemplate` (at the value level). Our example above will yield `instance Creatable (T A)`. Instances of `Creatable` are generated for generic template instantiations and non-generic template definitions.

The actual definitions of the three type classes are as follows:
```haskell
class LfTemplate t where
  lfSignatory : t -> [Party]
  lfCreate : t -> Update (ContractId t)
  lfCreate = magic @"create"
  lfFetch : ContractId t -> Update t
  lfFetch = magic @"fetch"

class Template t where
  signatory : t -> [Party]

class Template t => Creatable t where
  create : t -> Update (ContractId t)
  fetch : ContractId t -> Update t
```

Similarly, we split the `Choice` type class into `LfChoice`, `Choice` and `Exercisable`:
```haskell
class LfTemplate t => LfChoice t c r | t c -> r where
  lfController : t -> c -> [Party]
  lfAction : ContractId t -> t -> c -> Update r
  lfExercise : ContractId t -> c -> Update r
  lfExercise = magic @"exercise"

class Template t => Choice t c r | t c -> r where
  controller : t -> c -> [Party]
  action : ContractId t -> t -> c -> Update r

class (Creatable t, Choice t c r) => Exercisable t c r | t c -> r where
  exercise : ContractId t -> c -> Update r
```

## Desugaring by example

Before we dive into surface syntax and desugaring of generic templates and their instantiations, let's look at an example. The famous generic proposal workflow can be modelled as
```haskell
template Creatable t => Proposal t with
    asset : t
    receivers : [Party]
  where
    signatory (signatory t \\ receivers)
    observer receivers

    choice Accept : ContractId t
      controller receivers
      do
        create asset
```
The desugaring of this generic template definition will yield the following data type definitions and type class instances:
```haskell
data Proposal t = Proposal with
  asset : t
  receivers : [Party]

instance Creatable t => Template (Proposal t) where
  signatory this@Proposal{..} = signatory asset \\ receivers


data Accept = Accept

instance Creatable t => Choice (Proposal t) Accept (ContractId t) where
  controller this@Proposal{..} arg@Accept = receivers
  action self this@Proposal{..} arg@Accept = do
    create asset
```

Let's further assume we have already defined an `Iou` template somewhere else. In order to actually be able to create an instance of `Proposal Iou`, we need to instantiate the generic template `Proposal` with `Iou` for `t`. This is written as:
```haskell
template instance ProposalIou = Proposal Iou
```
`ProposalIou` is a name _we_ pick for this instantiation. This name is only relevant for creating a contract instance of `Proposal Iou` via the Ledger API. In DAML, such a contract instance will be created using the familiar `create Proposal with ...` syntax.

The template instantiation above desugars to the following data type definitions and type class instances:
```haskell
newtype ProposalIou = MkProposalIou with unProposalIou : Proposal Iou

instance LfTemplate ProposalIou where
  lfSignatories = signatories . unProposalIou

instance Creatable (Proposal Iou) where
  create t = fmap coerceContractId . lfCreate @PropsalIou . MkProposalIou
  fetch = fmap unProposalIou . lfFetch @ProposalIou . coerceContractId


instance LfChoice ProposalIou Accept (ContractId Iou) where
  lfControllers = controllers . unProposalIou
  lfAction cid = action (coerceContractId cid) . unProposalIou

instance Exercisable (Proposal Iou) Accept (ContractId Iou) where
  exercise = lfExercise @ProposalIou . coerceContractId
```

## Surface syntax

A generic template can be defined using the following syntax:
```haskell
template D a => T a with
    f_1 : U_1; ...
  where
    signatory sign

    choice C : R with g_1 : V_1; ...
      controller ctrl
      do act
```
This generic template has one type parameter `a`, which is subject to the constraint `D a`. This constraint can be empty, written as
```haskell
template T a with ...
````
All types `R`, `U_i` and `V_j` have the type variable `a` in scope.

An instantiation of this generic template with an actual type `A` in place of type parameter `a` can be declared through
```haskell
template instance TA = T A
```
`TA` is a name _we_ pick for this instantiation. This name is only relevant for creating a contract instance of `T A` via the Ledger API. In DAML, such a contract instance will be created using the familiar `create T with ...` syntax. Although the type `TA` also exists in DAML, it is not intended for use there.

Extending this syntax to more than one type parameter is trivial.

## Desugaring generic template definitions

Desugarting the generic template definition of `T` from above will generate the following data definitions type class instances:
```haskell
data T a = T with f_1 : U_1; ...

instance D a => Template (T a)
  signatory this@T{..} = sign


data C a = C with g_1 : V_1; ...

instance D a => Choice (T a) (C a) R where
  controller this@T{..} arg@C{..} = ctrl
  action self this@T{..} arg@C{..} = act
```
Extending this desugaring to more than one type parameter is trivial.

## Desugaring generic template instantiations

Desugaring the generic template instantion `TA = T A` from above yields:
```haskell
newtype TA = MkTA with unTA : T A

instance LfTemplate TA where
  lfSignatory = signatory . unTA

instance Creatable (T A) where
  create t = fmap coerceContractId . lfCreate @TA . MkTA
  fetch = fmap unTA . lfFetch @TA . coerceContractId


instance LfChoice TA (C A) R[A/a] where
  lfController = controller . unTA
  lfAction cid = action (coerceContractId cid) . unTA

instance Exercisable (T A) (C A) R[A/a] where
  exercise = lfExercise @TA . coerceContractId
```
Extending this desugaring to more than one type parameter is trivial. Our actual implementation will most likely use auxiliary functions to simplify the method definition sof the generated instances.

## Desugaring non-generic template definitions

The template definition
```haskell
template T with
    f_1 : U_1; ...
  where
    signatory sign

    choice C : R with g_1 : V_1; ...
      controller ctrl
      do act
```
gets desugared into
```haskell
data T = T with f_1 : U_1; ...

instance Template T where
  signatory this@T{..} = sign

instance LfTemplate T where
  lfSignatory = signatory

instance Creatable T where
  create = lfCreate
  fetch = lfFetch


data C = C with g_1 : V_1; ...

instance Choice T C R where
  controller this@T{..} arg@C{..} = ctrl
  action self this@T{..} arg@C{..} = act

instance LfChoice T C R where
  lfController = controller
  lfAction = action

instance Exercisable T C R where
  exercise = lfExercise
```

## Risks and challenges

The main challenge is generating the instances of `LfChoice` and `Exercisable` for the template instantiation `TA = T A`. The only solution I can envision right now is to find all instances of the shape
```haskell
instance D a => Choice (T a) (C a) R where
```
in the module `T` is defined in. Unfortunately, this can't be done in the parser but needs to run after the renamer (to find the module `T` is defined in) and before the type checker. This compiler phase can reconstruct the original template instantiation `TA = T A` easily if the parser produces the `newtype TA`, `instance LfTemplate TA` and `instance Creatable (T A)` part of the desugaring described above.

Another challenge might arise from the current syntactical restriction on template keys. Since dropping this restriction entirely is an option, the risk posed by this challenge is very low. To solve the challenge without dropping the restriction, some sort of cross-module inlining would be necessary.
