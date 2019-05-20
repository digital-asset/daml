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

We split the current `Template` type class into two type classes `Template` and `Creatable` which serve the following purpose:
1. `Template` indicates a (potentially generic) template definition. Some instances of `Template` will not be creatable. More precisely, an instantiation of a generic template at some concrete type arguments is only creatable if there exists a corresponding `template instance` declaration.
1. `Creatable` marks all instances of `Template` which are actually creatable. These are non-generic templates as well as instantiations of generic templates for which a `template instance` has been declared.

The actual definitions of the three type classes are as follows:
```haskell
class Template t where
  signatory : t -> [Party]

class Template t => Creatable t where
  create : t -> Update (ContractId t)
  fetch : ContractId t -> Update t
```

Similarly, we split the `Choice` type class into `Choice` and `Exercisable`:
```haskell
class Template t => Choice t c r | t c -> r where
  controller : t -> c -> [Party]
  action : ContractId t -> t -> c -> Update r

class (Creatable t, Choice t c r) => Exercisable t c r | t c -> r where
  exercise : ContractId t -> c -> Update r
```

On top of these type classes, each `template` definition will generate a type class capturing all information about the defined template.

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
The desugaring of this generic template definition will yield the following definitions and instances:
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


class Creatable t => ProposalInstance t where
    createProposal : Proposal t -> Update (ContractId (Proposal t))
    createProposal = magic @"create"
    fetchProposal : ContractId (Proposal t) -> Update (Proposal t)
    fetchProposal = magic @"fetch"
    exerciseProposalAccept : ContractId (Proposal t) -> Accept -> Update (ContractId t)
    exerciseProposalAccept = magic @"exercise"

instance ProposalInstance t => Creatable (Proposal t) where
    create = createProposal
    fetch = fetchProposal

instance ProposalInstance t => Exercisable (Proposal t) Accept (ContractId t) where
    exercise = exerciseProposalAccept
```
`ProposalInstance` is the type class which captures all information about the `Proposal` template. This type class is also used to mark types `t` for which contract instances of `Proposal t` are creatable.

Let's further assume we have already defined an `Iou` template somewhere else. In order to actually be able to create an instance of `Proposal Iou`, we need to instantiate the generic template `Proposal` with `Iou` for `t`. This is written as:
```haskell
template instance ProposalIou = Proposal Iou
```
`ProposalIou` is a name _we_ pick for this instantiation. This name is only relevant for creating a contract instance of `Proposal Iou` via the Ledger API. In DAML, such a contract instance will be created using the familiar `create Proposal with ...` syntax.

The template instantiation above desugars to the following data type definitions and type class instances:
```haskell
newtype ProposalIou = MkProposalIou with unProposalIou : Proposal Iou

instance ProposalInstance Iou where
```

The converter from GHC Core to DAML-LF will take the dictionary definition for `ProposalInstance Iou` together with the `newtype ProposalIou` definition as the starting point for defining a template for `ProposalIou` in DAML-LF. The converter will also replace all the `magic @...` code with builtin DAML-LF instructions for `create`, `fetch` and `exercise` at the right types.

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


class D a => TInstance a where
  createT :: T a -> Update (ContractId (T a))
  createT = magic @"create"
  fetchT :: ContractId (T a) -> Update (T a)
  fetchT = magic @"fetch"
  exerciseTC :: ContractId (T a) -> D a -> Update R
  exerciseTC = magic @"exercise"

instance TInstance a => Creatable (T a) where
  create = createT
  fetch = fetchT

instance TInstance a => Exercisable (T a) (C a) R where
  exercise = exerciseTC
```
Extending this desugaring to more than one choice or more than one type parameter is trivial.

## Desugaring generic template instantiations

Desugaring the generic template instantion `TA = T A` from above yields:
```haskell
newtype TA = MkTA with unTA : T A

instance TInstance A where
```
Extending this desugaring to more than one type parameter is trivial.

As mentioned above, the converter from GHC Core to DAML-LF will take the dictionary definition of `TInstance A` together with the `newtype TA` definition as the starting point for defining a template `TA` in DAML-LF. The converter will also overwrite the `magic @...` parts of the dictonary for `TInstance A` with DAML-LF instructions for `create`, `fetch` and `exercise` on template type `TA`.

To decide whether a type class has originates from the desugaring of a template definition, we check that its name ends in `Instance` and its internal structure matches such a type class definition.

## Desugaring non-generic template definitions

A definition of a non-generic template `T` is desugared as if it was generic template definition with zero type parameters. Since we want non-generic templates to be creatable without an explicit `template instance` declaration, we also add an instance
```haskell
instance TInstance where
```
when desugaring the template definition.

## Risks and challenges

A challenge might arise from the current syntactical restriction on template keys. Since dropping this restriction entirely is an option, the risk posed by this challenge is very low. To solve the challenge without dropping the restriction, some sort of cross-module inlining would be necessary.
