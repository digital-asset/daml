# DAML template syntax desugaring

Copyright 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: (Apache-2.0 OR BSD-3-Clause)

## Introduction
DAML syntax describes contracts and the choices that operate on them. When DAML syntax is interpreted, the first step is to parse it to Haskell abstract syntax trees.

This note shows how DAML examples are desugared by showing their Haskell source equivalents, and should help you understand the connection between DAML and Haskell.

## How DAML syntax desugars

### Example (1)

Here is a contract with one choice:

```haskell
template Iou
  with
    issuer : Party
    owner : Party
    amount : Float
    regulators : [Party]
  where
    ensure amount > 0
    signatory issuer, owner
    observer regulators
    agreement issuer <> " will pay " <> owner <> " " <> (show amount)

    choice Transfer : ContractId Iou
      with
        newOwner : Party
      controller owner
      do
        create this with owner = newOwner
```

The `class Template` (defined by the DAML standard library) represents the set of all contract types:

```haskell
class Template c where
    -- | Predicate that must hold for the succesful creation of the contract.
    ensure : c -> Bool ; ensure _ = True
    -- | The signatories of a contract.
    signatory : c -> [Party]
    -- | The observers of a contract.
    observer : c -> [Party] ; observer _ = []
    -- | The agreement text of a contract.
    agreement : c -> Text ; agreement _ = ""
```

In this example, `c` is identified with `Iou`. The rest of this section shows you how desugaring proceeds.

First, the definition of `Iou`:

```haskell
data Iou = Iou {
    issuer : Party
  , owner : Party
  , currency : Party
  , amount : Float
  , account : Party
  , regulators :[Party] } deriving (Eq, Show)
```

Next, an `instance` declaration for `Iou` to declare its membership in `Template`:

```haskell
instance Template Iou where
  ensure this@Iou{..} = amount > 0.0
  signatory this@Iou{..} = concat [toParties issuer, toParties owner]
  observer this@Iou{..} = concat [toParties owner, toParties regulators]
  agreement this@Iou{..} = issuer <> " will pay " <> owner <>  " " <> (show amount)
```

When a type `c` is a `Template` instance, `class Choice` (defined by the DAML standard library) defines a (multi-parameter type class) relation on types `c`, `e` and `r` such that `r` is uniquely determined by the pair `(c, e)`:

```haskell
class Template c => Choice c e r | c e -> r where
    consuming : NoEvent c e -> ChoiceType ; consuming _ = Consuming
    choiceController : c -> e -> [Party]
    choice : c -> ContractId c -> e -> Update r
```

In this example, `e` is identified with `Transfer` and `r` with `ContractId Iou`.

Desugaring first defines type `Transfer`:

```haskell
data Transfer = Transfer {
  newOwner : String } deriving (Eq, Show)
```

Next, an `instance` declaration establishes the triple `(Iou, Transfer, ContractID Iou)` as satisfying the `Choice` relation:

```haskell
instance Choice Iou Transfer (ContractId Iou) where
  choiceController this@Iou{..} arg@Transfer{..} = [owner]
  choice this@Iou{..} self arg@Transfer{..} = create this with owner = newOwner
```

### Example (2)

Here is a contract with two choices, this time using an alternative syntax (that predates the `choice` keyword):

```haskell
template Iou
  with
    issuer : Party
    owner : Party
    currency : Party
    amount : Float
    regulators : [Party]
  where
      ensure amount > 0
      signatory issuer, owner
      observer regulators
      agreement issuer <> " will pay " <> owner <> " " <> (show amount)
      controller [owner] can
        Transfer : ContractId Iou
        with
          newOwner : String
        do
          create this with owner = newOwner
        Split : (ContractId Iou, ContractId Iou)
        with
          splitAmount : Float
        do
          let restAmount = amount - splitAmount
          splitCid <- create this with amount = splitAmount
          restCid <- create this with amount = restAmount
          return (splitCid, restCid)
```

As before, `Iou` is identified with `c` and generates a `data` and `instance` declaration:

```haskell
data Iou = Iou {
    issuer : Party
  , owner : Party
  , amount : Float
  , regulators : [Party] } deriving (Eq, Show)

instance Template Iou where
  ensure this@Iou{..} = amount > 0.0
  signatory this@Iou{..} = concat [toParties issuer, toParties owner]
  observer this@Iou{..} = concat [toParties owner, toParties regulators]
  agreement this@Iou{..} = issuer <> " will pay " <> owner <> " " <> (show amount)
```

The two choices lead to two `instance Choice Iou e r` declarations, one for each of the triples `(Iou, Split, (ContractID Iou, ContractID Iou))` and `(Iou, Transfer, ContractID Iou)`:

```haskell
data Split = Split { splitAmount : Float } deriving (Eq)

instance Choice Iou Split (ContractId Iou, ContractId Iou) where
  choiceController this@Iou{..} arg@Split{..} = [owner]
  choice this@Iou{..} self arg@Split{..} = do
    let  restAmount = amount - splitAmount
    splitCid <- create this with amount = splitAmount
    restCid <- create this with amount = restAmount
    return (splitCid, restCid)

data Transfer = Transfer { newOwner : String } deriving (Eq, Show)

instance Choice Iou Transfer (ContractId Iou) where
  choiceController this@Iou{..} arg@Transfer{..} = [owner]
  choice this@Iou{..} self arg@Transfer{..} = create this with owner = newOwner
```

### Example (3)

The next contract exercises the so-called "contract keys" feature of DAML. Contract key syntax desugars to `instance` declarations of the following typeclass.
```haskell
class Template c => TemplateKey c k | c -> k where
  key : c ->
  maintainer : c -> [Party]
```
In the following `Enrollment` contract, there are no choices but there are declarations of `key` and `maintainer`.
```haskell
data Course =
  Course with
      institution : Party
      title : Text
  deriving (Show, Eq)

data Registration =
  Registration with
      student : Party
      course : Course
      year : Int
  deriving (Show, Eq)

template Enrollment
  with
      reg : Registration
  where
      signatory reg.student, reg.course.institution
      key reg : Registration
      maintainer reg.course.institution
```
What the above desugars to is shown below.
```haskell
data Course = ...
data Registration = ...

instance Template Enrollment where
  signatory this@Enrollment{..} = concat [toParties reg.student, toParties reg.course.institution]

instance TemplateKey Enrollment Registration where
  key this@Enrollment{..} = reg
  maintainer this@Enrollment{..} = concat [toParties reg.course.institution]
```
