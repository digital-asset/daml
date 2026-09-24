# Daml template syntax desugaring

Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All Rights Reserved.
SPDX-License-Identifier: (Apache-2.0 OR BSD-3-Clause)

## Introduction
Daml syntax describes contracts and the choices that operate on them. When Daml syntax is interpreted, the first step is to parse it to Haskell abstract syntax trees.

This note shows how Daml examples are desugared by showing their Haskell source equivalents, and should help you understand the connection between Daml and Haskell.

## How Daml syntax desugars

### Example (1)

Here is a contract with one choice:

```haskell
template Iou
  with
    issuer : Party
    owner : Party
    amount : Decimal
    regulators : [Party]
  where
    ensure amount > 0.0
    signatory issuer, owner
    observer regulators
    agreement show issuer <> " will pay " <> show owner <> " " <> show amount

    choice Transfer : ContractId Iou
      with
        newOwner : Party
      controller owner
      do
        create this with owner = newOwner
```

The `class Template` (defined by the Daml standard library) represents the set of all contract types:

```haskell
class Template t where
  signatory : t -> [Party]
  observer : t -> [Party]
  ensure : t -> Bool
  agreement : t -> Text
  create : t -> Update (ContractId t)
  fetch : ContractId t -> Update t
  archive : ContractId t -> Update ()
  toAnyTemplate : t -> AnyTemplate
  fromAnyTemplate : AnyTemplate -> Optional t
```

In this example, `t` is identified with `Iou`. The rest of this section shows you how desugaring proceeds.

First we have data type definitions for the `Iou` template and the `Transfer` choice.

```haskell
data Iou = Iou with
    issuer : Party
    owner : Party
    amount : Decimal
    regulators : [Party]
  deriving (Eq, Show)

data Transfer = Transfer with
    newOwner : Party
  deriving (Eq, Show)
```


Next we have the instance of the `Template` typeclass:

```haskell
instance Template Iou where
  ensure this@Iou {..} = amount > 0.0
  agreement this@Iou {..}
    = show issuer <> " will pay " <> show owner <> " " <> show amount
  signatory this@Iou {..}
    = concat
        [concat
           [toParties (owner),
            toParties (issuer)]]
  observer this@Iou {..}
    = concat
        [concat
           [concat
              [toParties (regulators)]]]
  archive cid
    = exercise cid Archive
  create = magic @"create"
  fetch = magic @"fetch"
  toAnyTemplate = magic @"toAnyTemplate"
  fromAnyTemplate = magic @"fromAnyTemplate"
  _templateTypeRep = magic @"_templateTypeRep"
```

When a type `t` is a `Template` instance, `class Choice` (defined by the Daml standard library) defines a (multi-parameter type class) relation on types `t`, `c` and `r` such that `r` is uniquely determined by the pair `(t, c)`:

```haskell
class Template t => Choice t c r | t c -> r where
  exercise : ContractId t -> c -> Update r
  _toAnyChoice : proxy t -> c -> Any
  _fromAnyChoice : proxy t -> Any -> Optional c
```

In this example, `c` is identified with `Transfer` and `r` with `ContractId Iou`.

The `instance` declaration establishes the triple `(Iou, Transfer, ContractId Iou)` as satisfying the `Choice` relation:

```haskell
instance Choice Iou Transfer (ContractId Iou) where
  exercise = magic @"exerciseIouTransfer"
  _toAnyChoice = magic @"toAnyChoiceIouTransfer"
  _fromAnyChoice
    = magic @"fromAnyChoiceIouTransfer"
```

Information about a choice that is not part of the `Choice` typeclass is recorded in a
separate top-level identifier. Specifically, this is a tuple containing the controller,
the choice body and information on whether the choice is pre-, post- or nonconsuming:

```
_choice$_IouTransfer :
  (Iou -> Transfer -> [Party],
   ContractId Iou
   -> Iou -> Transfer -> Update (ContractId Iou),
   PreConsuming Iou)
_choice$_IouTransfer
  = (\ this@Iou {..} arg@Transfer {..}
       -> let
          in
            concat
              [toParties (owner)],
     \ self this@Iou {..} arg@Transfer {..}
       -> let
          in do create (DA.Internal.Record.setField @"owner" newOwner this),
     PreConsuming)
```

### Example (2)

The next contract exercises the "contract keys" feature of Daml.
Contract key syntax desugars to `instance` declarations of the following typeclass.

```haskell
class Template t => TemplateKey t k | t -> k where
  key : t -> k
  fetchByKey : k -> Update (ContractId t, t)
  lookupByKey : k -> Update (Optional (ContractId t))
  _maintainer : proxy t -> k -> [Party]
  _toAnyContractKey : proxy t -> k -> Any
  _fromAnyContractKey : proxy t -> Any -> Optional ks
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
      maintainer key.course.institution
```

The `Course` and `Registration` data types remain as they are, but the `Enrollment` template results in several pieces after desugaring.

```haskell
data Enrollment =
  Enrollment with
    reg : Registration
  deriving (Show, Eq)

instance Template Enrollment where
  signatory this@Enrollment {..}
    = concat
        [concat
           [toParties
              ((DA.Internal.Record.getField
                  @"institution" (DA.Internal.Record.getField @"course" reg))),
            toParties
              ((DA.Internal.Record.getField @"student" reg))]]
  observer this@Enrollment {..} = concat []
  ensure this@Enrollment {..} = True
  agreement this@Enrollment {..} = ""
  archive cid
    = exercise cid Archive
  create = magic @"create"
  fetch = magic @"fetch"
  toAnyTemplate = magic @"toAnyTemplate"
  fromAnyTemplate = magic @"fromAnyTemplate"
  _templateTypeRep = magic @"_templateTypeRep"

instance TemplateKey Enrollment Registration where
  key this@Enrollment {..} = reg
  _maintainer _ key
    = concat
        [concat
           [toParties
              ((DA.Internal.Record.getField
                  @"institution" (DA.Internal.Record.getField @"course" key)))]]
  fetchByKey = magic @"fetchByKey"
  lookupByKey = magic @"lookupByKey"
  _toAnyContractKey = magic @"_toAnyContractKey"
  _fromAnyContractKey
    = magic @"_fromAnyContractKey"
```
