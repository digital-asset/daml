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
    agreement issuer <> " will pay " <> owner <> " " <> show amount

    choice Transfer : ContractId Iou
      with
        newOwner : Party
      controller owner
      do
        create this with owner = newOwner
```

The `class Template` (defined by the DAML standard library) represents the set of all contract types:

```haskell
class Template t where
  signatory : t -> [Party]
  observer : t -> [Party]
  ensure : t -> Bool
  agreement : t -> Text
  create : t -> Update (ContractId t)
  fetch : ContractId t -> Update t
  archive : ContractId t -> Update ()
```

In this example, `t` is identified with `Iou`. The rest of this section shows you how desugaring proceeds.

First we have data type definitions for the `Iou` template and the `Transfer` choice.

```haskell
data Iou = Iou with
    issuer : Party
    owner : Party
    currency : Party
    amount : Decimal
    account : Party
    regulators : [Party]
  deriving (Eq, Show)

data Transfer = Transfer with
    newOwner : Party
  deriving (Eq, Show)
```

Next we have a `class IouInstance` with the bulk of the definitions we will need.

```haskell
class IouInstance where
  signatoryIou : Iou -> [Party]
  signatoryIou this@Iou{..} = [issuer, owner]
  observerIou : Iou -> [Party]
  observerIou this@Iou{..} = regulators
  ensureIou : Iou -> Bool
  ensureIou this@Iou{..} = amount > 0.0
  agreementIou : Iou -> Text
  agreementIou this@Iou{..} = show issuer <> " will pay " <> show owner <> " " <> show amount
  createIou : Iou -> Update (ContractId Iou)
  createIou = magic @"create"
  fetchIou : ContractId Iou -> Update Iou
  fetchIou = magic @"fetch"
  archiveIou : ContractId Iou -> Update ()
  archiveIou cid = exerciseIouArchive cid Archive

  consumptionIouArchive : PreConsuming Iou
  consumptionIouArchive = PreConsuming
  controllerIouArchive : Iou -> Archive -> [Party]
  controllerIouArchive this@Iou{..} arg@Archive = signatoryIou this
  actionIouArchive : ContractId Iou -> Iou -> Archive -> Update ()
  actionIouArchive self this@Iou{..} arg@Archive = pure ()
  exerciseIouArchive : ContractId Iou -> Archive -> Update ()
  exerciseIouArchive = magic @"archive"

  consumptionIouTransfer : PreConsuming Iou
  consumptionIouTransfer = PreConsuming
  controllerIouTransfer : Iou -> Transfer -> [Party]
  controllerIouTransfer this@Iou{..} arg@Transfer{..} = [owner]
  actionIouTransfer : ContractId Iou -> Iou -> Transfer -> Update (ContractId Iou)
  actionIouTransfer self this@Iou{..} arg@Transfer{..} = create this with owner = newOwner
  exerciseIouTransfer : ContractId Iou -> Transfer -> Update (ContractId Iou)
  exerciseIouTransfer = magic @"exercise"
```

With that class defined, we can define an `instance` declaration for `Iou` to declare its membership in `Template`:
```haskell
instance IouInstance => Template Iou where
  signatory = signatoryIou
  observer = observerIou
  ensure = ensureIou
  agreement = agreementIou
  create = createIou
  fetch = fetchIou
  archive = archiveIou

instance IouInstance where
```

When a type `t` is a `Template` instance, `class Choice` (defined by the DAML standard library) defines a (multi-parameter type class) relation on types `t`, `c` and `r` such that `r` is uniquely determined by the pair `(t, c)`:

```haskell
class Template t => Choice t c r | t c -> r where
  exercise : ContractId t -> c -> Update r
```

In this example, `c` is identified with `Transfer` and `r` with `ContractId Iou`.

The `instance` declaration establishes the triple `(Iou, Transfer, ContractId Iou)` as satisfying the `Choice` relation:

```haskell
instance Choice Iou Transfer (ContractId Iou) where
  exercise = exerciseIouTransfer
```

### Example (2)

The next contract exercises the "contract keys" feature of DAML.
Contract key syntax desugars to `instance` declarations of the following typeclass.

```haskell
class Template t => TemplateKey t k | t -> k where
  key : t -> k
  fetchByKey : k -> Update (ContractId t, t)
  lookupByKey : k -> Update (Optional (ContractId t))
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

class EnrollmentInstance where
  signatoryEnrollment : Enrollment -> [Party]
  signatoryEnrollment this@Enrollment{..} = [reg.student, reg.course.institution]
  observerEnrollment : Enrollment -> [Party]
  observerEnrollment this@Enrollment{..} = []
  ensureEnrollment : Enrollment -> Bool
  ensureEnrollment this@Enrollment{..} = True
  agreementEnrollment : Enrollment -> Text
  agreementEnrollment this@Enrollment{..} = ""
  createEnrollment : Enrollment -> Update (ContractId Enrollment)
  createEnrollment = magic @"create"
  fetchEnrollment : ContractId Enrollment -> Update Enrollment
  fetchEnrollment = magic @"fetch"
  archiveEnrollment : ContractId Enrollment -> Update ()
  archiveEnrollment cid = exerciseEnrollmentArchive cid Archive

  hasKeyEnrollment : HasKey Enrollment
  hasKeyEnrollment = HasKey
  keyEnrollment : Enrollment -> Registration
  keyEnrollment this@Enrollment{..} = reg
  maintainerEnrollment : HasKey Enrollment -> Registration -> [Party]
  maintainerEnrollment HasKey key = [key.course.institution]
  fetchByKeyEnrollment : Registration -> Update (ContractId Enrollment, Enrollment)
  fetchByKeyEnrollment = magic @"fetchByKey"
  lookupByKeyEnrollment : Registration -> Update (Optional (ContractId Enrollment))
  lookupByKeyEnrollment = magic @"lookupByKey"

  consumptionEnrollmentArchive : PreConsuming Enrollment
  consumptionEnrollmentArchive = PreConsuming
  controllerEnrollmentArchive : Enrollment -> Archive -> [Party]
  controllerEnrollmentArchive this@Enrollment{..} arg@Archive = signatoryEnrollment this
  actionEnrollmentArchive : ContractId Enrollment -> Enrollment -> Archive -> Update ()
  actionEnrollmentArchive self this@Enrollment{..} arg@Archive = pure ()
  exerciseEnrollmentArchive : ContractId Enrollment -> Archive -> Update ()
  exerciseEnrollmentArchive = magic @"archive"

instance EnrollmentInstance where

instance EnrollmentInstance => Template Enrollment where
  signatory = signatoryEnrollment
  observer = observerEnrollment
  ensure = ensureEnrollment
  agreement = agreementEnrollment
  create = createEnrollment
  fetch = fetchEnrollment
  archive = archiveEnrollment

instance TemplateKey Enrollment Registration where
  key = keyEnrollment
  fetchByKey = fetchByKeyEnrollment
  lookupByKey = lookupByKeyEnrollment
```

### Example (3)

The final example shows a generic proposal template.

```haskell
template Template t => Proposal t with
    asset : t
    receivers : [Party]
    name : Text
  where
    signatory (signatory asset \\ receivers)
    observer receivers
    key (signatory this, name)
    maintainer (fst key)
    choice Accept : ContractId t
      controller receivers
      do
        create asset
```

Notice that the `Proposal` template has a type argument `t` with a `Template` constraint preceding it.
We also specify a primary key for the Proposal template by combining data from the underlying template as well as the proposal.
This desugars to the following declarations.

```haskell
data Proposal t = Proposal with
    asset : t
    receivers : [Party]
    name : Party
  deriving (Eq, Show)

data Accept = Accept with
  deriving (Eq, Show)

class Template t => ProposalInstance t where
    signatoryProposal : Proposal t -> [Party]
    signatoryProposal this@Proposal{..} = signatory asset \\ receivers
    observerProposal : Proposal t -> [Party]
    observerProposal this@Proposal{..} = receivers
    ensureProposal : Proposal t -> Bool
    ensureProposal this@Proposal{..} = True
    agreementProposal : Proposal t -> Text
    agreementProposal this@Proposal{..} = implode
        [ "Proposal:\n"
        , "* proposers: " <> show (signatory this) <> "\n"
        , "* receivers: " <> show receivers <> "\n"
        , "* agreement: " <> agreement asset
        ]
    createProposal : Proposal t -> Update (ContractId (Proposal t))
    createProposal = magic @"create"
    fetchProposal : ContractId (Proposal t) -> Update (Proposal t)
    fetchProposal = magic @"fetch"
    archiveProposal : ContractId (Proposal t) -> Update ()
    archiveProposal cid = exerciseProposalArchive cid Archive

    hasKeyProposal : HasKey (Proposal t)
    hasKeyProposal = HasKey
    keyProposal : Proposal t -> ([Party], Text)
    keyProposal this@Proposal{..} = (signatory this, name)
    maintainerProposal : HasKey (Proposal t) -> ([Party], Text) -> [Party]
    maintainerProposal HasKey key = fst key
    fetchByKeyProposal : ([Party], Text) -> Update (ContractId (Proposal t), Proposal t)
    fetchByKeyProposal = magic @"fetchByKey"
    lookupByKeyProposal : ([Party], Text) -> Update (Optional (ContractId (Proposal t)))
    lookupByKeyProposal = magic @"lookupByKey"

    consumptionProposalArchive : PreConsuming (Proposal t)
    consumptionProposalArchive = PreConsuming
    controllerProposalArchive : Proposal t -> Archive -> [Party]
    controllerProposalArchive this@Proposal{..} arg@Archive = signatoryProposal this
    actionProposalArchive : ContractId (Proposal t) -> Proposal t -> Archive -> Update ()
    actionProposalArchive self this@Proposal{..} arg@Archive = pure ()
    exerciseProposalArchive : ContractId (Proposal t) -> Archive -> Update ()
    exerciseProposalArchive = magic @"archive"

    consumptionProposalAccept : PreConsuming (Proposal t)
    consumptionProposalAccept = PreConsuming
    controllerProposalAccept : Proposal t -> Accept -> [Party]
    controllerProposalAccept this@Proposal{..} arg@Accept = receivers
    actionProposalAccept : ContractId (Proposal t) -> Proposal t -> Accept -> Update (ContractId t)
    actionProposalAccept self this@Proposal{..} arg@Accept = do
        create asset
    exerciseProposalAccept : ContractId (Proposal t) -> Accept -> Update (ContractId t)
    exerciseProposalAccept = magic @"exercise"

instance ProposalInstance t => Template (Proposal t) where
    signatory = signatoryProposal
    observer = observerProposal
    ensure = ensureProposal
    agreement = agreementProposal
    create = createProposal
    fetch = fetchProposal
    archive = archiveProposal

instance ProposalInstance t => TemplateKey (Proposal t) ([Party], Text) where
    key = keyProposal
    fetchByKey = fetchByKeyProposal
    lookupByKey = lookupByKeyProposal

instance ProposalInstance t => Choice (Proposal t) Accept (ContractId t) where
    exercise = exerciseProposalAccept

instance ProposalInstance t => Choice (Proposal t) Archive () where
    exercise = exerciseProposalArchive
```

### Example (3)(cont)

We showed the generic proposal template above, but have not showed what an instance looks like.
Let's instantiate the `Proposal` template with the `Iou` (concrete) template from Example 1.
This is done using the syntax below.

```haskell
template instance ProposalIou = Proposal Iou
```

This allows us to create and exercise choices on a proposal contract instantiated to an Iou contract.
The name `ProposalIou` is not needed in DAML code but is required when creating contracts via the Ledger API
(as client languages may not be able to express generic template and type instantiation).
The `template instance` desugars to the following declarations.

```haskell
newtype ProposalIou = ProposalIou (Proposal Iou)
instance ProposalInstance Iou where
```

The `instance` here simply leverages the implementation of the `ProposalInstance` class.