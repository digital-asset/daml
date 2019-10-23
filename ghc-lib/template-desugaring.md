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

Next we have a `class IouInstance` with the bulk of the definitions we will need.

```haskell
class IouInstance where
  _signatoryIou : Iou -> [Party]
  _signatoryIou this@Iou{..} = [issuer, owner]
  _observerIou : Iou -> [Party]
  _observerIou this@Iou{..} = regulators
  _ensureIou : Iou -> Bool
  _ensureIou this@Iou{..} = amount > 0.0
  _agreementIou : Iou -> Text
  _agreementIou this@Iou{..} = show issuer <> " will pay " <> show owner <> " " <> show amount
  _createIou : Iou -> Update (ContractId Iou)
  _createIou = magic @"create"
  _fetchIou : ContractId Iou -> Update Iou
  _fetchIou = magic @"fetch"
  _archiveIou : ContractId Iou -> Update ()
  _archiveIou cid = exerciseIouArchive cid Archive
  _toAnyTemplateIou : Iou -> AnyTemplate
  _toAnyTemplateIou = magic @"toAnyTemplate"
  _fromAnyTemplateIou : AnyTemplate -> Optional Iou
  _fromAnyTemplateIou = magic @"fromAnyTemplate"

  _consumptionIouArchive : PreConsuming Iou
  _consumptionIouArchive = PreConsuming
  _controllerIouArchive : Iou -> Archive -> [Party]
  _controllerIouArchive this@Iou{..} arg@Archive = signatoryIou this
  _actionIouArchive : ContractId Iou -> Iou -> Archive -> Update ()
  _actionIouArchive self this@Iou{..} arg@Archive = pure ()
  _exerciseIouArchive : ContractId Iou -> Archive -> Update ()
  _exerciseIouArchive = magic @"archive"
  _toAnyChoiceIouArchive : proxy Iou -> Archive -> AnyChoice
  _toAnyChoiceIouArchive = magic @"toAnyChoice"
  _fromAnyChoiceIouArchive : proxy Iou -> AnyChoice -> Optional Archive
  _fromAnyChoiceIouArchive = magic @"fromAnyChoice"

  _consumptionIouTransfer : PreConsuming Iou
  _consumptionIouTransfer = PreConsuming
  _controllerIouTransfer : Iou -> Transfer -> [Party]
  _controllerIouTransfer this@Iou{..} arg@Transfer{..} = [owner]
  _actionIouTransfer : ContractId Iou -> Iou -> Transfer -> Update (ContractId Iou)
  _actionIouTransfer self this@Iou{..} arg@Transfer{..} = create this with owner = newOwner
  _exerciseIouTransfer : ContractId Iou -> Transfer -> Update (ContractId Iou)
  _exerciseIouTransfer = magic @"exercise"
  _toAnyChoiceIouTransfer : proxy Iou -> Transfer -> AnyChoice
  _toAnyChoiceIouTransfer = magic @"toAnyChoice"
  _fromAnyChoiceIouTransfer : proxy Iou -> AnyChoice -> Optional Transfer
  _fromAnyChoiceIouTransfer = magic @"fromAnyChoice"
```

With that class defined, we can define an `instance` declaration for `Iou` to declare its membership in `Template`:
```haskell
instance IouInstance => Template Iou where
  signatory = _signatoryIou
  observer = _observerIou
  ensure = _ensureIou
  agreement = _agreementIou
  create = _createIou
  fetch = _fetchIou
  archive = _archiveIou
  toAnyTemplate = _toAnyTemplate
  fromAnyTemplate = _fromAnyTemplate

instance IouInstance
```

When a type `t` is a `Template` instance, `class Choice` (defined by the DAML standard library) defines a (multi-parameter type class) relation on types `t`, `c` and `r` such that `r` is uniquely determined by the pair `(t, c)`:

```haskell
class Template t => Choice t c r | t c -> r where
  exercise : ContractId t -> c -> Update r
  _toAnyChoice : proxy t -> c -> AnyChoice
  _fromAnyChoice : proxy t -> AnyChoice -> Optional c
```

In this example, `c` is identified with `Transfer` and `r` with `ContractId Iou`.

The `instance` declaration establishes the triple `(Iou, Transfer, ContractId Iou)` as satisfying the `Choice` relation:

```haskell
instance Choice Iou Transfer (ContractId Iou) where
  exercise = _exerciseIouTransfer
  _toAnyChoice = _toAnyChoiceIouTransfer
  _fromAnyChoice = _fromAnyChoiceIouTransfer
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
  _signatoryEnrollment : Enrollment -> [Party]
  _signatoryEnrollment this@Enrollment{..} = [reg.student, reg.course.institution]
  _observerEnrollment : Enrollment -> [Party]
  _observerEnrollment this@Enrollment{..} = []
  _ensureEnrollment : Enrollment -> Bool
  _ensureEnrollment this@Enrollment{..} = True
  _agreementEnrollment : Enrollment -> Text
  _agreementEnrollment this@Enrollment{..} = ""
  _createEnrollment : Enrollment -> Update (ContractId Enrollment)
  _createEnrollment = magic @"create"
  _fetchEnrollment : ContractId Enrollment -> Update Enrollment
  _fetchEnrollment = magic @"fetch"
  _archiveEnrollment : ContractId Enrollment -> Update ()
  _archiveEnrollment cid = exerciseEnrollmentArchive cid Archive
  _toAnyTemplateEnrollment : Enrollment -> AnyTemplate
  _toAnyTemplateEnrollment = magic @"toAnyTemplate"
  _fromAnyTemplateEnrollment : AnyTemplate -> Optional Enrollment
  _fromAnyTemplateEnrollment = magic @"fromAnyTemplate"

  _hasKeyEnrollment : HasKey Enrollment
  _hasKeyEnrollment = HasKey
  _keyEnrollment : Enrollment -> Registration
  _keyEnrollment this@Enrollment{..} = reg
  _maintainerEnrollment : HasKey Enrollment -> Registration -> [Party]
  _maintainerEnrollment HasKey key = [key.course.institution]
  _fetchByKeyEnrollment : Registration -> Update (ContractId Enrollment, Enrollment)
  _fetchByKeyEnrollment = magic @"fetchByKey"
  _lookupByKeyEnrollment : Registration -> Update (Optional (ContractId Enrollment))
  _lookupByKeyEnrollment = magic @"lookupByKey"

  _consumptionEnrollmentArchive : PreConsuming Enrollment
  _consumptionEnrollmentArchive = PreConsuming
  _controllerEnrollmentArchive : Enrollment -> Archive -> [Party]
  _controllerEnrollmentArchive this@Enrollment{..} arg@Archive = signatoryEnrollment this
  _actionEnrollmentArchive : ContractId Enrollment -> Enrollment -> Archive -> Update ()
  _actionEnrollmentArchive self this@Enrollment{..} arg@Archive = pure ()
  _exerciseEnrollmentArchive : ContractId Enrollment -> Archive -> Update ()
  _exerciseEnrollmentArchive = magic @"archive"
  _toAnyChoiceEnrollmentArchive : proxy Enrollment -> Archive -> AnyChoice
  _toAnyChoiceEnrollmentArchive = magic @"toAnyChoice"
  _fromAnyChoiceEnrollmentArchive : proxy Enrollment -> AnyChoice -> Optional Archive
  _fromAnyChoiceEnrollmentArchive = magic @"fromAnyChoice"

instance EnrollmentInstance

instance EnrollmentInstance => Template Enrollment where
  signatory = _signatoryEnrollment
  observer = _observerEnrollment
  ensure = _ensureEnrollment
  agreement = _agreementEnrollment
  create = _createEnrollment
  fetch = _fetchEnrollment
  archive = _archiveEnrollment
  toAnyTemplate = _toAnyTemplateEnrollment
  fromAnyTemplate = _fromAnyTemplateEnrollment

instance TemplateKey Enrollment Registration where
  key = _keyEnrollment
  fetchByKey = _fetchByKeyEnrollment
  lookupByKey = _lookupByKeyEnrollment
```

### Example (3)

The final example shows a generic proposal template.

```haskell
template Template t => Proposal t with
    asset : t
    receivers : [Party]
    name : Text
  where
    signatory receivers, signatory asset
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
    _signatoryProposal : Proposal t -> [Party]
    _signatoryProposal this@Proposal{..} = signatory asset \\ receivers
    _observerProposal : Proposal t -> [Party]
    _observerProposal this@Proposal{..} = receivers
    _ensureProposal : Proposal t -> Bool
    _ensureProposal this@Proposal{..} = True
    _agreementProposal : Proposal t -> Text
    _agreementProposal this@Proposal{..} = ""
    _createProposal : Proposal t -> Update (ContractId (Proposal t))
    _createProposal = magic @"create"
    _fetchProposal : ContractId (Proposal t) -> Update (Proposal t)
    _fetchProposal = magic @"fetch"
    _archiveProposal : ContractId (Proposal t) -> Update ()
    _archiveProposal cid = exerciseProposalArchive cid Archive
    _toAnyTemplateProposal : Proposal t -> AnyTemplate
    _toAnyTemplateProposal = magic @"toAnyTemplate"
    _fromAnyTemplateProposal : AnyTemplate -> Optional (Proposal t)
    _fromAnyTemplateProposal = magic @"fromAnyTemplate"

    _hasKeyProposal : HasKey (Proposal t)
    _hasKeyProposal = HasKey
    _keyProposal : Proposal t -> ([Party], Text)
    _keyProposal this@Proposal{..} = (signatory this, name)
    _maintainerProposal : HasKey (Proposal t) -> ([Party], Text) -> [Party]
    _maintainerProposal HasKey key = fst key
    _fetchByKeyProposal : ([Party], Text) -> Update (ContractId (Proposal t), Proposal t)
    _fetchByKeyProposal = magic @"fetchByKey"
    _lookupByKeyProposal : ([Party], Text) -> Update (Optional (ContractId (Proposal t)))
    _lookupByKeyProposal = magic @"lookupByKey"

    _consumptionProposalArchive : PreConsuming (Proposal t)
    _consumptionProposalArchive = PreConsuming
    _controllerProposalArchive : Proposal t -> Archive -> [Party]
    _controllerProposalArchive this@Proposal{..} arg@Archive = signatoryProposal this
    _actionProposalArchive : ContractId (Proposal t) -> Proposal t -> Archive -> Update ()
    _actionProposalArchive self this@Proposal{..} arg@Archive = pure ()
    _exerciseProposalArchive : ContractId (Proposal t) -> Archive -> Update ()
    _exerciseProposalArchive = magic @"archive"
    _toAnyChoiceProposalArchive : proxy (Proposal t) -> Archive -> AnyChoice
    _toAnyChoiceProposalArchive = magic @"toAnyChoice"
    _fromAnyChoiceProposalArchive : proxy (Proposal t) -> AnyChoice -> Optional Archive
    _fromAnyChoiceProposalArchive = magic @"fromAnyChoice"

    _consumptionProposalAccept : PreConsuming (Proposal t)
    _consumptionProposalAccept = PreConsuming
    _controllerProposalAccept : Proposal t -> Accept -> [Party]
    _controllerProposalAccept this@Proposal{..} arg@Accept = receivers
    _actionProposalAccept : ContractId (Proposal t) -> Proposal t -> Accept -> Update (ContractId t)
    _actionProposalAccept self this@Proposal{..} arg@Accept = do
        create asset
    _exerciseProposalAccept : ContractId (Proposal t) -> Accept -> Update (ContractId t)
    _exerciseProposalAccept = magic @"exercise"
    _toAnyChoiceProposalAccept : proxy (Proposal t) -> Accept -> AnyChoice
    _toAnyChoiceProposalAccept = magic @"toAnyChoice"
    _fromAnyChoiceProposalAccept : proxy (Proposal t) -> AnyChoice -> Optional Accept
    _fromAnyChoiceProposalAccept = magic @"fromAnyChoice"

instance ProposalInstance t => Template (Proposal t) where
    signatory = _signatoryProposal
    observer = _observerProposal
    ensure = _ensureProposal
    agreement = _agreementProposal
    create = _createProposal
    fetch = _fetchProposal
    archive = _archiveProposal
    toAnyTemplate = _toAnyTemplate
    fromAnyTemplate = _fromAnyTemplate

instance ProposalInstance t => TemplateKey (Proposal t) ([Party], Text) where
    key = _keyProposal
    fetchByKey = _fetchByKeyProposal
    lookupByKey = _lookupByKeyProposal

instance ProposalInstance t => Choice (Proposal t) Accept (ContractId t) where
    exercise = _exerciseProposalAccept
    _toAnyChoice = _toAnyChoiceProposalAccept
    _fromAnyChoice = _fromAnyChoiceProposalAccept

instance ProposalInstance t => Choice (Proposal t) Archive () where
    exercise = exerciseProposalArchive
    _toAnyChoice = _toAnyChoiceProposalArchive
    _fromAnyChoice = _fromAnyChoiceProposalArchive
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
type ProposalIou = Proposal Iou
instance ProposalInstance Iou
```

The `instance` here simply leverages the implementation of the `ProposalInstance` class.
