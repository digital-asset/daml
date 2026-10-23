# Multi-Party Agreement Tutorial

Learn Canton's unique party-based authorization model by building a collaborative agreement contract.

## What You'll Learn

- How Canton's **party model** differs from address-based blockchains
- Dynamic signatory lists and authorization
- Observer pattern for selective visibility
- Multi-party workflows without complex coordination

## The Problem

On public blockchains like Ethereum, authorization is simple: if you have the private key for an address, you can sign transactions. But what if you need multiple parties to coordinate on a decision?

Canton solves this with a **party-based authorization model** where contracts explicitly declare who must authorize actions.

## The Contract

```daml
template MultiPartyAgreement
  with
    proposer : Party
    signatories : [Party]
    requiredParties : [Party]
    terms : Text
  where
    signatory signatories
    observer requiredParties
```

### Key Concepts

**Signatories** (`signatory signatories`)
- Parties who have already signed the agreement
- ALL signatories must authorize any changes to this contract
- This is a **list that grows** as parties join

**Observers** (`observer requiredParties`)
- Parties who can see the contract but haven't signed yet
- They need visibility to exercise the `AddParty` choice
- Without observer status, Bob couldn't even see the agreement to join it

**Why both?**
- `signatories` = who has committed
- `requiredParties` = who is invited but hasn't committed yet

## The Workflow

### Step 1: Alice Proposes

```daml
agreementCid <- submit alice do
  createCmd MultiPartyAgreement with
    proposer = alice
    signatories = [alice]          -- Only Alice has signed
    requiredParties = [bob, carol]  -- Bob and Carol can see it
    terms = "We agree to collaborate on this project"
```

**What happens:**
- Alice creates the agreement
- She's the only signatory (she authorized creation)
- Bob and Carol are observers (they can see it but haven't signed)

### Step 2: Bob Joins

```daml
agreementCid <- submit bob do
  exerciseCmd agreementCid AddParty with newParty = bob
```

**What happens:**
- Bob exercises the `AddParty` choice
- The choice controller is `newParty` (Bob), so he must authorize
- A new contract is created with `signatories = [bob, alice]`
- The old contract is archived (consumed by the choice)

### Step 3: Carol Joins

```daml
submit carol do
  exerciseCmd agreementCid AddParty with newParty = carol
```

**Final state:**
- `signatories = [carol, bob, alice]`
- All three parties have now authorized the agreement

## The Choice

```daml
choice AddParty : ContractId MultiPartyAgreement
  with
    newParty : Party
  controller newParty
  do
    assertMsg "Party not in required list" (newParty `elem` requiredParties)
    assertMsg "Party already signed" (newParty `notElem` signatories)
    create this with signatories = newParty :: signatories
```