# Simple Token Tutorial

Learn how to build fungible tokens on Canton using patterns from the CIP-56 Canton Token Standard.

## What You'll Learn

- UTXO-style asset management
- Proposal/acceptance pattern for transfers (CIP-56)
- Split and merge operations
- Token holder privacy model
- Observer pattern for issuer oversight

## The Token Holding

```daml
template TokenHolding
  with
    issuer : Party
    owner : Party
    amount : Decimal
    instrument : Text
  where
    signatory owner
    observer issuer
```

### Key Design Decisions

**Why owner is the only signatory?**
- Owner controls their assets
- Can propose transfers without issuer approval
- Standard pattern for bearer tokens

**Why issuer is an observer?**
- Issuer sees all holdings (for compliance, total supply)
- Issuer doesn't control transfers (can't freeze without owner consent)
- Balance between privacy and transparency
agement |

## The Transfer Flow (CIP-56 Pattern)

### Why Proposal + Acceptance?

**Problem:** Alice can't just create a holding for Bob
```daml
-- This FAILS - Bob must authorize becoming owner (signatory)
create TokenHolding with owner = bob, ...
```

**Solution:** Two-step workflow

### Step 1: Alice Proposes Transfer

```daml
choice ProposeTransfer : ContractId TransferProposal
  with
    newOwner : Party
    transferAmount : Decimal
  controller owner
  do
    -- Create remainder for Alice
    if transferAmount < amount
      then create this with amount = amount - transferAmount
      else pure ()
    
    -- Create proposal for Bob
    create TransferProposal with
      sender = owner
      receiver = newOwner
      transferAmount
      ...
```

**What happens:**
- Alice's 100 token holding is **consumed** (archived)
- A 70 token holding is **created** for Alice (remainder)
- A **transfer proposal** is created for Bob to accept

### Step 2: Bob Accepts

```daml
template TransferProposal
  where
    signatory sender
    observer receiver  -- Bob can see the proposal
    
    choice Accept : ContractId TokenHolding
      controller receiver
      do
        create TokenHolding with
          owner = receiver
          amount = transferAmount
          ...
```

**What happens:**
- Bob exercises `Accept` (he authorizes)
- A 30 token holding is **created** for Bob
- The proposal is **consumed**

**Final state:**
- Alice: 70 token holding
- Bob: 30 token holding
- Both authorized their own holdings