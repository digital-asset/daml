# 🏛️ Daml Studio & Canton Privacy Suite

An interactive **Daml Smart Contract Modeling Engine**, **Canton Network Privacy Sub-Ledger Verifier**, and **Institutional Choice Execution Console** for **Digital Asset Daml (`digital-asset/daml`)**.

---

## 🌟 Key Features

- 🏛️ **Daml Need-to-Know Privacy**: Define `signatories`, `observers`, and choices (`Transfer`, `Split`, `Settle`) for multi-party financial workflows.
- 🔒 **Canton Sub-Ledger Synchronization**: Verify privacy-preserving contract visibility without global state leakage.
- 🌐 **Interactive Web Studio**: Live Daml contract creator and Canton privacy inspector on `http://localhost:3427`.
- ⌨️ **Universal CLI (`daml-cli`)**: Terminal utility for creating contracts and exercising choices.

---

## 🚀 Quickstart

```bash
# Launch Daml Studio
npm start
# Open http://localhost:3427

# Or run via CLI
node bin/daml-cli.js create
node bin/daml-cli.js exercise Transfer
```
