#!/usr/bin/env node

/**
 * Digital Asset Daml CLI
 */

import { defaultDamlEngine } from '../src/core/daml-engine.js';
import { defaultCantonPrivacy } from '../src/core/canton-privacy.js';

const args = process.argv.slice(2);
const command = args[0] || 'help';

async function main() {
  switch (command.toLowerCase()) {
    case 'create': {
      console.log('\n🏛️  Creating Daml Smart Contract Instance on Canton Network...');
      const contract = defaultDamlEngine.createContract({
        templateId: 'template_asset_token',
        issuer: 'GoldmanSachs',
        owner: 'JPMorgan',
        amount: 10000000,
      });
      console.log(`  Contract ID: ${contract.contractId}`);
      console.log(`  Signatories: ${contract.signatories.join(', ')}`);
      console.log(`  Observers:   ${contract.observers.join(', ')}`);
      console.log(`  Status:      ${contract.status}\n`);
      break;
    }

    case 'exercise': {
      const choice = args[1] || 'Transfer';
      console.log(`\n⚡ Exercising Daml Choice '${choice}'...`);
      const res = defaultDamlEngine.exerciseChoice({ choiceName: choice, actor: 'JPMorgan' });
      console.log(`  Exercised Choice: ${res.exercisedChoice}`);
      console.log(`  Archived Contract: ${res.archivedContractId}`);
      console.log(`  Created Contract:  ${res.createdContractId}`);
      console.log(`  Canton Privacy:    ${res.privacyGuarantee}\n`);
      break;
    }

    case 'studio': {
      console.log('\n🌐 Launching Daml Studio on :3427...');
      await import('../src/server/app.js');
      break;
    }

    default: {
      console.log(`
╔══════════════════════════════════════════════════════════════════╗
║               🏛️ DIGITAL ASSET DAML CLI                         ║
║   Smart Contract Modeling & Canton Privacy Sub-Ledger Suite      ║
╚══════════════════════════════════════════════════════════════════╝

Commands:
  daml-cli create                       Create Daml smart contract instance
  daml-cli exercise [choiceName]        Exercise choice (Transfer/Split) on contract
  daml-cli studio                       Launch Interactive Web Studio on :3427
      `);
      break;
    }
  }
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
