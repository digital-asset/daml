/**
 * Digital Asset Daml & Canton Unit Tests
 */

import { defaultDamlEngine } from '../src/core/daml-engine.js';
import { defaultCantonPrivacy } from '../src/core/canton-privacy.js';

async function runDamlTests() {
  console.log('Testing Digital Asset Daml Engine & Canton Privacy System...');

  // 1. Create Daml Contract
  const contract = defaultDamlEngine.createContract({
    templateId: 'template_asset_token',
    issuer: 'GoldmanSachs',
    owner: 'JPMorgan',
  });
  if (!contract.contractId || contract.signatories.length !== 2) {
    throw new Error('Daml contract creation failed');
  }

  // 2. Exercise Choice
  const choiceRes = defaultDamlEngine.exerciseChoice({
    contractId: contract.contractId,
    choiceName: 'Transfer',
    actor: 'JPMorgan',
  });
  if (!choiceRes.createdContractId) {
    throw new Error('Daml choice execution failed');
  }

  // 3. Canton Privacy Verification
  const privRes = defaultCantonPrivacy.verifySubLedgerPrivacy({
    partyName: 'GoldmanSachs',
    contractId: contract.contractId,
  });
  if (!privRes.isAuthorizedToView) {
    throw new Error('Canton privacy verification failed');
  }

  console.log(`✅ Daml Contract Lifecycle & Canton Privacy Verified (${contract.contractId})!`);
}

runDamlTests().catch(e => {
  console.error('❌ Daml Test Failed:', e);
  process.exit(1);
});
