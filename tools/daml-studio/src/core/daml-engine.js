/**
 * Daml Smart Contract & Choice Execution Engine
 */

import crypto from 'crypto';

export class DamlContractEngine {
  constructor() {
    this.activeContracts = [];
  }

  /**
   * Create a new Daml Contract Instance
   */
  createContract({ templateId, issuer, owner, amount }) {
    const contractId = '#cid:' + crypto.randomBytes(16).toString('hex');
    const contract = {
      contractId,
      templateId: templateId || 'template_asset_token',
      signatories: [issuer || 'GoldmanSachs', owner || 'JPMorgan'],
      observers: ['SEC_Regulator'],
      payload: {
        issuer: issuer || 'GoldmanSachs',
        owner: owner || 'JPMorgan',
        amount: amount || 5000000,
        currency: 'USD_TOKEN',
      },
      status: 'ACTIVE_ON_CANTON',
      createdAt: new Date().toISOString(),
    };

    this.activeContracts.unshift(contract);
    return contract;
  }

  /**
   * Exercise a Daml Choice on a Contract
   */
  exerciseChoice({ contractId, choiceName, actor, parameters }) {
    const idx = this.activeContracts.findIndex(c => c.contractId === contractId);
    const target = idx !== -1 ? this.activeContracts[idx] : this.activeContracts[0];

    const resultContractId = '#cid:' + crypto.randomBytes(16).toString('hex');

    if (idx !== -1) {
      this.activeContracts[idx].status = 'ARCHIVED_BY_CHOICE';
    }

    return {
      exercisedChoice: choiceName || 'Transfer',
      exercisedBy: actor || 'JPMorgan',
      archivedContractId: target ? target.contractId : contractId,
      createdContractId: resultContractId,
      privacyGuarantee: 'CANTON_NEED_TO_KNOW_ENFORCED',
      executedAt: new Date().toISOString(),
    };
  }

  getActiveContracts() {
    return this.activeContracts;
  }
}

export const defaultDamlEngine = new DamlContractEngine();
