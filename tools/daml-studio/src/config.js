/**
 * Digital Asset Daml & Canton Network Configuration
 */

export const DAML_CONFIG = {
  architecture: {
    language: 'Daml (Digital Asset Modeling Language)',
    compiledFormat: 'Daml-LF (Daml Ledger Format)',
    privacyNetwork: 'Canton Network (Sub-Ledger Synchronization)',
    privacyModel: 'Need-to-Know Authorization (Signatories & Observers)',
  },
  sampleTemplates: [
    {
      id: 'template_asset_token',
      name: 'Institutional AssetToken Template',
      signatories: ['IssuerParty (GoldmanSachs)', 'OwnerParty (JPMorgan)'],
      observers: ['RegulatorParty (SEC/FINRA)'],
      choices: ['Transfer', 'Split', 'Redeem'],
    },
    {
      id: 'template_collateral_swap',
      name: 'Canton Cross-Party CollateralSwap Template',
      signatories: ['PledgeeParty (DTCC)', 'PledgorParty (Citigroup)'],
      observers: ['AuditParty'],
      choices: ['SettleSwap', 'CancelSwap'],
    },
  ],
};
