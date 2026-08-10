/**
 * Daml & Canton Studio Web Server
 */

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { DAML_CONFIG } from '../config.js';
import { defaultDamlEngine } from '../core/daml-engine.js';
import { defaultCantonPrivacy } from '../core/canton-privacy.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const WEB_ROOT = path.join(__dirname, '../../web');

const app = express();
const PORT = process.env.PORT || 3427;

app.use(cors());
app.use(express.json());
app.use(express.static(WEB_ROOT));

// 1. Config & Templates
app.get('/api/config', (req, res) => {
  res.json({
    architecture: DAML_CONFIG.architecture,
    templates: DAML_CONFIG.sampleTemplates,
  });
});

// 2. Create Daml Contract
app.post('/api/daml/create', (req, res) => {
  try {
    const contract = defaultDamlEngine.createContract(req.body);
    res.json({ success: true, contract });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// 3. Exercise Daml Choice
app.post('/api/daml/exercise', (req, res) => {
  try {
    const result = defaultDamlEngine.exerciseChoice(req.body);
    res.json({ success: true, result });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// 4. Verify Canton Privacy
app.post('/api/canton/verify', (req, res) => {
  const result = defaultCantonPrivacy.verifySubLedgerPrivacy(req.body);
  res.json(result);
});

if (process.env.NODE_ENV !== 'test') {
  app.listen(PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🏛️  Digital Asset Daml & Canton Studio Running!`);
    console.log(`🌐 Web Dashboard: http://localhost:${PORT}`);
    console.log(`🔒 Privacy Model: Canton Network Need-to-Know Sub-Ledger`);
    console.log(`======================================================\n`);
  });
}

export default app;
