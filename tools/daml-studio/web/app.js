/**
 * Daml Studio Client Logic
 */

let lastContractId = null;

document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  loadConfig();
  initFormListeners();
});

function initTabs() {
  const tabs = document.querySelectorAll('.nav-tab');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.nav-tab').forEach(t => t.classList.toggle('active', t === tab));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.toggle('active', p.id === `tab-${tab.dataset.tab}`));
    });
  });
}

async function loadConfig() {
  try {
    const res = await fetch('/api/config');
    const data = await res.json();

    const select = document.getElementById('select-template');
    select.innerHTML = '';

    data.templates.forEach(t => {
      const opt = document.createElement('option');
      opt.value = t.id;
      opt.textContent = t.name;
      select.appendChild(opt);
    });
  } catch (e) {
    console.error(e);
  }
}

function initFormListeners() {
  // Create Contract
  document.getElementById('create-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const templateId = document.getElementById('select-template').value;
    const issuer = document.getElementById('input-issuer').value;
    const owner = document.getElementById('input-owner').value;
    const box = document.getElementById('create-result-box');

    try {
      const res = await fetch('/api/daml/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ templateId, issuer, owner }),
      });
      const data = await res.json();
      lastContractId = data.contract.contractId;

      box.innerHTML = `
        <div class="card" style="border-color: #2563eb; background: rgba(37, 99, 235, 0.08);">
          <strong style="color: #60a5fa;">🏛️ Daml Contract Created on Canton!</strong>
          <div class="mono mt-1" style="font-size: 0.78rem; color: #10b981;">Contract ID: ${data.contract.contractId}</div>
          <div class="mono text-muted" style="font-size: 0.75rem;">Signatories: ${data.contract.signatories.join(', ')}</div>
        </div>
      `;
    } catch (err) {
      box.innerHTML = `<div class="badge red">Creation error: ${err.message}</div>`;
    }
  });

  // Exercise Choice
  document.getElementById('choice-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const choiceName = document.getElementById('select-choice').value;
    const box = document.getElementById('choice-json-box');

    try {
      const res = await fetch('/api/daml/exercise', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contractId: lastContractId, choiceName }),
      });
      const data = await res.json();
      box.textContent = JSON.stringify(data.result, null, 2);
    } catch (err) {
      box.textContent = `Error: ${err.message}`;
    }
  });

  // Privacy Verifier
  document.getElementById('privacy-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const partyName = document.getElementById('privacy-party').value;
    const box = document.getElementById('privacy-json-box');

    try {
      const res = await fetch('/api/canton/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ partyName, contractId: lastContractId }),
      });
      const data = await res.json();
      box.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
      box.textContent = `Error: ${err.message}`;
    }
  });
}
