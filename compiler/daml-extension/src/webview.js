// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const vscode = acquireVsCodeApi();
function show_archived_changed() {
  const isChecked = document.getElementById('show_archived').checked;
  document.body.classList.toggle('hide_archived', !isChecked);
  vscode.postMessage({
    'command': 'set_show_archived',
    'value': isChecked
  });
}
function toggle_detailed_disclosure() {
  const isChecked = document.getElementById('show_detailed_disclosure').checked;
  document.body.classList.toggle('hidden_disclosure', !isChecked);
  vscode.postMessage({
    'command': 'set_show_detailed_disclosure',
    'value': isChecked
  });
}
function toggle_view() {
  document.body.classList.toggle('hide_transaction');
  document.body.classList.toggle('hide_table');
  vscode.postMessage({
    'command': 'set_selected_view',
    'value': document.body.classList.contains('hide_transaction') ? 'table' : 'transaction',
  });
}
window.addEventListener('message', event => {
  const message = event.data;
  switch (message.command) {
    case 'add_note':
        document.body.classList.remove('hide_note');
        document.getElementById('note').innerHTML = message.value;
        break;
    case 'set_view':
      switch (message.value.selected) {
        case 'transaction':
          document.body.classList.remove('hide_transaction');
          document.body.classList.add('hide_table');
          break;
        case 'table':
          document.body.classList.add('hide_transaction');
          document.body.classList.remove('hide_table');
          break;
        default:
          console.log('Unexpected value for select_view: ' + message.value.selected);
          break;
      }
      if (message.value.showArchived) {
        document.body.classList.remove('hide_archived');
        document.body.classList.add('show_archived');
        document.getElementById('show_archived').checked = true;
      } else {
        document.body.classList.remove('show_archived');
        document.body.classList.add('hide_archived');
        document.getElementById('show_archived').checked = false;
      }
      if (message.value.showDetailedDisclosure) {
        document.body.classList.remove('hidden_disclosure');
        document.body.classList.add('show_disclosure');
        document.getElementById('show_detailed_disclosure').checked = true;
      } else {
        document.body.classList.remove('show_disclosure');
        document.body.classList.add('hidden_disclosure');
        document.getElementById('show_detailed_disclosure').checked = false;
      }

      break;
  }
});

