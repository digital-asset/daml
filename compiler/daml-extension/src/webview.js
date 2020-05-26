// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const vscode = acquireVsCodeApi();
function show_archived_changed() {
  document.body.classList.toggle('hide_archived', !document.getElementById('show_archived').checked);
}
function toggle_view() {
  document.body.classList.toggle('hide_transaction');
  document.body.classList.toggle('hide_table');
  vscode.postMessage({
    'command': 'selected_view',
    'value': document.body.classList.contains('hide_transaction') ? 'table' : 'transaction'
  });
}
window.addEventListener('message', event => {
  const message = event.data;
  switch (message.command) {
    case 'add_note':
        document.body.classList.remove('hide_note');
        document.getElementById('note').innerHTML = message.value;
        break;
    case 'select_view':
      switch (message.value) {
        case 'transaction':
          document.body.classList.remove('hide_transaction');
          document.body.classList.add('hide_table');
          break;
        case 'table':
          document.body.classList.add('hide_transaction');
          document.body.classList.remove('hide_table');
          break;
        default:
          console.log('Unexpected value for select_view: ' + message.value);
          break;
      }
      break;
  }
});

