// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const vscode = acquireVsCodeApi();

function toggleCheckbox(checkboxId, classId, cmdId) {
  const isChecked = document.getElementById(checkboxId).checked;
  document.body.classList.toggle(classId, !isChecked);
  vscode.postMessage({
    'command': cmdId,
    'value': isChecked
  });
}

function show_archived_changed() {
  toggleCheckbox('show_archived', 'hide_archived', 'set_show_archived')
}
function toggle_detailed_disclosure() {
  toggleCheckbox('show_detailed_disclosure', 'hidden_disclosure', 'set_show_detailed_disclosure');
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

  function showOrHideClassWithName(show, showClass, hideClass, checkBoxId) {
        document.body.classList.remove(show ? hideClass : showClass);
        document.body.classList.add(show ? showClass : hideClass);
        document.getElementById(checkBoxId).checked = show;
  };

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
      showOrHideClassWithName(message.value.showArchived, 'show_archived', 'hide_archived', 'show_archived')
      showOrHideClassWithName(message.value.showDetailedDisclosure, 'show_disclosure', 'hidden_disclosure', 'show_detailed_disclosure')
      break;
  }
});

