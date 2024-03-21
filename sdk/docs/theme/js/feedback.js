// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

function guid() {
  function s4() {
    return Math.floor((1 + Math.random()) * 0x10000)
      .toString(16)
      .substring(1);
  }
  return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
    s4() + '-' + s4() + s4() + s4();
}

window.correlation_id = guid();

document.addEventListener('mopinion_ready', function(e) {
  if(e.detail.key === "595a8379e6886f118bd0b1931aaded3c77ea0975") {
    $('.thumbs-wrapper.button').one("click", function(e) {
      var value = $(e.currentTarget).find("label").attr("title")
      setTimeout($.proxy($('.btn-submit').click, $('.btn-submit')), 100);
      console.log(e);
      if(value === "negative") {
        srv.openModal(true, "708a82385638b3e808089a278636337b67cb4901");
      }
    });
  }

  window.open_feedback = function() {
    srv.openModal(true, "708a82385638b3e808089a278636337b67cb4901");
  }
});
