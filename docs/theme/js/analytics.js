// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

$(document).ready(function () {
  function attachEvent(elem, type) {
    elem.click(function() {
      gtag('event', 'click', {
        'event_category' : elem.text(),
        'event_label' : elem.attr("href")
      })
    })
  }
  $('a.external').each(function() { attachEvent( $(this), 'external'); });
  $('.hs-cta-wrapper').each(function() { attachEvent( $(this), 'cta'); });
  $('a.download.internal').each(function() { attachEvent( $(this), 'download'); });
  $('div.pdf-download a').each(function() { attachEvent( $(this), 'pdf'); });
});