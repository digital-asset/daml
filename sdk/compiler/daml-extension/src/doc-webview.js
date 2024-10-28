// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

function scrollToDef(name) {
  let elem = document.getElementsByName(name)[0];
  if (!elem) return;
  window.scroll(0, elem.getBoundingClientRect().top + document.documentElement.scrollTop);
}

window.onload = function() {
  document.querySelectorAll('a[href^="#"]').forEach(a => a.onclick = function() {
    scrollToDef(a.getAttribute('href').substring(1))
  });
};

window.addEventListener('message', event => scrollToDef(event.data));

