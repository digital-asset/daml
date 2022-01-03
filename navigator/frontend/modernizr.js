// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Variation of https://webpack.js.org/loaders/val-loader/#modernizr
const fs = require("fs");
const modernizr = require("modernizr");

module.exports = function (options = {}, loaderContext) {
  const content = fs.readFileSync(loaderContext.resourcePath, 'utf8');
  return new Promise(function (resolve) {
    modernizr.build(JSON.parse(content), function (output) {
      resolve({
        cacheable: true,
        code: `var modernizr; var hadGlobal = 'Modernizr' in window; var oldGlobal = window.Modernizr; ${output} modernizr = window.Modernizr; if (hadGlobal) { window.Modernizr = oldGlobal; } else { delete window.Modernizr; } export default modernizr;`,
      });
    });
  });
};

