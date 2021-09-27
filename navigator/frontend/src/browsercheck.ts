// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * This file implements a simple check to make sure the browser supports all required features.
 *
 * It is meant to catch cases where users run Navigator on ancient browsers (IE9), or browsers
 * with a limited feature set (some mobile browsers).
 *
 * For this reason, the browser check is implemented in a separate file, does not use any
 * advanced JavaScript language features, and uses a simple alert() to notify the user.
 */

import Modernizr from 'modernizr';

// List of required features.
// Note that the properties on the Modernizr object have different names than
// in the `../modernizr-config.json` object.
const features = [
  'csscalc',
  'flexbox',
  'nthchild',
  'es5array',
  'es5date',
  'es5function',
  'es5object',
  'es5',
  'strictmode',
  'es5string',
  'es5syntax',
  'es5undefined',
  // 'promises', - we use a polyfill for this
  'history',
  'json',
  // 'fetch', - we use a polyfill for this
];

// Find all features from above list that are not supported in current browser
const missingFeatures = [];
console.log(typeof Modernizr);
console.log(Object.keys(Modernizr));
console.log(Modernizr);
console.log(Modernizr["es5undefined"]);
for (let i = 0; i < features.length; ++i) {
  const feature = features[i]
  if (!Modernizr[feature]) {
    missingFeatures.push(feature);
  }
}

// Notify user if any required feature is missing
if (missingFeatures.length > 0) {
  alert(`Warning: Navigator is not supported on your browser.
  Please refer to the documentation for system requirements.

  You may proceed using the app at your own risk.

  Missing features: ${missingFeatures.join(', ')}`);
}
