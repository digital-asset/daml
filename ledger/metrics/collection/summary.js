#!/usr/bin/env node

// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const http = require("http");

const window = "6hours";

const url = target =>
  `http://localhost:2000/render?target=summarize(${target},%20%27${window}%27,%20%27last%27)&from=-${window}&format=json`;

function fetch(url) {
  return new Promise((resolve, reject) => {
    http.get(url, res => {
      res.setEncoding("utf8");
      let body = "";
      res.on("data", data => {
        body += data;
      });
      res.on("end", () => {
        resolve(JSON.parse(body));
      });
      res.on("error", reject);
    });
  });
}

function toDict(rawData) {
  const result = {};
  rawData.forEach(row => {
    const words = row.tags.name.split(".");
    const target = words.slice(undefined, -1).join(".");
    const stat = words[words.length - 1];
    const data = row.datapoints[row.datapoints.length - 1][0];
    result[target] = result[target] || {};
    result[target][stat] = data;
  });
  return result;
}

const formatNumber = num =>
  num === undefined || isNaN(num)
    ? ""
    : Math.round(num * 100 + Number.EPSILON) / 100;

function toCsv(dictData) {
  const result = [];
  for (const key in dictData) {
    const rowData = dictData[key];
    const count = formatNumber(rowData.count);
    const mean = formatNumber(rowData.mean);
    const total = formatNumber(rowData.mean * rowData.count);
    const min = formatNumber(rowData.min);
    const max = formatNumber(rowData.max);
    result.push(`${key}, ${count}, ${mean}, ${total}, ${min}, ${max}`);
  }
  return "Metric, Count, Mean, Total, Min, Max\n" + result.sort().join("\n");
}

async function fetchData() {
  const maxDepth = 7;
  const roots = ["daml", "jvm"];

   // [1, 2, 3, ...]
  const depths = Array.from(Array(maxDepth), (_, i) => i + 1);

  // ["*", "*.*", "*.*.*", ...]
  const stars = depths.map(depth =>
    Array.from(Array(depth), () => "*").join(".")
  );

  // Array of all URLs to fetch
  const urls = roots.reduce(
    (acc, root) =>
      acc.concat(
        stars.map(path => url(`${root}.${path}.{mean,count,min,max}`))
      ),
    []
  );

  const nestedData = await Promise.all(urls.map(fetch));
  return Array.prototype.concat(...nestedData);
}

async function run() {
  const rawData = await fetchData();
  const dictData = toDict(rawData);
  const csvData = toCsv(dictData);
  console.log(csvData);
}

run();
