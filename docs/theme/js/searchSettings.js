// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// search engine settings

window.searchEngine = 'custom'; // 'sphinx' vs 'custom'

window.customSearches = [
    {
        name : "hoogle",
        url : 'https://hoogle.daml.com/?mode=json&hoogle=',
        frequency : 100,
        inlineMinChar : 1,
        process : function(res, searchState) {
            searchState.error = null;
            searchState.results = res.map(function(item) {
                var out = {};
                out.link = item.url;
                out.title = item.item;
                out.snippet = item.docs;
                return out;
            })
        }
    },
    {
        name : "search360",
        url : 'https://api.sitesearch360.com/sites?&site=docs.daml.com&includeContent=true&highlightQueryTerms=true&limit=100&query=',
        frequency : 200,
        inlineMinChar : 3,
        process : function(res, searchState) {
            searchState.results = [];
            if ('_' in res.suggests)
                searchState.results = res.suggests["_"].map(function(item) {
                    var out = {};
                    out.link = item.link;
                    if(item.name !== "") out.title = item.name;
                    else out.title = item.link;
                    out.snippet = item.content;
                    return out;
                });
            searchState.error = null;
        }
    }
]