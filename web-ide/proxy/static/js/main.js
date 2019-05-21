// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

window.addEventListener('load', function(event) {
    var acceptButton = document.querySelector('#accept-btn');
    acceptButton.addEventListener('click', acceptClick);

    if (!supportsArrowFunction()) {
        var alert = document.querySelector('#alert');
        alert.className = 'alert'
    }

});

function supportsArrowFunction() {
    var supported = false
    try {
        eval('var f = x => 1')
        if (f && f(1) === 1) {
            supported = true 
        }
    } catch (err) {}
    return supported
}

function acceptClick(event) {
    event.preventDefault();
    event.srcElement.removeEventListener('click', acceptClick)
    setCookie("accepted", true, "/", document.location.hostname, document.location.protocol)
    location.reload(true);
}

function setCookie(sKey, sValue, sPath, sDomain, protocol) {
    if (!sKey || /^(?:expires|max\-age|path|domain|secure)$/i.test(sKey)) { return false; }
    var sExpires = "; expires=Fri, 31 Dec 9999 23:59:59 GMT";
    var bSecure = protocol === "https:";
    document.cookie = encodeURIComponent(sKey) + "=" + encodeURIComponent(sValue) + sExpires + (sDomain ? "; domain=" + sDomain : "") + (sPath ? "; path=" + sPath : "") + (bSecure ? "; secure" : "");
    return true;
}
