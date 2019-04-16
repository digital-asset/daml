window.addEventListener('load', (event) => {
    var acceptButton = document.querySelector('#accept-btn');
    acceptButton.addEventListener('click', acceptClick);
});

function acceptClick(event) {
    event.preventDefault();
    event.srcElement.removeEventListener('click', acceptClick)
    setCookie("accepted", true, "/", document.location.hostname, document.location.protocol)
    location.reload(true);
}

function setCookie(sKey, sValue, sPath, sDomain, protocol) {
    if (!sKey || /^(?:expires|max\-age|path|domain|secure)$/i.test(sKey)) { return false; }
    var sExpires = "; expires=Fri, 31 Dec 9999 23:59:59 GMT";
    var bSecure = protocol.startsWith("https:") ? true : false
    document.cookie = encodeURIComponent(sKey) + "=" + encodeURIComponent(sValue) + sExpires + (sDomain ? "; domain=" + sDomain : "") + (sPath ? "; path=" + sPath : "") + (bSecure ? "; secure" : "");
    return true;
  }