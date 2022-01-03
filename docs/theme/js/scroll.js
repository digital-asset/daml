// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

$(document).ready(function () {
    var $topbar = $('.topbar');
    var $content = $('.wy-nav-content');
    var contentPaddingTop = $content.css('padding-top');

    function setHeader() {            
        $topbar.addClass('fixed');
        $topbar.css('width', $('.wy-nav-content-wrap').css('width'));
        $content.css('padding-top', $topbar.height() + 30 + parseInt(contentPaddingTop));
    }

    $(window).resize(function () {
        setHeader();
    });

    setHeader();
});
