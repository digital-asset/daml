// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

$(document).ready(function () {
    var $topbar = $('.topbar');
    var $content = $('.wy-nav-content');
    var contentPaddingTop = $content.css('padding-top');

    function scrollHeader() {
        var scroll = $(window).scrollTop();
        if (scroll > 0) {
            $topbar.addClass('fixed');
            $topbar.css('width', $('.wy-nav-content-wrap').css('width'));
            $content.css('padding-top', $topbar.height() + parseInt(contentPaddingTop));
        } else {
            $topbar.removeClass('fixed');
            $topbar.css('width', '100%');
            $content.css('padding-top', contentPaddingTop);
        }
    }

    $(window).scroll(function () {
        scrollHeader();
    });

    $(window).resize(function () {
        scrollHeader();
    });

    scrollHeader();
});
