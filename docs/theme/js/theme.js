// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

var jQuery = (typeof (window) != 'undefined') ? window.jQuery : require('jquery');

// Sphinx theme nav state
function ThemeNav() {

    var nav = {
        navBar: null,
        win: null,
        winScroll: false,
        winResize: false,
        linkScroll: false,
        winPosition: 0,
        winHeight: null,
        docHeight: null,
        isRunning: false
    };

    nav.enable = function (withStickyNav) {
        var self = this;

        // TODO this can likely be removed once the theme javascript is broken
        // out from the RTD assets. This just ensures old projects that are
        // calling `enable()` get the sticky menu on by default. All other cals
        // to `enable` should include an argument for enabling the sticky menu.
        if (typeof (withStickyNav) == 'undefined') {
            withStickyNav = true;
        }

        if (self.isRunning) {
            // Only allow enabling nav logic once
            return;
        }

        self.isRunning = true;
        jQuery(function ($) {
            self.init($);

            self.reset();
            self.win.on('hashchange', self.reset);

            if (withStickyNav) {
                // Set scroll monitor
                self.win.on('scroll', function () {
                    if (!self.linkScroll) {
                        if (!self.winScroll) {
                            self.winScroll = true;
                            requestAnimationFrame(function () {
                                self.onScroll();
                            });
                        }
                    }
                });
            }

            // Set resize monitor
            self.win.on('resize', function () {
                if (!self.winResize) {
                    self.winResize = true;
                    requestAnimationFrame(function () {
                        self.onResize();
                    });
                }
            });

            self.onResize();
        });

    };

    // TODO remove this with a split in theme and Read the Docs JS logic as
    // well, it's only here to support 0.3.0 installs of our theme.
    nav.enableSticky = function () {
        this.enable(true);
    };

    nav.init = function ($) {
        var doc = $(document),
            self = this;

        this.navBar = $('div.wy-side-scroll:first');
        this.win = $(window);

        // Set up javascript UX bits
        $(document)
        // Shift nav in mobile when clicking the menu.
            .on('click', "[data-toggle='wy-nav-top']", function () {
                $("[data-toggle='wy-nav-shift']").toggleClass("shift");
                $("[data-toggle='rst-versions']").toggleClass("shift");
            })

            // Nav menu link click operations
            .on('click', ".wy-menu-vertical .current ul li a", function () {
                var target = $(this);
                // Close menu when you click a link.
                $("[data-toggle='wy-nav-shift']").removeClass("shift");
                $("[data-toggle='rst-versions']").toggleClass("shift");
                // Handle dynamic display of l3 and l4 nav lists
                self.toggleCurrent(target);
                self.hashChange();
            })
            .on('click', "[data-toggle='rst-current-version']", function () {
                $("[data-toggle='rst-versions']").toggleClass("shift-up");
            })

        // Make tables responsive
        $("table.docutils:not(.field-list,.footnote,.citation)")
            .wrap("<div class='wy-table-responsive'></div>");

        // Add extra class to responsive tables that contain
        // footnotes or citations so that we can target them for styling
        $("table.docutils.footnote")
            .wrap("<div class='wy-table-responsive footnote'></div>");
        $("table.docutils.citation")
            .wrap("<div class='wy-table-responsive citation'></div>");

        // Add expand links to all parents of nested ul
        $('.wy-menu-vertical ul').not('.simple').siblings('a').each(function () {
            var link = $(this);
            expand = $('<span class="toctree-expand"></span>');
            expand.on('click', function (ev) {
                self.toggleCurrent(link);
                ev.stopPropagation();
                return false;
            });
            link.prepend(expand);
        });
    };

    nav.reset = function () {
        // Get anchor from URL and open up nested nav
        var anchor = encodeURI(window.location.hash) || '#';

        try {
            var vmenu = $('.wy-menu-vertical');
            var link = vmenu.find('[href="' + anchor + '"]');
            if (link.length === 0) {
                // this link was not found in the sidebar.
                // Find associated id element, then its closest section
                // in the document and try with that one.
                var id_elt = $('.document [id="' + anchor.substring(1) + '"]');
                var closest_section = id_elt.closest('div.section');
                link = vmenu.find('[href="#' + closest_section.attr("id") + '"]');
                if (link.length === 0) {
                    // still not found in the sidebar. fall back to main section
                    link = vmenu.find('[href="#"]');
                }
            }
            // If we found a matching link then reset current and re-apply
            // otherwise retain the existing match
            if (link.length > 0) {
                $('.wy-menu-vertical .current').removeClass('current');
                link.addClass('current');
                link.closest('li.toctree-l1').addClass('current');
                link.closest('li.toctree-l1').parent().addClass('current');
                link.closest('li.toctree-l1').addClass('current');
                link.closest('li.toctree-l2').addClass('current');
                link.closest('li.toctree-l3').addClass('current');
                link.closest('li.toctree-l4').addClass('current');
                link[0].scrollIntoView();
            }
        } catch (err) {
            console.log("Error expanding nav for anchor", err);
        }

    };

    nav.onScroll = function () {
        this.winScroll = false;
        var newWinPosition = this.win.scrollTop(),
            winBottom = newWinPosition + this.winHeight,
            navPosition = this.navBar.scrollTop(),
            newNavPosition = navPosition + (newWinPosition - this.winPosition);
        if (newWinPosition < 0 || winBottom > this.docHeight) {
            return;
        }
        this.navBar.scrollTop(newNavPosition);
        this.winPosition = newWinPosition;
    };

    nav.onResize = function () {
        this.winResize = false;
        this.winHeight = this.win.height();
        this.docHeight = $(document).height();
    };

    nav.hashChange = function () {
        this.linkScroll = true;
        this.win.one('hashchange', function () {
            this.linkScroll = false;
        });
    };

    nav.toggleCurrent = function (elem) {
        var parent_li = elem.closest('li');
        parent_li.siblings('li.current').removeClass('current');
        parent_li.siblings().find('li.current').removeClass('current');
        parent_li.find('> ul li.current').removeClass('current');
        parent_li.toggleClass('current');
    }

    return nav;
}

module.exports.ThemeNav = ThemeNav();

if (typeof (window) != 'undefined') {
    window.SphinxRtdTheme = {
        Navigation: module.exports.ThemeNav,
        // TODO remove this once static assets are split up between the theme
        // and Read the Docs. For now, this patches 0.3.0 to be backwards
        // compatible with a pre-0.3.0 layout.html
        StickyNav: module.exports.ThemeNav,
    };
}

// requestAnimationFrame polyfill by Erik Möller. fixes from Paul Irish and Tino Zijdel
// https://gist.github.com/paulirish/1579671
// MIT license

(function () {
    var lastTime = 0;
    var vendors = ['ms', 'moz', 'webkit', 'o'];
    for (var x = 0; x < vendors.length && !window.requestAnimationFrame; ++x) {
        window.requestAnimationFrame = window[vendors[x] + 'RequestAnimationFrame'];
        window.cancelAnimationFrame = window[vendors[x] + 'CancelAnimationFrame']
            || window[vendors[x] + 'CancelRequestAnimationFrame'];
    }

    if (!window.requestAnimationFrame)
        window.requestAnimationFrame = function (callback, element) {
            var currTime = new Date().getTime();
            var timeToCall = Math.max(0, 16 - (currTime - lastTime));
            var id = window.setTimeout(function () {
                    callback(currTime + timeToCall);
                },
                timeToCall);
            lastTime = currTime + timeToCall;
            return id;
        };

    if (!window.cancelAnimationFrame)
        window.cancelAnimationFrame = function (id) {
            clearTimeout(id);
        };
}());

// da custom scripts

function menu() {
    var $contentMenu = $('.content-menu');
    var $contentMenuWrapper = $('.content-menu-wrapper');
    var $contentMenuToc = $('.content-menu-toc');
    if ($(window).width() >= 1600) {
        var contentMenuOffsetTop = 172;
        // $contentMenu.removeClass('collapsed');
        if ($contentMenuToc.innerHeight() + contentMenuOffsetTop + 160 >= $(window).height()) {
            $contentMenu.css({
                'height': $(window).height() - contentMenuOffsetTop - 50
            });
            $contentMenuWrapper.css({
                'height': '100%'
            });
        } else {
            $contentMenu.css({
                'height': $contentMenuToc.innerHeight() + 110
            });
            $contentMenuWrapper.css({
                'height': '100%'
            });
        }
    } else {
        var height = Math.min($(window).height() - 150, $contentMenuToc.innerHeight() + 60);
        $contentMenu.css({
            'height': 'auto'
        });
        $contentMenuWrapper.css({
            'height': "" + height + "px"
        });
    }
}

function scrollContentMenu($sections) {
    var currentScroll = $(this).scrollTop();
    var $currentSection;
    $sections.each(function () {
        var divPosition = $(this).offset().top - $('.topbar').height() - 25;
        if (divPosition - 1 < currentScroll + $('.topbar').height() + 25) {
            $currentSection = $(this);
        }
    });
    var id = '';
    if ($currentSection !== undefined) {
        id = $currentSection.attr('id');
    }
    var $activeLink = $('.content-menu a[href="#' + id + '"]');
    if ($activeLink.length > 0) {
        $('.content-menu-toc li').removeClass('active');
        $activeLink.parent().addClass('active');
        $activeLink[0].scrollIntoView({block: "center"});
    }
}

function setHighlighterWidth() {
    var contentWidth = $('.wy-nav-content').outerWidth();
    $('div[class*="highlight"]:has(div[class*="highlight"])').css('width', contentWidth + 'px');
}

function performSearch(fullSearchState, search, results, inline) {
    var query = fullSearchState.latestQuery;
    var searchState = fullSearchState[search.name];
    if (!inline || query.length >= search.inlineMinChar) {
        clearTimeout(searchState.timeout);
        searchState.timeout = setTimeout(function () {
            var url = search.url + query;
            $('.search-inline .search-alert').removeClass('active');
            fetch(url)
                .then(function (response) {
                    if (!response.ok) {
                        throw new Error(response.status);
                    } else {
                        return response.json();
                    }
                })
                .then(function(res) {
                    if (res !== undefined) {
                        searchState.query = query;
                        search.process(res, searchState);
                        updateResults(fullSearchState, results, 0);
                    } else {
                        searchState.query = query;
                        searchState.results = [];
                        searchState.error = "unedined response";
                        updateResults(fullSearchState, results, 0);
                    }
                })
                .catch(function (error) {
                    searchState.query = query;
                    searchState.results = [];
                    searchState.error = error.message;
                    updateResults(fullSearchState, results, 0);
                });
        }, search.frequency);
    } else {
        searchState.query = query;
        searchState.results = [];
        searchState.error = null;
        updateResults(fullSearchState, results, 0);
    }
}

function isEmpty(obj) {
    for(var key in obj) {
        if(obj.hasOwnProperty(key))
            return false;
    }
    return true;
}

function createSearchResults(items, error, query, resultsNode) {
    resultsNode.empty();
    var status = document.createElement('p');
    if (items.length === 0) {
        if (query === '') {
            resultsNode.removeClass('active');
        } else if(!isEmpty(error)) {
            status.setAttribute("style", "position: relative; display: inline-block !important");
            status.appendChild(document.createTextNode('Something went wrong, try again later, status - ' + JSON.stringify(error)));
            resultsNode.append(status);
        } else {
            status.setAttribute("style", "position: relative; display: inline-block");
            status.appendChild(document.createTextNode('Your search did not match any documents. Please make sure that all words are spelled correctly and that you have entered at least three characters for full text search.'));
            resultsNode.append(status);
        }
    } else {
        var list = document.createElement('ul');
        list.classList.add("search");
        for (var i = 0; i < items.length; i++) {
            var item = document.createElement('li');
            item.setAttribute("onclick", "window.location.href='"+items[i].link+"'")
            var link = document.createElement('a');
            link.setAttribute("href", items[i].link);
            link.innerHTML = items[i].title;
            var text = document.createElement('div');
            text.classList.add("context");
            text.innerHTML = items[i].snippet;
            item.appendChild(link);
            item.appendChild(text);
            list.appendChild(item);
        }
        resultsNode.append(list);
        var numRes = Math.min(3, items.length);
        status.appendChild(document.createTextNode('Showing top ' + numRes +' of ' + items.length + ' results. '));
        var allLink = document.createElement('a');
        allLink.setAttribute("href", '/search.html?query=' + query);
        allLink.appendChild(document.createTextNode('See all results...'));
        status.append(allLink);
        resultsNode.append(status);
    }
    resultsNode.addClass('active');
}

function updateResults(searchState, resultsNode, n) {
    // If an update is inprogress, try again in 100ms
    if(searchState.updateInProgress) {
        if(n < 20) {
            setTimeout(function() {
                updateResults(searchState, resultsNode, n+1);
            }, 100);
            return;
        } else {
            console.warn("Search update taking too long. Unblocking search.")
        }
    }

    // Activate semaphore
    searchState.updateInProgress = true;

    // Unify items
    var items = [];
    var error = {};
    var uptodate = true;
    for(var i in customSearches) {
        var search = customSearches[i];
        if(searchState.latestQuery === searchState[search.name].query) {
            items = items.concat(searchState[search.name].results);
            if(searchState[search.name].error !== null) error[search.name] = searchState[search.name].error;
        } else {
            uptodate = false;
        }
    }

    // Write the search results
    if(items.length > 0 || uptodate)
        createSearchResults(items, error, searchState.latestQuery, resultsNode);

    // Release semaphore
    searchState.updateInProgress = false;
}

$(document).ready(function () {
    // initialize search state
    var searchState = {
        latestQuery : "",
        updateInProgress : false
    }
    for (var i in customSearches) {
        searchState[customSearches[i].name] = {
            query : "",
            results : [],
            error : null,
            timeout : null
        }
    }

    // menu - expand / collapse captions - custom
    $('.wy-menu-vertical .caption').on('click', function (ev) {
        $(this.nextElementSibling).toggleClass('open');
        $(this).toggleClass('open');
        ev.stopPropagation();
    });
    // menu - expand / collapse captions - open after clicking link
    $('.wy-menu-vertical ul.current').addClass('open');
    $('.wy-menu-vertical ul.current').prev('p.caption').addClass('open');
    // content menu - expand / collapse
    $('.content-menu-expand').on('click', function (ev) {
        $(this).parent().toggleClass('collapsed');
        menu();
        ev.stopPropagation();
    });
    // content menu
    if ($(window).width() >= 1600) $('.content-menu').removeClass('collapsed');
    menu();
    // content menu - active element
    var $sections = $('.section .section'); // there is subheaders in header so section in section
    scrollContentMenu($sections);
    $(window).scroll(function () {
        scrollContentMenu($sections);
        $('.wy-grid-for-nav').addClass('custom-scroll');
        setTimeout(function () {
            $('.wy-grid-for-nav').removeClass('custom-scroll');
        }, 50);
    });
    $('.content-menu-wrapper li a').on('click', function (ev) {
        if ($(window).width() < 1600) {
            $('.content-menu').toggleClass('collapsed');
        }
        var href = $(this).attr('href');
        // Special treatment for going back to the top since those links
        // don't have an id and just go to #
        var position = href === "#" ? 0 : $(href).offset().top;
        $("html, body").animate(
          {scrollTop: position},
          "slow",
          function () {
            window.location.hash = href;
          });
        ev.preventDefault();
    });
    // sections padding
    $('.section h1').each(function (index, item) {
        $(item).parent().addClass('h1-section');
    });
    $('.section h2').each(function (index, item) {
        $(item).parent().addClass('h2-section');
    });
    $('.section h3').each(function (index, item) {
        $(item).parent().addClass('h3-section');
    });
    $('.section h4').each(function (index, item) {
        $(item).parent().addClass('h4-section');
    });
    $('.section h5').each(function (index, item) {
        $(item).parent().addClass('h5-section');
    });
    $('.section h6').each(function (index, item) {
        $(item).parent().addClass('h6-section');
    });
    if (searchEngine === 'custom') {
        var queryParam = new URLSearchParams(window.location.search).get('query');
        if (queryParam !== undefined && queryParam !== null) {
            $('#rtd-search-inline-form [name=q]').val(unescape(queryParam));
            searchState.latestQuery = queryParam;
            var results = $('#search-results');
            for(var i in customSearches)
                performSearch(searchState, customSearches[i], results, false);
        }
        $(".search-inline-img").click(function (event) {
            var query = $('#rtd-search-inline-form [name=q]').val();
            var link = '/search.html?query=' + query;
            window.location.href = link;
        });
        $("#rtd-search-inline-form").keydown(function (event) {
            if (event.keyCode === 38 || event.keyCode === 40) {
                event.preventDefault();
            }
        })
            .keyup(function (event) {
                var searchPage = window.location.pathname === "/search.html";
                var query = escape($('#rtd-search-inline-form [name=q]').val());
                var results = searchPage ? $('#search-results') : $('#search-inline-results');
                if(searchPage && window.history.pushState) {
                    var newurl = window.location.protocol + "//" + window.location.host + window.location.pathname + "?query=" + query;
                    window.history.pushState({path:newurl},'',newurl);
                }
                // escape
                if (event.keyCode === 27) {
                    $('.search-inline-results').removeClass('active');
                } else {
                    // enter or key up or key down
                    if (event.keyCode === 13 || event.keyCode === 38 || event.keyCode === 40) {
                        var activeIndex = -1;
                        var results = $('#search-inline-results ul.search li');
                        results.each(function (index, item) {
                            if ($(item).hasClass('active')) {
                                activeIndex = index;
                                return false;
                            }
                        });
                        // enter
                        if (event.keyCode === 13) {
                            var link = '/search.html?query=' + query;
                            if (activeIndex !== -1) {
                                link = $(results[activeIndex]).find('a').attr('href');
                            }
                            window.location.href = link;
                        }
                        // key up
                        if (event.keyCode === 38) {
                            if (activeIndex === -1) {
                                $(results[2]).addClass('active');
                            } else {
                                if (activeIndex > 0) {
                                    $(results[activeIndex]).removeClass('active');
                                    $(results[activeIndex - 1]).addClass('active');
                                }
                            }
                        }
                        // key down
                        if (event.keyCode === 40) {
                            var activeIndex = -1;
                            results.each(function (index, item) {
                                if ($(item).hasClass('active')) {
                                    activeIndex = index;
                                    return false;
                                }
                            });
                            if (activeIndex === -1) {
                                $(results[0]).addClass('active');
                            } else {
                                if (activeIndex < 2) {
                                    $(results[activeIndex]).removeClass('active');
                                    $(results[activeIndex + 1]).addClass('active');
                                }
                            }
                        }
                    } else {
                        searchState.latestQuery = query;
                        for(var i in customSearches)
                            performSearch(searchState, customSearches[i], results, true);
                    }
                }
            });
    }
    setHighlighterWidth();
});

$(window).resize(function () {
    if ($(window).width() >= 1600) $('.content-menu').removeClass('collapsed');
    menu();
    setHighlighterWidth();
});

if (searchEngine === 'sphinx') {
    $.getScript("/_static/searchtools-da.js", function () {
        $(".search-inline-img").click(function (event) {
            var query = $('#rtd-search-inline-form [name=q]')[0].value;
            if (query.length >= 3) {
                var link = '/search.html?q=' + query + '&check_keywords=yes&area=default';
                window.location.href = link;
                $('.search-inline .search-alert').removeClass('active');
            } else {
                $('.search-inline .search-alert').addClass('active');
            }
        });
        $("#rtd-search-inline-form").keydown(function (event) {
            if (event.keyCode === 38 || event.keyCode === 40) {
                event.preventDefault();
            }
        })
            .keyup(function (event) {
                window.setTimeout(function () {
                    var query = $('#rtd-search-inline-form [name=q]')[0].value;
                    // escape
                    if (event.keyCode === 27) {
                        $('.search-inline-results').removeClass('active');
                    } else {
                        // enter or key up or key down
                        if (event.keyCode === 13 || event.keyCode === 38 || event.keyCode === 40) {
                            var activeIndex = -1;
                            var results = $('#search-inline-results ul.search li');
                            results.each(function (index, item) {
                                if ($(item).hasClass('active')) {
                                    activeIndex = index;
                                    return false;
                                }
                            });
                            // enter
                            if (event.keyCode === 13) {
                                var link = '/search.html?q=' + query + '&check_keywords=yes&area=default';
                                if (activeIndex !== -1) {
                                    link = '/' + $(results[activeIndex]).find('a').attr('href');
                                }
                                window.location.href = link;
                            }
                            // key up
                            if (event.keyCode === 38) {
                                if (activeIndex === -1) {
                                    $(results[2]).addClass('active');
                                } else {
                                    if (activeIndex > 0) {
                                        $(results[activeIndex]).removeClass('active');
                                        $(results[activeIndex - 1]).addClass('active');
                                    }
                                }
                            }
                            // key down
                            if (event.keyCode === 40) {
                                var activeIndex = -1;
                                results.each(function (index, item) {
                                    if ($(item).hasClass('active')) {
                                        activeIndex = index;
                                        return false;
                                    }
                                });
                                if (activeIndex === -1) {
                                    $(results[0]).addClass('active');
                                } else {
                                    if (activeIndex < 2) {
                                        $(results[activeIndex]).removeClass('active');
                                        $(results[activeIndex + 1]).addClass('active');
                                    }
                                }
                            }
                        } else {
                            var resultsWrapper = $('.search-inline-results');
                            if (query.length >= 3) {
                                Search.breakPendingQuery();
                                window.setTimeout(function () {
                                    resultsWrapper.empty();
                                    $('.search-inline .search-alert').removeClass('active');
                                    resultsWrapper.addClass('active');
                                    Search.setInline();
                                    Search.loadIndex("/searchindex.js");
                                    Search.performSearch(query);
                                }, 50);
                            } else {
                                resultsWrapper.removeClass('active');
                                $('.search-inline .search-alert').addClass('active');
                                if (query.length === 0) {
                                    $('.search-inline .search-alert').removeClass('active');
                                }
                            }
                        }
                    }
                }, 50);
            })
    });
}
