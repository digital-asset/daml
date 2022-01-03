// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2018 Digital Asset Holdings, LLC subject to Python Software
// Foundation's rights in Python

(function() {
    'use strict';
  
    // Parses versions in URL segments like:
    // "3", "dev", "release/2.7" or "3.6rc2"
    var version_regexs = [
      '(?:\\d)',
      '(?:\\d\\.\\d[\\w\\d\\.]*)',
      '(?:\\d\\d\\d\\d-\\d\\d-\\d\\d)',
      '(?:release/\\d.\\d[\\x\\d\\.]*)',
      '(?:latest)'];
  
    function build_version_select(all_versions, current_version) {
      var buf = ['<select class="da-version-switcher" style="display:none">'];
  
      $.each(all_versions, function(version, title) {
        buf.push('<option value="' + version + '"');
        if (version == current_version)
          buf.push(' selected="selected">' + title + '</option>');
        else
          buf.push('>' + title + '</option>');
      });
  
      buf.push('</select>');
      return buf.join('');
    }
  
    function navigate_to_first_existing(urls) {
      // Navigate to the first existing URL in urls.
      var url = urls.shift();
      if (urls.length == 0) {
        window.location.href = url;
        return;
      }
      $.ajax({
        url: url,
        success: function() {
          window.location.href = url;
        },
        error: function() {
          navigate_to_first_existing(urls);
        }
      });
    }
  
  function on_version_switch() {
      var selected_version = $(this).children('option:selected').attr('value') + '/';
      var url = window.location.href;
      var root = versions_root_in_url(url)
      var current_version = version_segment_in_url(url);
      var new_url = url.replace(root + current_version,
                                root + selected_version);
      if (new_url != url) {
        navigate_to_first_existing([
          new_url,
          root + selected_version,
          root,
          '/'
        ]);
      }
    }
  
    function get_host() {
      var port = window.location.port
      var host = window.location.protocol + '//' +
        window.location.hostname +
        (port != "" ? ':' + port : "") +
        '/';
        return host;
    }
  
    // Returns the path segment of the version as a string, like '3.6/'
    // or '' if not found.
    function version_segment_in_url(url) {
      var host = get_host().replace(/\//g,'\\/');
      var version_segment = '(?:(?:' + version_regexs.join('|') + ')/)';
      var version_regexp = host + '.*?(' + version_segment + ')';
      var match = url.match(version_regexp);
      if (match !== null)
        return match[1];
      return ''
    }
  
    function nthIndex(str, pat, n){
      var re = new RegExp(pat, "g");
      var m = null;
      for(var i = 0; i < n; i++)
        m = re.exec(str);
      return m ? re.lastIndex : -1;
    }
  
    function count(str, pat){
      var re = new RegExp(pat, "g");
      return (str.match(re) || []).length;
    }

    function versions_root_in_url(url) {
      var version = version_segment_in_url(url);
      if(version == '') {
        var depth_from_root = count(DOCUMENTATION_OPTIONS.URL_ROOT, "../");
        var depth = count(url, "/");
        var dir = url.substring(0, nthIndex(url, '/', depth - depth_from_root));
        return dir;
      } else {
        return url.substring(0, url.indexOf(version));
      }
    }
  
    jQuery(document).ready(function() {
      var version = version_segment_in_url(document.location.href);
      version = version.substring(0, version.length - 1);
      if (version == "")
        version = jQuery('.version_switcher_placeholder').html();
  
      var versions_location = versions_root_in_url(document.location.href) + "versions.json"
      jQuery.getJSON(versions_location, function(all_versions) {
        var version_select = build_version_select(all_versions, version);
  
        jQuery('.version_switcher_placeholder').after(version_select);
        setTimeout(function() {
          jQuery('.version_switcher_placeholder').remove();
          jQuery('.da-version-switcher').css('display', 'inline');
          jQuery('.da-version-switcher').bind('change', on_version_switch);
          jQuery('select').niceSelect();
        });
      });
    });
  })();
  