"""
sphinx-mermaid
~~~~~~~~~~~~~~~

Allow mermaid diagrams to be included in Sphinx-generated
documents inline.

:copyright: Copyright 2016-2023 by Martín Gaitán and others
:license: BSD, see LICENSE for details.
"""

from __future__ import annotations

import codecs
import errno
import os
import posixpath
import re
import shlex
import uuid
from hashlib import sha1
from json import loads
from subprocess import PIPE, Popen
from tempfile import TemporaryDirectory

import sphinx
from docutils import nodes
from docutils.parsers.rst import Directive, directives
from docutils.statemachine import ViewList
from packaging.version import Version
from sphinx.application import Sphinx
from sphinx.locale import _
from sphinx.util import logging
from sphinx.util.i18n import search_image_for_language
from sphinx.util.osutil import ensuredir
from yaml import dump

from .autoclassdiag import class_diagram
from .exceptions import MermaidError

logger = logging.getLogger(__name__)

mapname_re = re.compile(r'<map id="(.*?)"')
_MERMAID_INIT_JS_DEFAULT = "mermaid.initialize({startOnLoad:false});"
_MERMAID_RUN_NO_D3_ZOOM = """
import mermaid from "{mermaid_js_url}";
window.addEventListener("load", () => mermaid.run());
"""

_MERMAID_RUN_D3_ZOOM = """
import mermaid from "{mermaid_js_url}";
const load = async () => {{
    await mermaid.run();
    const all_mermaids = document.querySelectorAll(".mermaid");
    const mermaids_to_add_zoom = {d3_node_count} === -1 ? all_mermaids.length : {d3_node_count};
    const mermaids_processed = document.querySelectorAll(".mermaid[data-processed='true']");
    if(mermaids_to_add_zoom > 0) {{
        var svgs = d3.selectAll("{d3_selector}");
        if(all_mermaids.length !== mermaids_processed.length) {{
            // try again in a sec, wait for mermaids to load
            setTimeout(load, 200);
            return;
        }} else if(svgs.size() !== mermaids_to_add_zoom) {{
            // try again in a sec, wait for mermaids to load
            setTimeout(load, 200);
            return;
        }} else {{
            svgs.each(function() {{
                var svg = d3.select(this);
                svg.html("<g class='wrapper'>" + svg.html() + "</g>");
                var inner = svg.select("g");
                var zoom = d3.zoom().on("zoom", function(event) {{
                    inner.attr("transform", event.transform);
                }});
                svg.call(zoom);
            }});
        }}
    }}
}};

window.addEventListener("load", load);
"""


class mermaid(nodes.General, nodes.Inline, nodes.Element):
    pass


def figure_wrapper(directive, node, caption):
    figure_node = nodes.figure("", node)
    if "align" in node:
        figure_node["align"] = node.attributes.pop("align")

    parsed = nodes.Element()
    directive.state.nested_parse(ViewList([caption], source=""), directive.content_offset, parsed)
    caption_node = nodes.caption(parsed[0].rawsource, "", *parsed[0].children)
    caption_node.source = parsed[0].source
    caption_node.line = parsed[0].line
    figure_node += caption_node
    return figure_node


def align_spec(argument):
    return directives.choice(argument, ("left", "center", "right"))


class Mermaid(Directive):
    """
    Directive to insert arbitrary Mermaid markup.
    """

    has_content = True
    required_arguments = 0
    optional_arguments = 1
    final_argument_whitespace = False
    option_spec = {
        # Sphinx directives
        "name": directives.unchanged,
        "alt": directives.unchanged,
        "align": align_spec,
        "caption": directives.unchanged,
        "zoom": directives.unchanged,
        # Mermaid directives
        "config": directives.unchanged,
        "title": directives.unchanged,
    }

    def get_mm_code(self):
        if self.arguments:
            # try to load mermaid code from an external file
            document = self.state.document
            if self.content:
                return [
                    document.reporter.warning(
                        "Mermaid directive cannot have both content and " "a filename argument",
                        line=self.lineno,
                    )
                ]
            env = self.state.document.settings.env
            argument = search_image_for_language(self.arguments[0], env)
            rel_filename, filename = env.relfn2path(argument)
            env.note_dependency(rel_filename)
            try:
                with codecs.open(filename, "r", "utf-8") as fp:
                    mmcode = fp.read()
            except OSError:
                return [
                    document.reporter.warning(
                        "External Mermaid file %r not found or reading " "it failed" % filename,
                        line=self.lineno,
                    )
                ]
        else:
            # inline mermaid code
            mmcode = "\n".join(self.content)
        return mmcode

    def run(self, **kwargs):
        mmcode = self.get_mm_code()
        # mmcode is a list, so it's a system message, not content to be included in the
        # document.
        if not isinstance(mmcode, str):
            return mmcode

        # if mmcode is empty, ignore the directive.
        if not mmcode.strip():
            return [
                self.state_machine.reporter.warning(
                    'Ignoring "mermaid" directive without content.',
                    line=self.lineno,
                )
            ]

        # Wrap the mermaid code into a code node.
        node = mermaid()
        node["code"] = mmcode
        node["options"] = {}
        # Sphinx directives
        if "alt" in self.options:
            node["alt"] = self.options["alt"]
        if "align" in self.options:
            node["align"] = self.options["align"]
        if "inline" in self.options:
            node["inline"] = True
        if "zoom" in self.options:
            node["zoom"] = True
            node["zoom_id"] = f"id-{uuid.uuid4()}"

        # Mermaid directives
        mm_config = "---"
        if "config" in self.options:
            mm_config += "\n"
            mm_config += dump({"config": loads(self.options["config"])})
        if "title" in self.options:
            mm_config += "\n"
            mm_config += f"title: {self.options['title']}"
        mm_config += "\n---\n"
        if mm_config != "---\n---\n":
            node["code"] = mm_config + node["code"]

        caption = self.options.get("caption")
        if caption is not None:
            node = figure_wrapper(self, node, caption)

        self.add_name(node)
        return [node]


class MermaidClassDiagram(Mermaid):
    has_content = False
    required_arguments = 1
    optional_arguments = 100
    option_spec = Mermaid.option_spec.copy()
    option_spec.update(
        {
            "full": directives.flag,
            "namespace": directives.unchanged,
            "strict": directives.flag,
        }
    )

    def get_mm_code(self):
        return class_diagram(
            *self.arguments,
            full="full" in self.options,
            strict="strict" in self.options,
            namespace=self.options.get("namespace"),
        )


def render_mm(self, code, options, _fmt, prefix="mermaid"):
    """Render mermaid code into a PNG or PDF output file."""

    if _fmt == "raw":
        _fmt = "png"

    mermaid_cmd = self.builder.config.mermaid_cmd
    mermaid_cmd_shell = self.builder.config.mermaid_cmd_shell in {True, "True", "true"}
    hashkey = (code + str(options) + str(self.builder.config.mermaid_sequence_config)).encode("utf-8")

    basename = f"{prefix}-{sha1(hashkey).hexdigest()}"
    fname = f"{basename}.{_fmt}"
    relfn = posixpath.join(self.builder.imgpath, fname)
    outdir = os.path.join(self.builder.outdir, self.builder.imagedir)
    outfn = os.path.join(outdir, fname)
    with TemporaryDirectory() as tempDir:
        tmpfn = os.path.join(tempDir, basename)

        if os.path.isfile(outfn):
            return relfn, outfn

        ensuredir(os.path.dirname(outfn))

        with open(tmpfn, "w", encoding="utf-8") as t:
            t.write(code)

        if isinstance(mermaid_cmd, str):
            mm_args = shlex.split(mermaid_cmd)
        else:
            mm_args = list(mermaid_cmd)

        mm_args.extend(self.builder.config.mermaid_params)
        mm_args += ["-i", tmpfn, "-o", outfn]
        if self.builder.config.mermaid_sequence_config:
            mm_args.extend(["--configFile", self.builder.config.mermaid_sequence_config])

        try:
            p = Popen(mm_args, shell=mermaid_cmd_shell, stdout=PIPE, stdin=PIPE, stderr=PIPE)
        except FileNotFoundError:
            logger.warning("command %r cannot be run (needed for mermaid " "output), check the mermaid_cmd setting" % mermaid_cmd)
            return None, None

        stdout, stderr = p.communicate(str.encode(code))
        if self.builder.config.mermaid_verbose:
            logger.info(stdout)

        if p.returncode != 0:
            raise MermaidError("Mermaid exited with error:\n[stderr]\n%s\n" "[stdout]\n%s" % (stderr, stdout))
        if not os.path.isfile(outfn):
            raise MermaidError("Mermaid did not produce an output file:\n[stderr]\n%s\n" "[stdout]\n%s" % (stderr, stdout))
        return relfn, outfn


def _render_mm_html_raw(self, node, code, options, prefix="mermaid", imgcls=None, alt=None):
    classes = ["mermaid"]
    attrs = {}

    if "align" in node:
        classes.append(f"align-{node['align']}")
        attrs["align"] = node["align"]

    if "zoom_id" in node:
        attrs["data-zoom-id"] = node["zoom_id"]

    if "ids" in node and len(node["ids"]) == 1:
        attrs["id"] = node["ids"][0]

    tag_template = """<pre {attr_defs} class="{classes}">
        {code}
    </pre>"""
    attr_defs = ['{}="{}"'.format(k, v) for k, v in attrs.items()]
    self.body.append(tag_template.format(attr_defs=" ".join(attr_defs), classes=" ".join(classes), code=self.encode(code)))
    raise nodes.SkipNode


def render_mm_html(self, node, code, options, prefix="mermaid", imgcls=None, alt=None):
    _fmt = self.builder.config.mermaid_output_format
    if _fmt == "raw":
        return _render_mm_html_raw(self, node, code, options, prefix="mermaid", imgcls=None, alt=None)

    try:
        if _fmt not in ("png", "svg"):
            raise MermaidError("mermaid_output_format must be one of 'raw', 'png', " "'svg', but is %r" % _fmt)

        fname, outfn = render_mm(self, code, options, _fmt, prefix)
    except MermaidError as exc:
        logger.warning(f"mermaid code {code!r}: " + str(exc))
        raise nodes.SkipNode

    if fname is None:
        self.body.append(self.encode(code))
    else:
        if alt is None:
            alt = node.get("alt", self.encode(code).strip())
        imgcss = imgcls and f'class="{imgcls}"' or ""
        if _fmt == "svg":
            svgtag = f"""<object data="{fname}" type="image/svg+xml">
            <p class="warning">{alt}</p></object>
"""
            self.body.append(svgtag)
        else:
            if "align" in node:
                self.body.append('<pre align="%s" class="align-%s">' % (node["align"], node["align"]))

            self.body.append(f'<img src="{fname}" alt="{alt}" {imgcss}/>\n')
            if "align" in node:
                self.body.append("</pre>\n")

    raise nodes.SkipNode


def html_visit_mermaid(self, node):
    render_mm_html(self, node, node["code"], node["options"])


def render_mm_latex(self, node, code, options, prefix="mermaid"):
    try:
        fname, outfn = render_mm(self, code, options, "pdf", prefix)
    except MermaidError as exc:
        logger.warning(f"mm code {code!r}: " + str(exc))
        raise nodes.SkipNode

    if self.builder.config.mermaid_pdfcrop != "":
        mm_args = [self.builder.config.mermaid_pdfcrop, outfn]
        try:
            p = Popen(mm_args, stdout=PIPE, stdin=PIPE, stderr=PIPE)
        except OSError as err:
            if err.errno != errno.ENOENT:  # No such file or directory
                raise
            logger.warning(f"command {self.builder.config.mermaid_pdfcrop!r} cannot be run (needed to crop pdf), check the mermaid_cmd setting")
            return None, None

        stdout, stderr = p.communicate()
        if self.builder.config.mermaid_verbose:
            logger.info(stdout)

        if p.returncode != 0:
            raise MermaidError("PdfCrop exited with error:\n[stderr]\n%s\n" "[stdout]\n%s" % (stderr, stdout))
        if not os.path.isfile(outfn):
            raise MermaidError("PdfCrop did not produce an output file:\n[stderr]\n%s\n" "[stdout]\n%s" % (stderr, stdout))

        fname = "{filename[0]}-crop{filename[1]}".format(filename=os.path.splitext(fname))

    is_inline = self.is_inline(node)
    if is_inline:
        para_separator = ""
    else:
        para_separator = "\n"

    if fname is not None:
        post = None
        if not is_inline and "align" in node:
            if node["align"] == "left":
                self.body.append("{")
                post = "\\hspace*{\\fill}}"
            elif node["align"] == "right":
                self.body.append("{\\hspace*{\\fill}")
                post = "}"
        self.body.append("%s\\sphinxincludegraphics{%s}%s" % (para_separator, fname, para_separator))
        if post:
            self.body.append(post)

    raise nodes.SkipNode


def latex_visit_mermaid(self, node):
    render_mm_latex(self, node, node["code"], node["options"])


def render_mm_texinfo(self, node, code, options, prefix="mermaid"):
    try:
        fname, outfn = render_mm(self, code, options, "png", prefix)
    except MermaidError as exc:
        logger.warning(f"mm code {code!r}: " + str(exc))
        raise nodes.SkipNode
    if fname is not None:
        self.body.append("@image{%s,,,[mermaid],png}\n" % fname[:-4])
    raise nodes.SkipNode


def texinfo_visit_mermaid(self, node):
    render_mm_texinfo(self, node, node["code"], node["options"])


def text_visit_mermaid(self, node):
    if "alt" in node.attributes:
        self.add_text(_("[graph: %s]") % node["alt"])
    else:
        self.add_text(_("[graph]"))
    raise nodes.SkipNode


def man_visit_mermaid(self, node):
    if "alt" in node.attributes:
        self.body.append(_("[graph: %s]") % node["alt"])
    else:
        self.body.append(_("[graph]"))
    raise nodes.SkipNode


def install_js(
    app: Sphinx,
    pagename,
    templatename: str,
    context: dict,
    doctree: nodes.document | None,
) -> None:
    # Skip for pages without Mermaid diagrams
    if doctree and not doctree.next_node(mermaid):
        return

    # Add required JavaScript
    if app.config.mermaid_use_local:
        _mermaid_js_url = app.config.mermaid_use_local
    elif app.config.mermaid_version == "latest":
        _mermaid_js_url = "https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.esm.min.mjs"
    elif Version(app.config.mermaid_version) > Version("10.2.0"):
        _mermaid_js_url = f"https://cdn.jsdelivr.net/npm/mermaid@{app.config.mermaid_version}/dist/mermaid.esm.min.mjs"
    elif app.config.mermaid_version:
        raise MermaidError("Requires mermaid js version 10.3.0 or later")

    app.add_js_file(_mermaid_js_url, priority=app.config.mermaid_js_priority, type="module")

    if app.config.mermaid_elk_use_local:
        _mermaid_elk_js_url = app.config.mermaid_elk_use_local
    elif app.config.mermaid_include_elk == "latest":
        _mermaid_elk_js_url = "https://cdn.jsdelivr.net/npm/@mermaid-js/layout-elk/dist/mermaid-layout-elk.esm.min.mjs"
    elif app.config.mermaid_include_elk:
        _mermaid_elk_js_url = (
            f"https://cdn.jsdelivr.net/npm/@mermaid-js/layout-elk@{app.config.mermaid_include_elk}/dist/mermaid-layout-elk.esm.min.mjs"
        )
    else:
        _mermaid_elk_js_url = None
    if _mermaid_elk_js_url:
        app.add_js_file(_mermaid_elk_js_url, priority=app.config.mermaid_js_priority, type="module")

    if app.config.mermaid_init_js == _MERMAID_INIT_JS_DEFAULT:
        # Update if esm is used and no custom init-js is provided
        if _mermaid_elk_js_url:
            # Add registration of ELK layouts
            app.config.mermaid_init_js = f'import mermaid from "{_mermaid_js_url}";import elkLayouts from "{_mermaid_elk_js_url}";mermaid.registerLayoutLoaders(elkLayouts);{app.config.mermaid_init_js}'
        else:
            app.config.mermaid_init_js = f'import mermaid from "{_mermaid_js_url}";{app.config.mermaid_init_js}'

    if app.config.mermaid_init_js:
        # If mermaid is local the init-call must be placed after `html_js_files` which has a priority of 800.
        priority = app.config.mermaid_init_js_priority if _mermaid_js_url is not None else 801
        app.add_js_file(None, body=app.config.mermaid_init_js, priority=priority, type="module")

    _wrote_mermaid_run = False
    if app.config.mermaid_output_format == "raw":
        if app.config.d3_use_local:
            _d3_js_url = app.config.d3_use_local
        elif app.config.d3_version == "latest":
            _d3_js_url = "https://cdn.jsdelivr.net/npm/d3/dist/d3.min.js"
        elif app.config.d3_version:
            _d3_js_url = f"https://cdn.jsdelivr.net/npm/d3@{app.config.d3_version}/dist/d3.min.js"
        app.add_js_file(_d3_js_url, priority=app.config.mermaid_js_priority)

        if app.config.mermaid_d3_zoom:
            _d3_js_script = _MERMAID_RUN_D3_ZOOM.format(d3_selector=".mermaid svg", d3_node_count=-1, mermaid_js_url=_mermaid_js_url)
            app.add_js_file(None, body=_d3_js_script, priority=app.config.mermaid_js_priority, type="module")
            _wrote_mermaid_run = True
        elif doctree:
            mermaid_nodes = doctree.findall(mermaid)
            _d3_selector = ""
            count = 0
            for mermaid_node in mermaid_nodes:
                if "zoom_id" in mermaid_node:
                    _zoom_id = mermaid_node["zoom_id"]
                    if _d3_selector == "":
                        _d3_selector += f".mermaid[data-zoom-id={_zoom_id}] svg"
                    else:
                        _d3_selector += f", .mermaid[data-zoom-id={_zoom_id}] svg"
                    count += 1
            if _d3_selector != "":
                _d3_js_script = _MERMAID_RUN_D3_ZOOM.format(d3_selector=_d3_selector, d3_node_count=count, mermaid_js_url=_mermaid_js_url)
                app.add_js_file(None, body=_d3_js_script, priority=app.config.mermaid_js_priority, type="module")
                _wrote_mermaid_run = True

    if not _wrote_mermaid_run and _mermaid_js_url:
        app.add_js_file(
            None, body=_MERMAID_RUN_NO_D3_ZOOM.format(mermaid_js_url=_mermaid_js_url), priority=app.config.mermaid_js_priority, type="module"
        )


def setup(app):
    app.add_node(
        mermaid,
        html=(html_visit_mermaid, None),
        latex=(latex_visit_mermaid, None),
        texinfo=(texinfo_visit_mermaid, None),
        text=(text_visit_mermaid, None),
        man=(man_visit_mermaid, None),
    )
    app.add_directive("mermaid", Mermaid)
    app.add_directive("autoclasstree", MermaidClassDiagram)

    app.add_config_value("mermaid_cmd", "mmdc", "html")
    app.add_config_value("mermaid_cmd_shell", "False", "html")
    app.add_config_value("mermaid_pdfcrop", "", "html")
    app.add_config_value("mermaid_output_format", "raw", "html")
    app.add_config_value("mermaid_params", list(), "html")
    app.add_config_value("mermaid_verbose", False, "html")
    app.add_config_value("mermaid_sequence_config", False, "html")

    app.add_config_value("mermaid_use_local", "", "html")
    app.add_config_value("mermaid_version", "11.2.0", "html")
    app.add_config_value("mermaid_elk_use_local", "", "html")
    app.add_config_value("mermaid_include_elk", "0.1.4", "html")
    app.add_config_value("mermaid_js_priority", 500, "html")
    app.add_config_value("mermaid_init_js_priority", 500, "html")
    app.add_config_value("mermaid_init_js", _MERMAID_INIT_JS_DEFAULT, "html")
    app.add_config_value("d3_use_local", "", "html")
    app.add_config_value("d3_version", "7.9.0", "html")
    app.add_config_value("mermaid_d3_zoom", False, "html")
    app.connect("html-page-context", install_js)

    return {"version": sphinx.__display_version__, "parallel_read_safe": True}
