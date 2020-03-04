DirectoryInfo = provider(fields = ["directory"])

def _create_directory_impl(ctx):
    output = ctx.actions.declare_directory(ctx.attr.name)
    ctx.actions.run_shell(
        outputs = [output],
        command = """\
for file in {files}; do
    echo "$file content" > {output}/$file
done
""".format(
            output = output.path,
            files = " ".join(ctx.attr.files),
        ),
    )
    return [DirectoryInfo(directory = output)]

create_directory = rule(
    _create_directory_impl,
    attrs = {
        "files": attr.string_list(),
    },
)

def _list_directory_impl(ctx):
    output = ctx.actions.declare_file(ctx.attr.name)
    ctx.actions.run_shell(
        inputs = [ctx.attr.directory[DirectoryInfo].directory],
        outputs = [output],
        command = """\
for file in {directory}/*; do
    cat $file >> {output}
done
""".format(
            directory = ctx.attr.directory[DirectoryInfo].directory.path,
            output = output.path,
        ),
    )
    return [DefaultInfo(files = depset([output]))]

list_directory = rule(
    _list_directory_impl,
    attrs = {
        "directory": attr.label(),
    },
)
