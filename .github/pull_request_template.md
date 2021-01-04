### Pull Request Checklist

- [ ] Read and understand the [contribution guidelines](https://github.com/digital-asset/daml/blob/main/CONTRIBUTING.md)
- [ ] Include appropriate tests
- [ ] Set a descriptive title and thorough description
- [ ] Add a reference to the [issue this PR will solve](https://github.com/digital-asset/daml/issues), if appropriate
- [ ] Include changelog additions in one or more commit message bodies between the `CHANGELOG_BEGIN` and `CHANGELOG_END` tags
- [ ] Normal production system change, include purpose of change in description

NOTE: CI is not automatically run on non-members pull-requests for security
reasons. The reviewer will have to comment with `/AzurePipelines run` to
trigger the build.
