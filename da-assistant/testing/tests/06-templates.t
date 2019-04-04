
Project templates are a feature of SDK Assistant that allow adding examples
and starter code to SDK projects.

Lets first pick a known SDK version to test against:

  $ da use 0.10.2
  Installing packages:.* (re)
  Activating packages:.* (re)
  Activating SDK version 0.10.2...
Each SDK release comes with a set of templates which we can query with the
'template list' command:

  $ da template list | grep getting-started
  getting-started \[built-in\] (re)

Listing templates can be used either without or with an active project:

  $ da new template-proj
  Creating a project called template-proj
  in .*\/template-proj\. (re)

  $ cd template-proj
  $ da template list | grep getting-started
  getting-started \[built-in\] (re)

Templates can come with an activation script. To test that lets create
a new builtin template:

  $ export TEST_TEMPLATE=$HOME/.da/packages/sdk/0.10.2/templates/_test/
  $ mkdir -p $TEST_TEMPLATE/_template
  $ touch $TEST_TEMPLATE/file-in-template
  $ printf "#!/usr/bin/env sh\necho test activate" > $TEST_TEMPLATE/_template/activate
  $ chmod +x $TEST_TEMPLATE/_template/activate
  $ da project add _test
  Template '_test' added to project.
  test activate
  $ find . | sort
  .
  ./da.yaml
  ./daml
  ./daml/Main.daml
  ./file-in-template

Project templates can be published:

  $ da new appletree
  Creating a project called appletree
  in .*\/appletree. (re)
  $ cd appletree
  $ mkdir -p a/b/c
  $ echo abc > a/b/c/d.txt
  $ echo root > root.txt
  $ touch da.yaml
  $ echo y | da template publish my-templates/appletree/0.10.2 > /tmp/output-project.txt 2>&1
  $ cat /tmp/output-project.txt | head -n 1
  The following files will be uploaded as part of the template:
  $ cat /tmp/output-project.txt | head -n 9 | tail -n 8 | sort
    .*\/a (re)
    .*\/a\/b (re)
    .*\/a\/b\/c (re)
    .*\/a/b\/c\/d.txt (re)
    .*\/da.yaml (re)
    .*\/daml (re)
    .*\/daml\/Main.daml (re)
    .*\/root.txt (re)
  $ cat /tmp/output-project.txt | head -n 10 | tail -n 1
  Do you approve publishing? [y/N]
  $ cat /tmp/output-project.txt | head -n 11 | tail -n 1
  Template appletree (release line 0.10.2) successfully published.
  $ cd ..
  $ da new my-templates/appletree applt; cd applt; ls | sort
  a
  da.yaml
  daml
  root.txt
  $ ls a/b/c/d.txt
  a/b/c/d.txt
  $ cd ..

Addon templates can be published:

  $ mkdir addonapple; cd addonapple
  $ mkdir -p 1/2/3
  $ echo addon > 1/2/3/addon.txt
  $ echo y | da template publish my-templates/appletree-addon/0.10.2 > /tmp/output.txt 2>&1
  $ cat /tmp/output.txt | head -n 1
  The following files will be uploaded as part of the template:
  $ cat /tmp/output.txt | head -n 5 | tail -n 4 | sort
    .*\/1 (re)
    .*\/1\/2 (re)
    .*\/1\/2\/3 (re)
    .*\/1/2\/3\/addon.txt (re)
  $ cat /tmp/output.txt | head -n 7 | tail -n 2
  Do you approve publishing? [y/N]
  Template appletree-addon (release line 0.10.2) successfully published.
  $ da new example01
  Creating a project called example01
  in .*\/example01. (re)
  $ da add my-templates/appletree-addon bbb
  $ ls bbb/1/2/3/addon.txt
  bbb/1/2/3/addon.txt