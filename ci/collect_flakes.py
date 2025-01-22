import os
import sys
import subprocess
import datetime
import json
import urllib.parse
import tempfile
from typing import List

# TODO: set milestone, set ttl

milestone = "M97 Flaky Tests"


def run_cmd(cmd: List[str]):
    """
    Runs a command with capture_ouput=True. Returns the result object if it
    succeeds, otherwise logs stdout and stderror and raises an exception.
    """
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR while executing command:")
        print(f"Command was {cmd}")
        print(f"stderr was {result.stderr}")
        print(f"stdout was {result.stdout}")
        raise Exception("command failed")
    return result


def call_gh(*args):
    """
    Calls the gh command line tool with the given arguments.
    """
    print(f"Calling gh {args}")
    result = run_cmd(["gh"] + list(args))
    return result


def extract_failed_tests(report_filename: str):
    """
    Extracts the names of the failed tests from the given report file.
    """
    result = run_cmd([
        "jq",
        "-r", 'select(.testResult.status == "FAILED") | .id.testResult.label',
        report_filename])
    return result.stdout.splitlines()


def report_failed_test(branch: str, test_name: str):
    """
    Reports a failed test as a github issue. If a github issue already exists
    for that failed test then adds an entry to its body.
    """
    title = f"TEST PLEASE IGNORE [{branch}] Flaky {test_name}"
    result = call_gh(
        "issue",
        "list",
        "--repo", "digital-asset/daml",
        "--author", "githubuser-da",
        "--milestone", milestone,
        "--state", "all",
        "--search", f"in:title {test_name}",
        "--json", "number,title,body,closed")
    print(f"Found issues: {result.stdout.strip()}")
    matches = [
        e
        for e in json.loads(result.stdout)
        if e["title"] == title
    ]
    if matches:
        match = matches[0]
        id, body, closed = str(match["number"]), match["body"], match["closed"]
        if closed:
            gh_reopen_issue(id)
        gh_update_issue(id, body)
    else:
        gh_create_issue(title)


def gh_create_issue(title: str):
    """
    Create a new github flaky test issue for the given test name.
    """
    body = ("This issue was created automatically by the CI. "
            "Please fix the test before closing the issue."
            "\n\n"
            f"{mk_issue_entry()}")
    with tempfile.NamedTemporaryFile(
            delete_on_close=False, mode='w') as temp_file:
        temp_file.write(body)
        temp_file.close()
        result = call_gh(
            "issue",
            "create",
            "--milestone", milestone,
            "--title", title,
            "--body-file", temp_file.name)
    print(f"Created issue {result.stdout.strip()}")


def mk_issue_entry():
    """
    Returns a string of the form "date url:<url>" where <url> is a link to the
    build logs for the current job and task.
    """
    date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    url = "https://dev.azure.com/digitalasset/daml/_build/results?"
    url += urllib.parse.urlencode({
        "buildId": os.environ["BUILD_BUILDID"],
        "view": "logs",
        "j": os.environ["SYSTEM_JOBID"],
        #        "t": os.environ["SYSTEM_TASKINSTANCEID"]
    })
    return f"{date_str} [logs]({url})"


def gh_update_issue(id: str, body: str):
    """
    Updates the body of the given issue with a new entry.
    """
    new_body = f"{body}\n{mk_issue_entry()}"
    with tempfile.NamedTemporaryFile(
            delete_on_close=False, mode='w') as temp_file:
        temp_file.write(new_body)
        temp_file.close()
        call_gh("issue", "edit", id, "--body-file", temp_file.name)
    print(f"Updated issue {id}")


def gh_reopen_issue(id: str):
    """
    Re-opens a github issue given its id.
    """
    call_gh("issue", "reopen", id)
    print(f"Unarchived issue {id}")


if __name__ == "__main__":
    [_, branch, report_filename] = sys.argv
    failing_tests = extract_failed_tests(report_filename)
    print(f"Reporting {len(failing_tests)} failing tests as github issues.")
    for test_name in failing_tests:
        print(f"Reporting {test_name}")
        report_failed_test(branch, test_name)
