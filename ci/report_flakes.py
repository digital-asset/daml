import datetime
import http.client
import json
import os
import subprocess
import sys
import tempfile
import urllib.parse

from typing import List

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
    return run_cmd(["gh"] + list(args))


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
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
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
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
        temp_file.write(new_body)
        temp_file.close()
        call_gh("issue", "edit", id, "--body-file", temp_file.name)
    print(f"Added a line to issue {id}")


def gh_reopen_issue(id: str):
    """
    Re-opens a github issue given its id.
    """
    call_gh("issue", "reopen", id)
    print(f"Re-opened issue {id}")


# This would be more consise with the requests library, but it fails to install
# via nix on macOS.
def az_set_logs_ttl(access_token: str, days: int):
    """
    Creates a lease for the logs of the current build ensuring that they won't
    be deleted for the given number of days.
    """
    url = "".join([
        os.environ['SYSTEM_COLLECTIONURI'],
        os.environ['SYSTEM_TEAMPROJECT'],
        "/_apis/build/retention/leases"
    ])
    parsed_url = urllib.parse.urlparse(url)
    host = parsed_url.netloc
    path = parsed_url.path + "?api-version=7.1"
    headers = {
        'Authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    data = [
        {
            "daysValid": days,
            "definitionId": os.environ['SYSTEM_DEFINITIONID'],
            "ownerId": f"User:{os.environ['BUILD_REQUESTEDFORID']}",
            "protectPipeline": False,
            "runId": os.environ['BUILD_BUILDID']
        }
    ]
    conn = http.client.HTTPSConnection(host)
    conn.request("POST", path, body=json.dumps(data), headers=headers)
    response = conn.getresponse()
    if response.status != 200:
        raise Exception(
            f"Request failed with status {response.status}: {response.reason}")
    conn.close()


if __name__ == "__main__":
    [_, access_token, branch, report_filename] = sys.argv
    failing_tests = extract_failed_tests(report_filename)
    print(f"Reporting {len(failing_tests)} failing tests as github issues.")
    for test_name in failing_tests:
        print(f"Reporting {test_name}")
        report_failed_test(branch, test_name)
    if failing_tests:
        print('Increasing logs retention to 2 years')
        az_set_logs_ttl(access_token, 11)
