import datetime
import http.client
import json
import os
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request

from typing import List

milestone = "M97 Flaky Tests"


def call_gh(*args):
    """
    Calls the gh command line tool with the given arguments. Throws if gh
    retruns a non-zero exit code.
    """
    cmd = ["gh"] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR while executing command:")
        print(f"Command was {cmd}")
        print(f"stderr was {result.stderr}")
        print(f"stdout was {result.stdout}")
        raise Exception("command failed")
    return result


def extract_failed_tests(report_filename: str):
    """
    Extracts the names of the failed tests from the given report file.
    """
    with open(report_filename) as f:
        for line in f:
            entry = json.loads(line)
            if "testResult" in entry and entry["testResult"]["status"] == "FAILED":
                yield entry["id"]["testResult"]["label"]

def extract_test_name_map():
    """
    Extracts the short and long test names from a file. The file is a JSON object
    with keys being the short names and values being the long names.
    """
    with open("test_name_map.json") as f:
        return json.load(f)

def print_failed_test(branck: str, test_name: str):
    print(f"Flaky test detected: {test_name}")

def report_failed_test(branch: str, test_name: str):
    """
    Reports a failed test as a github issue. If a github issue already exists
    for that failed test then adds an entry to its body.
    """
    title = f"[{branch}] Flaky {test_name}"
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
    date = datetime.datetime.now(datetime.timezone.utc)
    date_str = date.strftime("%Y-%m-%d %H:%M:%S")
    url = "https://dev.azure.com/digitalasset/daml/_build/results?"
    url += urllib.parse.urlencode({
        "buildId": os.environ["BUILD_BUILDID"],
        "view": "logs",
        "j": os.environ["SYSTEM_JOBID"],
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
        "/_apis/build/retention/leases?api-version=7.1"
    ])
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
    req = urllib.request.Request(
        url, data=json.dumps(data).encode(), headers=headers)
    urllib.request.urlopen(req)


if __name__ == "__main__":
    [_, access_token, branch, report_filename] = sys.argv
    failing_tests = list(extract_failed_tests(report_filename))
    print(f"Reporting {len(failing_tests)} failing tests as github issues.")
    test_name_map = extract_test_name_map
    print(f"^^^^ {test_name_map}")
    for test_name in failing_tests:
        print(f"Reporting {test_name}")
        print(f"=== {test_name_map[test_name]} )
        #report_failed_test(branch, test_name)
        print_failed_test(branch, test_name)
    if failing_tests:
        print('Increasing logs retention to 2 years')
        az_set_logs_ttl(access_token, 365 * 2)
