import datetime
import http.client
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request

from typing import List

milestone = "M97 Flaky Tests"
parent_issue = "23477"
project_owner = "digital-asset"
project_number = "5"


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
    Extracts the names and statuses of the failed and timed out tests from the
    given report file.
    """
    with open(report_filename) as f:
        for line in f:
            entry = json.loads(line)
            if "testResult" in entry:
                status = entry["testResult"]["status"]
                if status in ("FAILED", "TIMEOUT"):
                    yield entry["id"]["testResult"]["label"], status


def report_failed_test(branch: str, test_name: str, status: str, note: str = ""):
    """
    Reports a failed test as a github issue. If a github issue already exists
    for that failed test then adds an entry to its body. Timeouts are reported
    in separate issues, marked with [TIMEOUT]. The note, if any, is appended to
    the entry.
    """
    if status == "TIMEOUT":
        title = f"[{branch}] [TIMEOUT] Flaky {test_name}"
    else:
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
        gh_update_issue(id, body, note)
    else:
        gh_create_issue(title, note)


def gh_create_issue(title: str, note: str):
    """
    Create a new github flaky test issue for the given test name.
    """
    body = ("This issue was created automatically by the CI. "
            "Please fix the test before closing the issue."
            "\n\n"
            f"{mk_issue_entry(note)}")
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
        temp_file.write(body)
        temp_file.close()
        result = call_gh(
            "issue",
            "create",
            "--milestone", milestone,
            "--title", title,
            "--body-file", temp_file.name)
    url = result.stdout.strip()
    print(f"Created issue {url}")
    gh_add_sub_issue(url.rsplit("/", 1)[-1])
    call_gh("project", "item-add", project_number,
            "--owner", project_owner, "--url", url)
    print(f"Added issue {url} to project {project_owner}/{project_number}")


def gh_add_sub_issue(number: str):
    """
    Makes the given issue a sub-issue of the parent flaky tests issue.
    """
    # The sub-issues API takes the issue's internal id, not its number.
    id = call_gh("api", f"repos/digital-asset/daml/issues/{number}",
                 "--jq", ".id").stdout.strip()
    call_gh("api", "--method", "POST",
            f"repos/digital-asset/daml/issues/{parent_issue}/sub_issues",
            "-F", f"sub_issue_id={id}")
    print(f"Made issue {number} a sub-issue of {parent_issue}")


def mk_issue_entry(note: str = ""):
    """
    Returns a string of the form "date [logs](<url>) note" where <url> is a
    link to the build logs for the current job and task.
    """
    date = datetime.datetime.now(datetime.timezone.utc)
    date_str = date.strftime("%Y-%m-%d %H:%M:%S")
    url = "https://dev.azure.com/digitalasset/daml/_build/results?"
    url += urllib.parse.urlencode({
        "buildId": os.environ["BUILD_BUILDID"],
        "view": "logs",
        "j": os.environ["SYSTEM_JOBID"],
    })
    return f"{date_str} [logs]({url}) {note}".rstrip()


def gh_update_issue(id: str, body: str, note: str):
    """
    Updates the body of the given issue with a new entry.
    """
    new_body = f"{body}\n{mk_issue_entry(note)}"
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


def az_get(access_token: str, url: str):
    """
    GETs the given Azure DevOps REST API url and returns the response body.
    """
    req = urllib.request.Request(
        url, headers={'Authorization': f"Bearer {access_token}"})
    with urllib.request.urlopen(req) as response:
        return response.read()


def earlier_attempt_failures(access_token: str):
    """
    Returns {test label: "FAILED" or "TIMEOUT"} for the tests that failed in
    earlier attempts of the current job, read from the summary bazel prints
    at the end of the "Build" step, e.g.

        //docs:daml-intro-test                          TIMEOUT in 355.0s

    Retried jobs run in the same build, on the same merge commit, so the PR
    head and the target branch are the same as in those earlier attempts.
    """
    builds = "".join([
        os.environ['SYSTEM_COLLECTIONURI'],
        os.environ['SYSTEM_TEAMPROJECT'],
        "/_apis/build/builds/",
        os.environ['BUILD_BUILDID'],
    ])
    job_name = os.environ['SYSTEM_JOBDISPLAYNAME']
    timeline = json.loads(az_get(access_token, f"{builds}/timeline?api-version=7.1"))
    previous = [
        attempt["timelineId"]
        for record in timeline["records"]
        if record["type"] == "Job" and record["name"] == job_name
        for attempt in record.get("previousAttempts") or []
    ]
    failures = {}
    for timeline_id in previous:
        old = json.loads(az_get(access_token, f"{builds}/timeline/{timeline_id}?api-version=7.1"))
        jobs = {
            r["id"]
            for r in old["records"]
            if r["type"] == "Job" and r["name"] == job_name
        }
        for record in old["records"]:
            if (record["type"] == "Task" and record["parentId"] in jobs
                    and record["name"] == "Build" and record.get("log")):
                log = az_get(access_token, record["log"]["url"]).decode("utf-8", "replace")
                for label, status in re.findall(
                        r"^\S+ (//\S+)\s+(FAILED|TIMEOUT) in ", log, re.MULTILINE):
                    failures[label] = status
    return failures


def extract_passed_tests(report_filename: str):
    """
    Returns the labels of the tests in the given report file that passed after
    actually running, i.e. not served from the local or remote cache.
    """
    passed = set()
    with open(report_filename) as f:
        for line in f:
            entry = json.loads(line)
            result = entry.get("testResult")
            if (result and result["status"] == "PASSED"
                    and not result.get("cachedLocally")
                    and not result.get("executionInfo", {}).get("cachedRemotely")):
                passed.add(entry["id"]["testResult"]["label"])
    return passed


def report_pr_flakes(access_token: str, target_branch: str, report_filename: str):
    """
    On a retried PR job, reports the tests that failed or timed out in an
    earlier attempt and passed in this one. They go to the same issues as
    flakes on the target branch, with a note saying which PR they came from.
    """
    attempt = os.environ['SYSTEM_JOBATTEMPT']
    failures = earlier_attempt_failures(access_token)
    passed = extract_passed_tests(report_filename)
    flaky = sorted((label, status) for label, status in failures.items() if label in passed)
    print(f"{len(failures)} tests failed in earlier attempts, {len(flaky)} of them passed in attempt {attempt}.")
    for test_name, status in flaky:
        note = (f"(PR #{os.environ['SYSTEM_PULLREQUEST_PULLREQUESTNUMBER']}: "
                f"{status} in an earlier attempt, passed in attempt {attempt})")
        print(f"Reporting {test_name} ({status})")
        report_failed_test(target_branch, test_name, status, note)
    if flaky:
        print('Increasing logs retention to 2 years')
        az_set_logs_ttl(access_token, 365 * 2)


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--pr":
        [_, _, access_token, target_branch, report_filename] = sys.argv
        report_pr_flakes(access_token, target_branch, report_filename)
        sys.exit(0)
    [_, access_token, branch, report_filename] = sys.argv
    failing_tests = list(extract_failed_tests(report_filename))
    print(f"Reporting {len(failing_tests)} failing tests as github issues.")
    for test_name, status in failing_tests:
        print(f"Reporting {test_name} ({status})")
        report_failed_test(branch, test_name, status)
    if failing_tests:
        print('Increasing logs retention to 2 years')
        az_set_logs_ttl(access_token, 365 * 2)
