import logging
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, TextIO, Tuple, cast

import click
import koji
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.format_util import green_print
from bugzilla import Bugzilla
from bugzilla.bug import Bug
from jira import JIRA, Issue
from tenacity import retry, stop_after_attempt

from elliottlib import Runtime, constants, early_kernel
from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.config_model import KernelBugSweepConfig
from elliottlib.exceptions import ElliottFatalError

LOGGER = logging.getLogger(__name__)


@retry(reraise=True, stop=stop_after_attempt(3))
def _search_issues(jira_client, *args, **kwargs):
    return jira_client.search_issues(*args, **kwargs)


class FindBugsKernelCli:
    def __init__(
        self,
        runtime: Runtime,
        trackers: Sequence[str],
        clone: bool,
        reconcile: bool,
        update_tracker: bool,
        dry_run: bool,
    ):
        self._runtime = runtime
        self._logger = LOGGER
        self.trackers = list(trackers)
        self.clone = clone
        self.reconcile = reconcile
        self.update_tracker = update_tracker
        self.dry_run = dry_run
        self._id_bugs: Dict[int, Bug] = {}  # cache for kernel bug; key is bug_id, value is Bug object
        self._tracker_map: Dict[int, Issue] = {}  # bug_id -> KMAINT jira mapping

    async def run(self):
        logger = self._logger
        if self.reconcile and not self.clone:
            raise ElliottFatalError("--reconcile must be used with --clone")
        if self._runtime.assembly_type is not AssemblyTypes.STREAM:
            raise ElliottFatalError("This command only supports stream assembly")
        group_config = self._runtime.group_config
        raw_config = self._runtime.gitdata.load_data(key='bug', replace_vars=group_config.vars).data.get(
            "kernel_bug_sweep"
        )
        if not raw_config:
            logger.warning("kernel_bug_sweep is not defined in bug.yml")
            return
        config = KernelBugSweepConfig.model_validate(raw_config)
        jira_tracker = self._runtime.get_bug_tracker("jira")
        jira_client: JIRA = jira_tracker._client
        bz_tracker = self._runtime.get_bug_tracker("bugzilla")
        bz_client: Bugzilla = bz_tracker._client
        koji_api = self._runtime.build_retrying_koji_client(caching=True)

        # Getting KMAINT trackers
        trackers_keys = self.trackers
        trackers: List[Issue] = []
        if trackers_keys:
            logger.info("Find kernel bugs linked from KMAINT tracker(s): %s", trackers_keys)
            for key in trackers_keys:
                logger.info("Getting tracker JIRA %s...", key)
                tracker = jira_client.issue(key)
                trackers.append(tracker)
        else:
            logger.info("Searching for open trackers...")
            trackers = self._find_kmaint_trackers(jira_client, config.tracker_jira.project, config.tracker_jira.labels)
            trackers_keys = [t.key for t in trackers]
            logger.info("Found %s tracker(s): %s", len(trackers_keys), trackers_keys)

        # Get kernel bugs linked from KMAINT trackers
        report: Dict[str, Any] = {"kernel_bugs": []}
        for tracker in trackers:
            bugs = self._find_bugs(jira_client, tracker, bz_client, config.bugzilla.target_releases)
            bug_ids = {int(b.id) for b in bugs}
            logger.info("Found %s bug(s) from %s: %s", len(bugs), tracker, bug_ids)
            for bug_id, bug in zip(bug_ids, bugs):
                if bug_id in self._tracker_map and self._tracker_map[bug_id].key != tracker.key:
                    raise ValueError(
                        f"Bug {bug_id} is linked in multiple KMAINT trackers: {tracker.key} {self._tracker_map[bug_id].key}"
                    )
                self._id_bugs[bug_id] = bug
                self._tracker_map[bug_id] = tracker
                report["kernel_bugs"].append(
                    {
                        "id": bug_id,
                        "status": bug.status,
                        "summary": bug.summary,
                        "tracker": tracker,
                    }
                )
            if self.update_tracker:
                self._update_tracker(jira_client, tracker, koji_api, config.target_jira)

        if self.clone and self._id_bugs:
            # Clone kernel bugs into OCP Jira
            logger.info("Cloning bugs...")
            cloned_issues = self._clone_bugs(jira_client, list(self._id_bugs.values()), config.target_jira)
            report["clones"] = {}
            for bug_id, issues in cloned_issues.items():
                report["clones"][bug_id] = sorted(issue.key for issue in issues)
            logger.info("Done.")

        # Print a report
        self._print_report(report, sys.stdout)

    @staticmethod
    def _find_kmaint_trackers(jira_client: JIRA, tracker_project: str, labels: List[str]):
        conditions = [
            f"project = {tracker_project}",
            "status != Closed",
        ]
        if labels:
            conditions.extend([f"labels = \"{label}\"" for label in labels])
        jql = f'{" AND ".join(conditions)} ORDER BY created DESC'
        # 50 most recently created KMAINT trackers should be more than enough
        matched_issues = _search_issues(jira_client, jql, maxResults=50)
        return cast(List[Issue], matched_issues)

    def _find_bugs(self, jira_client: JIRA, tracker: Issue, bz_client: Bugzilla, bz_target_releases: Sequence[str]):
        logger = self._logger
        logger.info("Searching bugs in JIRA %s...", tracker.key)
        links = jira_client.remote_links(tracker.key)
        # Search for kernel bugs in tracker content
        pattern = re.compile(r"(?:bugzilla.redhat.com/|bugzilla.redhat.com/show_bug.cgi\?id=|bz)(\d+)")
        content = f"{tracker.fields.summary}\n{tracker.fields.description}"
        for link in links:
            content += f"\n{link.object.title}\n{link.object.url}"
        m = pattern.findall(content)
        bug_ids = sorted(set(map(int, m)))
        if not bug_ids:
            logger.info("No bugs found from %s", tracker.key)
            return []
        filtered_bugs = self._get_and_filter_bugs(bz_client, bug_ids, bz_target_releases)
        return filtered_bugs

    def _get_and_filter_bugs(self, bz_client: Bugzilla, bug_ids: List[int], bz_target_releases: Sequence[str]):
        """Get specified bugs from Bugzilla, then return those bugs that match the defined target release."""
        logger = self._logger
        filtered_bugs: List[Bug] = []
        logger.info("Getting bugs %s from Bugzilla...", bug_ids)
        bugs = cast(List[Optional[Bug]], bz_client.getbugs(bug_ids))
        target_releases = set(bz_target_releases)
        for bug_id, bug in zip(bug_ids, bugs):
            if not bug:
                raise IOError(f"Error getting bug {bug_id}")
            target_release = bug.cf_zstream_target_release
            if not target_release:
                logger.warning("Target release of bug %s is not set", bug.weburl)
                continue
            if target_release not in target_releases:
                logger.warning(
                    "Bug %s is skipped because target release \"%s\" is not listed", bug.weburl, target_release
                )
                continue
            logger.info("Found bug %s matching target release %s", bug_id, target_release)
            filtered_bugs.append(bug)
        return filtered_bugs

    def _clone_bugs(self, jira_client: JIRA, bugs: Sequence[Bug], conf: KernelBugSweepConfig.TargetJiraConfig):
        logger = self._logger
        ocp_target_release = conf.target_release
        result: Dict[int, List[Issue]] = {}  # key is bug_id, value is a list of cloned jiras
        for bug in bugs:
            bug_id = int(bug.id)
            kmaint_tracker = self._tracker_map.get(bug_id)
            kmaint_tracker_key = kmaint_tracker.key if kmaint_tracker else None
            logger.info("Checking if %s was already cloned to OCP %s...", bug_id, ocp_target_release)
            jql_str = f'project = {conf.project} and component = {conf.component} and labels = art:cloned-kernel-bug and labels = "art:bz#{bug_id}" and "Target Version" = "{ocp_target_release}" order by created DESC'
            found_issues = cast(List[Issue], _search_issues(jira_client, jql_str=jql_str))
            if not found_issues:  # this bug is not already cloned into OCP Jira
                logger.info("Creating JIRA for bug %s...", bug.weburl)
                fields = self._new_jira_fields_from_bug(bug, ocp_target_release, kmaint_tracker_key, conf)
                if not self.dry_run:
                    issue = jira_client.create_issue(fields)
                    jira_client.add_remote_link(issue.key, {"title": f"BZ{bug_id}", "url": bug.weburl})
                    if kmaint_tracker:
                        jira_client.create_issue_link("Blocks", issue.key, kmaint_tracker)
                    result[bug_id] = [issue]
                else:
                    logger.info("[DRY RUN] Would have created Jira for bug %s", bug_id)
            else:  # this bug is already cloned into OCP Jira
                logger.info("Bug %s is already cloned into OCP: %s", bug_id, [issue.key for issue in found_issues])
                result[bug_id] = found_issues
                if not self.reconcile:
                    continue
                fields = self._new_jira_fields_from_bug(bug, ocp_target_release, kmaint_tracker_key, conf)
                for issue in found_issues:
                    if issue.fields.status.name.lower() == "closed":
                        logger.info("No need to reconcile %s because it is Closed.", issue.key)
                        continue
                    logger.info(
                        "Reconciling Jira %s (cloned from bug %s) for %s", issue.key, bug_id, ocp_target_release
                    )
                    if not self.dry_run:
                        issue.update(fields)
                    else:
                        logger.info("[DRY RUN] Would have updated Jira %s to match bug %s", issue.key, bug_id)

        return result

    @staticmethod
    def _print_report(report: Dict, out: TextIO):
        print_func = green_print if out.isatty() else print  # use green_print if out is a TTY
        bugs = sorted(report.get("kernel_bugs", []), key=lambda bug: bug["id"])
        clones = report.get("clones", {})
        for bug in bugs:
            cloned_issues = clones.get(bug['id'], [])
            text = f"{bug['tracker']}\t{bug['id']}\t{'N/A' if not cloned_issues else ','.join(cloned_issues)}\t{bug['status']}\t{bug['summary']}"
            print_func(text, file=out)

    def _update_tracker(
        self,
        jira_client: JIRA,
        tracker: Issue,
        koji_api: koji.ClientSession,
        conf: KernelBugSweepConfig.TargetJiraConfig,
    ):
        logger = LOGGER
        logger.info("Checking if an update to tracker %s is needed...", tracker.key)
        # Determine which NVRs have the fix. e.g. ["kernel-5.14.0-284.14.1.el9_2"]
        nvrs, candidate, shipped = early_kernel.get_tracker_builds_and_tags(logger, tracker, koji_api, conf)

        if shipped:
            early_kernel.process_shipped_tracker(logger, self.dry_run, jira_client, tracker, nvrs, shipped)
        elif candidate:
            early_kernel.comment_on_tracker(
                logger,
                self.dry_run,
                jira_client,
                tracker,
                [f"Build(s) {nvrs} was/were already tagged into {candidate}."],
                # do not reword, see NOTE in method
            )
        else:
            logger.info("No need to update tracker %s", tracker.key)
            return

    @staticmethod
    def _new_jira_fields_from_bug(
        bug: Bug, ocp_target_version: str, kmaint_tracker: Optional[str], conf: KernelBugSweepConfig.TargetJiraConfig
    ):
        summary = f"{bug.summary} [rhocp-{ocp_target_version}]"
        if not summary.startswith("kernel"):  # ensure bug summary start with "kernel"
            summary = "kernel[-rt]: " + summary
        hint = f"Cloned from {bug.weburl} by OpenShift ART Team.\n"
        priority_mapping = {
            "urgent": "Critical",
            "high": "Major",
            "medium": "Normal",
            "low": "Minor",
            "unspecified": "Undefined",
        }
        bug_groups = set(bug.groups)
        fields = {
            "project": {"key": conf.project},
            "components": [{"name": conf.component}],
            "security": {'name': 'Red Hat Employee'} if 'private' in bug_groups or 'redhat' in bug_groups else None,
            "priority": {'name': priority_mapping.get(bug.priority, "Undefined")},
            "summary": summary,
            "description": bug.description,
            "issuetype": {"name": "Bug"},
            "versions": [{"name": ocp_target_version[: ocp_target_version.rindex(".")]}],
            f"{JIRABugTracker.field_target_version}": [
                {
                    "name": ocp_target_version,
                }
            ],
            "labels": ["art:cloned-kernel-bug", f"art:bz#{bug.id}"],
        }
        if kmaint_tracker:
            fields["labels"].append(f"art:kmaint:{kmaint_tracker}")

        is_cve_tracker = set(constants.TRACKER_BUG_KEYWORDS).issubset(set(bug.keywords))
        if is_cve_tracker:
            # TODO: The following lines are commented out because we haven't reached to agreement
            # on how to handle kernel CVEs in OCP at this moment.
            # Without the following lines, kernel CVEs will be copied as normal (non-CVE) bugs.
            # Find flaw bugs associated with the CVE tracker
            #
            # cve_flaws = []
            # for flaw_id, flaw_bug in zip(bug.blocks, bug.bugzilla.getbugs(bug.blocks)):
            #     if not flaw_bug:
            #         raise IOError(f"Error getting flaw bug {flaw_id}. Permission issue?")
            #     if not BugzillaBug(flaw_bug).is_flaw_bug():
            #         continue  # this is not a flaw bug
            #     cve_flaws.append(flaw_bug)
            # labels = {"Security", "SecurityTracking"}
            # cve_names = re.findall(r"(CVE-\d+-\d+)", bug.summary)
            # labels |= set(cve_names)
            # labels |= {f"pscomponent:{component}" for component in bug.components}
            # labels |= {f"flaw:bz#{flaw.id}" for flaw in cve_flaws}
            # fields["labels"] += sorted(labels)

            hint += "Please note that this bug is cloned as a non-CVE bug intentionally due to limitations in interoperability in internal processes.\n"

        fields["description"] = f"{hint}\n----\n{bug.description}"
        return fields


@cli.command("find-bugs:kernel", short_help="Find kernel bugs")
@click.option(
    "--tracker", "trackers", metavar='JIRA_KEY', multiple=True, help="Find by the specified KMAINT tracker JIRA_KEY"
)
@click.option("--clone", is_flag=True, default=False, help="Clone kernel bugs into OCP Jira")
@click.option(
    "--reconcile",
    is_flag=True,
    default=False,
    help="Update summary, description, etc for already cloned Jira bugs. Must be used with --clone",
)
@click.option("--update-tracker", is_flag=True, default=False, help="Update KMAINT trackers state, links, and comments")
@click.option("--dry-run", is_flag=True, default=False, help="Don't change anything")
@click.pass_obj
@click_coroutine
async def find_bugs_kernel_cli(
    runtime: Runtime, trackers: Tuple[str, ...], clone: bool, reconcile: bool, update_tracker: bool, dry_run: bool
):
    """Find kernel bugs in Bugzilla for weekly kernel release through OCP.

    Example 1: Find kernel bugs and print them out
    \b
        $ elliott -g openshift-4.14 find-bugs:kernel

    Example 2: Find kernel bugs and clone them into OCP Jira
    \b
        $ elliott -g openshift-4.14 find-bugs:kernel --clone

    Example 3: Clone kernel bugs into OCP Jira and also update already cloned Jiras
    \b
        $ elliott -g openshift-4.14 find-bugs:kernel --clone --reconcile
    """
    runtime.initialize(mode="none")
    cli = FindBugsKernelCli(
        runtime=runtime,
        trackers=trackers,
        clone=clone,
        reconcile=reconcile,
        update_tracker=update_tracker,
        dry_run=dry_run,
    )
    await cli.run()
