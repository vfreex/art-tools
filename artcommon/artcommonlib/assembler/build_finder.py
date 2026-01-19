import asyncio
import logging
from asyncio.locks import Lock
from typing import Iterable

import asyncstdlib as a
import koji
from artcommonlib.build_util import find_latest_builds
from artcommonlib.rpm_utils import parse_nvr


class BrewBuildFinder:
    """A helper class for finding builds from Brew."""

    def __init__(self, brew_session: koji.ClientSession, logger: logging.Logger | None = None):
        self._brew_session = brew_session
        self._brew_session_lock = Lock()
        self._logger = logger or logging.getLogger(__name__)
        # Cache build_id/nvr -> build_dict to prevent unnecessary queries.
        self._build_cache: dict[str | int, dict | None] = {}

    async def find_from_tag(
        self, build_type: str, tag: str, inherit: bool, assembly: str | None, event: int | None = None
    ) -> list[dict]:
        """Find builds from a specific tag.
        If assembly is given, it will be used to filter the builds by looking at ".assembly.<assembly-name>" part of
        the release field. Note that this method doesn't respect any pins specified in the assembly config.

        :param build_type: "rpm" or "image"
        :param tag: The tag to find builds from.
        :param inherit: Whether to include builds from parent tags.
        :param assembly: Assembly name to query. If None, this method will return true latest builds.
        :param event: Brew event ID.
        :return: A list of build dicts.
        """
        self._logger.info(f"Finding {build_type} builds from tag: {tag}")
        if not assembly:
            # indicates the user wants the latest builds; no assembly filter applied
            self._logger.debug("Finding latest builds in Brew tag %s...", tag)
            builds = await self._list_tagged__cached(tag, latest=True, inherit=inherit, event=event, type=build_type)
        else:
            self._logger.debug("Finding builds specific to assembly %s in Brew tag %s...", assembly, tag)
            tagged_builds = await self._list_tagged__cached(
                tag, latest=False, inherit=inherit, event=event, type=build_type
            )
            builds = list(find_latest_builds(tagged_builds, assembly))
        self._cache_build(*builds)
        return builds

    async def _list_tagged_async(
        self, tag: str, latest: bool, inherit: bool, event: int | None, type: str | None
    ) -> list[dict]:
        async with self._brew_session_lock:
            return await asyncio.to_thread(
                self._brew_session.listTagged, tag, latest=latest, inherit=inherit, event=event, type=type
            )

    @a.lru_cache
    async def _list_tagged__cached(
        self, tag: str, latest: bool, inherit: bool, event: int | None, type: str | None
    ) -> list[dict]:
        return await self._list_tagged_async(tag, latest, inherit, event, type)

    def _cache_build(self, *build: dict):
        """Save build dict to cache"""
        for b in build:
            self._build_cache[b["build_id"]] = b
            self._build_cache[b["nvr"]] = b

    def _get_build_objects(self, id_or_nvrs, session):
        """Get information of multiple Koji/Brew builds

        :param ids_or_nvrs: list of build nvr strings or numbers.
        :param session: instance of :class:`koji.ClientSession`
        :return: a list Koji/Brew build objects
        """
        self._logger.debug("Fetching build info for {} from Koji/Brew...".format(id_or_nvrs))
        tasks = []
        with session.multicall(strict=True) as m:
            for b in id_or_nvrs:
                tasks.append(m.getBuild(b))
        return [task.result for task in tasks]

    def _get_builds(self, id_or_nvrs: Iterable[str | int]) -> list[dict | None]:
        """Get build dicts from Brew. This method uses an internal cache to avoid unnecessary queries.
        :params id_or_nvrs: list of build IDs or NVRs
        :return: a list of Brew build dicts; may contain None for missing builds
        """
        cache_miss = set(id_or_nvrs) - self._build_cache.keys()
        if cache_miss:
            cache_miss = list(cache_miss)
            builds = self._get_build_objects(cache_miss, self._brew_session)
            not_found = []
            for id_or_nvre, build in zip(cache_miss, builds):
                if build:
                    self._cache_build(build)
                else:
                    not_found.append(id_or_nvre)
                    # None indicates the build ID or NVRE doesn't exist
                    self._build_cache[id_or_nvre] = None
        return [self._build_cache[key] for key in id_or_nvrs]

    def from_group_deps(self, el_version: int, assembly_group_config: dict, member_rpm_names: set[str]) -> list[dict]:
        """Returns rpm builds defined in group config dependencies
        :param el_version: RHEL version
        :param group_config: a Model for group config
        :param member_rpm_names: Set of member rpms names. Group dependencies must not include member rpms.
        :param check_existence: Whether to check for the existence of the rpms.
        :return: a dict; keys are component names, values are Brew build dicts
        """
        deps = assembly_group_config.get("dependencies", {}).get("rpms", [])
        el_key = f"el{el_version}"
        dep_nvrs = {parse_nvr(dep[el_key])["name"]: dep[el_key] for dep in deps if el_key in dep}
        prohibited = dep_nvrs.keys() & member_rpm_names
        if prohibited:
            raise ValueError(f"Group dependencies cannot include member rpms: {prohibited}")
        builds = self._get_builds(dep_nvrs.values())
        missing = [nvr for nvr, build in zip(dep_nvrs.values(), builds) if build is None]
        if missing:
            raise ValueError(f"The following group dependency builds do not exist in Brew: {missing}")
        return builds
