import asyncio
import logging
from collections.abc import Collection

import koji
from artcommonlib.build_util import find_latest_builds

LOGGER = logging.getLogger(__name__)


# class Assembler:
#     """
#     Assembles release content according to assembly definitions.
#     """

#     def __init__(self):
#         pass

#     def assemble(self):
#         # Implementation of the assembly process
#         pass


class BrewRpmAssembler:
    """
    Assembles rpm builds from Brew tags.
    """

    def __init__(self, brew_tags: list[str], inherit: bool, event: int | None, koji_api: koji.ClientSession):
        self._brew_tags = brew_tags
        self._koji_api = koji_api
        self._inherit = inherit
        self._event = event

    def _list_tagged_rpms(self, tags: list[str], latest: bool) -> list[dict]:
        with self._koji_api.multicall(strict=True) as m:
            calls = [
                m.listTagged(tag, latest=latest, inherit=self._inherit, event=self._event, type="rpm") for tag in tags
            ]
        return [call.result for call in calls]

    async def assemble_with_assembly(self, assembly_name: str, assembly_config: dict):
        # Assemblies are enabled. We need all tagged builds in the brew tag then find the latest ones for the assembly.
        LOGGER.debug("Finding builds specific to assembly %s in Brew tag %s...", assembly_name, self._brew_tags)
        tagged_builds = await asyncio.to_thread(self._list_tagged_rpms, self._brew_tags, latest=False)
        latest_builds = find_latest_builds(tagged_builds, assembly_name)
        return latest_builds

    async def assemble_without_assembly(self) -> list[dict]:
        LOGGER.debug("Finding latest builds in Brew tags %s...", self._brew_tags)
        latest_builds = await asyncio.to_thread(self._list_tagged_rpms, self._brew_tags, latest=True)
        return latest_builds


# class BrewRpmAssembler:
#     """
#     Finds Brew RPM builds according to specified criteria.
#     """

#     def __init__(self):
#         pass

#     # find Brew builds by tags
#     def find_by_tags(self, tags: Collection[str], inherit: bool, assembly: str | None, build_type: str | None = None, event: int | None = None):
#         """
#         Finds Brew builds by tags.
#         :param tags: The tags to filter builds by.
#         :param inherit: Whether to include inherited builds.
#         :param build_type: The type of builds to include.
#         :param assembly: The assembly to filter builds by.
#         :param event: The event ID to filter builds by.
#         """
#         if not assembly:
#             # Assemblies are disabled. We need the true latest tagged builds in the brew tag
#             LOGGER.debug("Finding latest builds in Brew tag %s...", tag)
#             builds = self._koji_api.listTagged(tag, latest=True, inherit=inherit, event=event, type=build_type)
#         else:
#             # Assemblies are enabled. We need all tagged builds in the brew tag then find the latest ones for the assembly.
#             LOGGER.debug("Finding builds specific to assembly %s in Brew tag %s...", assembly, tag)
#             tagged_builds = self._koji_api.listTagged(tag, latest=False, inherit=inherit, event=event, type=build_type)
#             builds = find_latest_builds(tagged_builds, assembly)
#         component_builds = {build["name"]: build for build in builds}
#         LOGGER.debug("Found %s builds.", len(component_builds))
#         # for build in component_builds.values():  # Save to cache
#         #     self._cache_build(build)
#         return component_builds
