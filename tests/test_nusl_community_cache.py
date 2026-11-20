"""Community lookup in the NUSL transformer, with the community read mocked.

Run without an app, e.g. inside the nr-docs devcontainer:
    PYTHONPATH=. pytest tests/test_nusl_community_cache.py
"""

import logging
from unittest import mock

import pytest
from invenio_communities.errors import CommunityDeletedError
from invenio_pidstore.errors import PIDDoesNotExistError
from oarepo_runtime.datastreams.types import StreamBatch, StreamEntry

from nr_oaipmh_harvesters.nusl import transformer as t

# 998__a codes mapped to existing slugs in NUSL_ID_TO_SLUG_MAPPING
GENERIC_CODE = next(c for c, s in t.NUSL_ID_TO_SLUG_MAPPING.items() if s == "generic")
NEW_CODE = "zapadoceska_univerzita"
NEW_SLUG = t.NUSL_ID_TO_SLUG_MAPPING[NEW_CODE]  # "6f0m"


class FakeRead:
    """Stands in for current_communities.service.read; records the slugs read."""

    def __init__(self, **communities):
        self.communities = dict(communities)
        self.deleted = set()
        self.error = None
        self.calls = []

    def __call__(self, identity, slug):
        self.calls.append(slug)
        if self.error:
            raise self.error
        if slug in self.deleted:
            raise CommunityDeletedError(mock.Mock())
        if slug not in self.communities:
            raise PIDDoesNotExistError("comid", slug)
        return mock.Mock(id=self.communities[slug])


@pytest.fixture
def read():
    fake = FakeRead(generic="id-generic")
    service = mock.Mock(read=fake)
    with mock.patch(
        "invenio_communities.proxies.current_communities", mock.Mock(service=service)
    ):
        yield fake


@pytest.fixture
def clock():
    """Controllable time.monotonic() as seen by the transformer."""
    now = [1000.0]
    with mock.patch.object(t.time, "monotonic", lambda: now[0]):
        yield now


@pytest.fixture(autouse=True)
def empty_memo():
    t._community_ids.clear()
    t._missing_communities.clear()
    yield
    t._community_ids.clear()
    t._missing_communities.clear()


def run_998(*codes):
    entry = StreamEntry(entry={"998__a": list(codes)})
    entry.transformed = {}
    entry.processed = set()
    t.transform_998_collection({}, entry)
    return entry.transformed.get("parent")


def run_transformer(code, allowed_slugs):
    entry = StreamEntry(
        entry={"998__a": [code]}, context={"oai": {"identifier": "oai:x:1"}}
    )
    t.NUSLTransformer(identity=None, allowed_slugs=allowed_slugs).apply(
        StreamBatch(entries=[entry])
    )
    return entry


def test_found_community_is_read_once(read, clock):
    for _ in range(3):
        parent = run_998(GENERIC_CODE)
        assert parent == {"communities": {"default": "id-generic", "ids": ["id-generic"]}}
    assert read.calls == ["generic"]


def test_missing_community_is_read_again_only_after_interval(read, clock, caplog):
    with caplog.at_level(logging.WARNING, logger="oaipmh.harvester"):
        assert run_998(NEW_CODE) is None
        for _ in range(100):
            clock[0] += 0.1
            assert run_998(NEW_CODE) is None
    assert read.calls == [NEW_SLUG]
    assert len(caplog.records) == 101
    assert NEW_SLUG in caplog.records[0].getMessage()
    assert NEW_CODE in caplog.records[0].getMessage()

    # community created while the worker runs; found on the next read
    read.communities[NEW_SLUG] = "id-new"
    clock[0] += t.COMMUNITY_MISS_RETRY_INTERVAL
    assert run_998(NEW_CODE) == {"communities": {"default": "id-new", "ids": ["id-new"]}}
    assert run_998(NEW_CODE)["communities"]["default"] == "id-new"
    assert read.calls == [NEW_SLUG, NEW_SLUG]


def test_deleted_community_is_not_assigned(read, clock, caplog):
    read.communities[NEW_SLUG] = "id-new"
    read.deleted.add(NEW_SLUG)
    with caplog.at_level(logging.WARNING, logger="oaipmh.harvester"):
        assert run_998(NEW_CODE) is None
    assert len(caplog.records) == 1


def test_unmapped_code_does_not_read_or_log(read, clock, caplog):
    with caplog.at_level(logging.DEBUG, logger="oaipmh.harvester"):
        assert run_998("not_a_nusl_collection") is None
    assert caplog.records == []
    assert read.calls == []


def test_unexpected_read_error_is_not_treated_as_missing(read, clock):
    read.error = RuntimeError("database unavailable")
    entry = run_transformer(GENERIC_CODE, allowed_slugs=None)
    assert [e.message for e in entry.errors] == ["database unavailable"]
    assert "generic" not in t._missing_communities

    read.error = None
    assert run_998(GENERIC_CODE)["communities"]["default"] == "id-generic"


def test_first_existing_community_is_default(read, clock):
    read.communities[NEW_SLUG] = "id-new"
    parent = run_998("not_a_nusl_collection", NEW_CODE, GENERIC_CODE)
    assert parent == {"communities": {"default": "id-new", "ids": ["id-new", "id-generic"]}}


def test_allowlisted_record_without_community_is_an_error(read, clock):
    entry = run_transformer(NEW_CODE, allowed_slugs=NEW_SLUG)
    assert not entry.filtered
    assert len(entry.errors) == 1
    assert f"Community {NEW_SLUG} not found" in entry.errors[0].message


def test_allowlisted_record_with_community_passes(read, clock):
    entry = run_transformer(GENERIC_CODE, allowed_slugs="generic")
    assert entry.errors == []
    assert entry.entry["parent"]["communities"]["default"] == "id-generic"
