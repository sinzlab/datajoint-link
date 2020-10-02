import os
import random

import pytest
import datajoint as dj


USES_EXTERNAL = False


@pytest.fixture
def deletion_requested_entities_primary_keys(n_entities, src_data):
    proportion_deletion_requested = float(os.environ.get("PROPORTION_DELETION_REQUESTED", 0.2))
    n_deletion_requested = round(n_entities * proportion_deletion_requested)
    random.seed(42)
    deletion_requested_entities = [random.choice(src_data) for _ in range(n_deletion_requested)]
    return sorted([dict(prim_attr=e["prim_attr"]) for e in deletion_requested_entities], key=lambda e: e["prim_attr"])


@pytest.fixture
def outbound_deletion_requested_flags(deletion_requested_entities_primary_keys):
    return [e for e in deletion_requested_entities_primary_keys]


@pytest.fixture
def expected_local_deletion_requested_flags(deletion_requested_entities_primary_keys):
    return [e for e in deletion_requested_entities_primary_keys]


@pytest.fixture
def src_admin_conn(src_db_config, get_conn):
    with get_conn(src_db_config, "admin") as conn:
        yield conn


@pytest.fixture
def outbound_table_cls(src_db, local_db, outbound_schema_name, src_table_name, local_table_cls, src_admin_conn):
    module = dj.create_virtual_module("outbound_schema", outbound_schema_name, connection=src_admin_conn)
    return getattr(module, src_table_name + "Outbound")


@pytest.fixture
def insert_flags_and_refresh(local_table_cls_with_pulled_data, outbound_deletion_requested_flags, outbound_table_cls):
    outbound_table_cls().DeletionRequested().insert(outbound_deletion_requested_flags)
    local_table_cls_with_pulled_data().refresh()


@pytest.fixture
def insert_flags_refresh_and_delete_deletion_requested_entities(
    insert_flags_and_refresh, local_table_cls_with_pulled_data
):
    flags = local_table_cls_with_pulled_data().DeletionRequested.fetch(as_dict=True)
    (local_table_cls_with_pulled_data() & flags).delete()


@pytest.mark.usefixtures("insert_flags_and_refresh")
def test_if_deletion_requested_entities_are_present_in_deletion_requested_part_table_on_local_side(
    local_table_cls_with_pulled_data, expected_local_deletion_requested_flags
):
    actual_local_deletion_requested_flags = local_table_cls_with_pulled_data().DeletionRequested.fetch(as_dict=True)
    assert actual_local_deletion_requested_flags == expected_local_deletion_requested_flags


@pytest.mark.usefixtures("insert_flags_refresh_and_delete_deletion_requested_entities")
def test_if_locally_deleted_deletion_requested_entities_are_present_in_deletion_approved_part_table_in_outbound_table(
    outbound_table_cls, outbound_deletion_requested_flags
):
    deletion_approved_flags = outbound_table_cls.DeletionApproved().fetch(as_dict=True)
    assert deletion_approved_flags == outbound_deletion_requested_flags
