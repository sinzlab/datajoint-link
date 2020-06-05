import os
import random

import pytest
import datajoint as dj


USES_EXTERNAL = False


@pytest.fixture
def flagged_entities_primary_keys(n_entities, src_data):
    proportion_flagged = float(os.environ.get("PROPORTION_FLAGGED", 0.2))
    n_flagged = round(n_entities * proportion_flagged)
    random.seed(42)
    flagged_entities = [random.choice(src_data) for _ in range(n_flagged)]
    return sorted([dict(prim_attr=e["prim_attr"]) for e in flagged_entities], key=lambda e: e["prim_attr"])


@pytest.fixture
def source_flags(local_db_config, flagged_entities_primary_keys):
    return [
        dict(e, remote_host=local_db_config.name, remote_schema=local_db_config.schema_name)
        for e in flagged_entities_primary_keys
    ]


@pytest.fixture
def expected_local_flags(src_db_config, flagged_entities_primary_keys):
    return [
        dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name)
        for e in flagged_entities_primary_keys
    ]


@pytest.fixture
def src_admin_conn(src_db_config, get_conn):
    with get_conn(src_db_config, "admin") as conn:
        yield conn


@pytest.fixture
def outbound_table_cls(src_db, local_db, outbound_schema_name, src_table_name, local_table_cls, src_admin_conn):
    module = dj.create_virtual_module("outbound_schema", outbound_schema_name, connection=src_admin_conn)
    return getattr(module, src_table_name + "Outbound")


@pytest.fixture
def insert_flags(local_table_cls_with_pulled_data, source_flags, outbound_table_cls):
    outbound_table_cls().FlaggedForDeletion().insert(source_flags)


@pytest.fixture
def delete_flagged_entities(local_table_cls_with_pulled_data):
    flags = local_table_cls_with_pulled_data().flagged_for_deletion.fetch(as_dict=True)
    (local_table_cls_with_pulled_data() & flags).delete()


@pytest.mark.usefixtures("insert_flags")
def test_if_flagged_entities_are_present_in_flagged_for_deletion_part_table_on_local_side(
    local_table_cls_with_pulled_data, expected_local_flags
):
    actual_local_flags = local_table_cls_with_pulled_data().flagged_for_deletion.fetch(as_dict=True)
    assert actual_local_flags == expected_local_flags


@pytest.mark.usefixtures("insert_flags", "delete_flagged_entities")
def test_if_deleted_flagged_entities_are_present_in_ready_for_deletion_part_table_on_source_side(
    outbound_table_cls, source_flags
):
    ready_for_deletion_flags = outbound_table_cls.ReadyForDeletion().fetch(as_dict=True)
    assert ready_for_deletion_flags == source_flags


@pytest.mark.usefixtures("insert_flags")
def test_if_pulling_skips_flagged_entities(local_table_cls_with_pulled_data):
    data_before_pull = local_table_cls_with_pulled_data().fetch(as_dict=True)
    with pytest.warns(UserWarning):
        local_table_cls_with_pulled_data().pull()
        data_after_pull = local_table_cls_with_pulled_data().fetch(as_dict=True)
        assert data_before_pull == data_after_pull
