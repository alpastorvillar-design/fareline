from __future__ import annotations

from pathlib import Path

import pytest

from fareline.m2 import paths


def test_a_posix_path_becomes_a_three_slash_file_uri() -> None:
    assert paths.as_uri("/opt/fareline/warehouse") == "file:///opt/fareline/warehouse"


def test_a_windows_path_keeps_its_drive_out_of_the_authority() -> None:
    # file://C:/x makes "C:" the URI authority and the path unreachable.
    assert paths.as_uri(r"C:\Users\example\warehouse") == "file:///C:/Users/example/warehouse"


def test_a_file_uri_survives_a_round_trip_back_to_a_local_path() -> None:
    from urllib.parse import unquote, urlparse

    parsed = urlparse(paths.as_uri("/opt/fareline/warehouse"))

    assert parsed.netloc == ""
    assert unquote(parsed.path) == "/opt/fareline/warehouse"


def test_an_isolated_rebuild_root_is_accepted(tmp_path: Path) -> None:
    resolved = paths.verify_rebuild_root(
        tmp_path / "rebuild", protected=[tmp_path / "warehouse", tmp_path / "landing"]
    )

    assert resolved == (tmp_path / "rebuild").resolve()


def test_a_rebuild_root_equal_to_a_protected_root_is_refused(tmp_path: Path) -> None:
    with pytest.raises(paths.UnsafeRebuildRoot):
        paths.verify_rebuild_root(tmp_path / "warehouse", protected=[tmp_path / "warehouse"])


def test_a_rebuild_root_inside_a_protected_root_is_refused(tmp_path: Path) -> None:
    with pytest.raises(paths.UnsafeRebuildRoot):
        paths.verify_rebuild_root(
            tmp_path / "warehouse" / "scratch", protected=[tmp_path / "warehouse"]
        )


def test_a_rebuild_root_containing_a_protected_root_is_refused(tmp_path: Path) -> None:
    # The worst case: deleting the parent takes the warehouse with it.
    with pytest.raises(paths.UnsafeRebuildRoot):
        paths.verify_rebuild_root(tmp_path, protected=[tmp_path / "warehouse"])


def test_a_filesystem_root_is_never_a_rebuild_root(tmp_path: Path) -> None:
    with pytest.raises(paths.UnsafeRebuildRoot):
        paths.verify_rebuild_root(Path(tmp_path.anchor), protected=[])


def test_the_refusal_names_the_protected_root_it_overlaps(tmp_path: Path) -> None:
    with pytest.raises(paths.UnsafeRebuildRoot) as error:
        paths.verify_rebuild_root(tmp_path / "landing", protected=[tmp_path / "landing"])

    assert "landing" in str(error.value)


def test_clearing_an_isolated_rebuild_root_removes_it(tmp_path: Path) -> None:
    rebuild = tmp_path / "rebuild"
    paths.prepare_rebuild_root(rebuild, protected=[tmp_path / "warehouse"])
    (rebuild / "table").mkdir()
    (rebuild / "table" / "part.parquet").write_bytes(b"x")

    paths.clear_rebuild_root(rebuild, protected=[tmp_path / "warehouse"])

    assert rebuild.is_dir()
    assert paths.rebuild_marker_path(rebuild).is_file()
    assert not (rebuild / "table").exists()


def test_clearing_an_absent_rebuild_root_is_a_no_op(tmp_path: Path) -> None:
    paths.clear_rebuild_root(tmp_path / "rebuild", protected=[tmp_path / "warehouse"])

    assert paths.rebuild_marker_path(tmp_path / "rebuild").is_file()


def test_an_existing_unmarked_directory_is_never_deleted_as_a_rebuild(tmp_path: Path) -> None:
    rebuild = tmp_path / "rebuild"
    rebuild.mkdir()
    valuable = rebuild / "unrelated.txt"
    valuable.write_text("keep", encoding="utf-8")

    with pytest.raises(paths.UnsafeRebuildRoot, match="not owned by Fareline"):
        paths.clear_rebuild_root(rebuild, protected=[tmp_path / "warehouse"])

    assert valuable.read_text(encoding="utf-8") == "keep"


def test_clearing_refuses_a_protected_root_without_deleting_anything(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    (warehouse / "contracted").mkdir(parents=True)
    (warehouse / "contracted" / "part.parquet").write_bytes(b"x")

    with pytest.raises(paths.UnsafeRebuildRoot):
        paths.clear_rebuild_root(warehouse, protected=[warehouse])

    assert (warehouse / "contracted" / "part.parquet").is_file()


def test_a_git_checkout_above_the_working_directory_is_found(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    (checkout / "src" / "fareline").mkdir(parents=True)

    assert paths.repository_root(checkout / "src" / "fareline") == checkout.resolve()


def test_a_directory_outside_any_checkout_has_no_repository_root(tmp_path: Path) -> None:
    assert paths.repository_root(tmp_path) is None


def test_each_contract_fingerprint_owns_its_own_physical_tables(tmp_path: Path) -> None:
    # Appending a revised output schema to the previous table is a Delta
    # metadata conflict, so a revision has to land somewhere else.
    first = paths.table_paths(tmp_path, "yellow", "a" * 64)
    second = paths.table_paths(tmp_path, "yellow", "b" * 64)

    assert set(first.all()).isdisjoint(second.all())
    assert all("a" * 64 in str(item) for item in first.all())
    assert first == paths.table_paths(tmp_path, "yellow", "a" * 64)


def test_two_services_never_share_a_physical_table(tmp_path: Path) -> None:
    fingerprint = "c" * 64

    yellow = paths.table_paths(tmp_path, "yellow", fingerprint)
    hvfhv = paths.table_paths(tmp_path, "hvfhv", fingerprint)

    assert set(yellow.all()).isdisjoint(hvfhv.all())
