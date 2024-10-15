import zipfile
from io import BytesIO


def test_create_archive_in_memory():
    with BytesIO() as zip_memory:
        with zipfile.ZipFile(zip_memory, "w") as zf:
            zf.writestr("hello_world.txt", "How are you?")

        zip_data = zip_memory.getvalue()
    assert len(zip_data) > 0


def test_create_archive_file(tmp_path):
    with zipfile.ZipFile(tmp_path / "archive.zip", "w") as zf:
        zf.writestr("hello_world.txt", "How are you?")

    assert (tmp_path / "archive.zip").is_file()
