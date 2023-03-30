from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
import typing as t
from zipfile import ZipFile

from kedro.io import AbstractDataSet


@dataclass
class KaggleBundle:
    dataset_or_competition: str
    members: t.Dict[str, BytesIO]
    is_competition: bool
    single_file: bool


class KaggleDataSet(AbstractDataSet[KaggleBundle, KaggleBundle]):
    def __init__(
        self,
        dataset: str,
        directory: str,
        is_competition: bool = False,
        file_name: t.Optional[str] = None,
    ):
        self._dataset_or_competition = dataset
        self._directory = Path(directory)
        self._is_competition = is_competition
        self._file_name = file_name

        if not self._directory.is_dir():
            raise ValueError("Target directory does not exist")

    @property
    def _api(self):
        # Perform import here to avoid early attempt to authenticate
        from kaggle import api as kaggle_api

        return kaggle_api

    def __list_members(self) -> t.List[str]:
        if self._is_competition:
            members = [
                member.ref
                for member in self._api.competition_list_files(
                    self._dataset_or_competition
                )
            ]
        else:
            result = self._api.dataset_list_files(self._dataset_or_competition)
            members = [member.ref for member in result.files] if result.files else []

        return members

    def __unzip_and_delete(self, zip_path: str, members_list: t.List[str]) -> None:
        with ZipFile(str(zip_path)) as zip:
            zip.extractall(str(self._directory), members=members_list)

        zip_path.unlink()

    def __download_single_file(self, members_list) -> None:
        if self._is_competition:
            self._api.competition_download_file(
                self._dataset_or_competition,
                self._file_name,
                path=str(self._directory),
                force=True,
                quiet=False,
            )
        else:
            self._api.dataset_download_file(
                self._dataset_or_competition,
                self._file_name,
                path=str(self._directory),
                force=True,
                quiet=False,
            )

            self.__unzip_and_delete(
                self._directory / (self._file_name + ".zip"), members_list
            )

    def __download_whole_dataset(self, members_list: t.List[str]) -> None:
        if self._is_competition:
            self._api.competition_download_files(
                self._dataset_or_competition,
                path=str(self._directory),
                force=True,
                quiet=False,
            )

            # Extract and remove zip
            self.__unzip_and_delete(
                self._directory / (self._dataset_or_competition + ".zip"), members_list
            )
        else:
            # Always unzip
            self._api.dataset_download_files(
                self._dataset_or_competition,
                path=str(self._directory),
                force=True,
                quiet=False,
                unzip=True,
            )

    def _load(self) -> KaggleBundle:
        if self._file_name is None:
            members_list = self.__list_members()
            self.__download_whole_dataset(members_list)
        else:
            members_list = self.__list_members()
            if self._file_name not in members_list:
                raise ValueError(f"File not found, available files are {members_list}")
            self.__download_single_file(members_list)

        members = {}
        for member_filename in members_list:
            with open(self._directory / member_filename, "rb") as fh:
                members[member_filename] = BytesIO(fh.read())

        return KaggleBundle(
            self._dataset_or_competition,
            members,
            self._is_competition,
            self._file_name is not None,
        )

    def _save(self, _) -> None:
        raise NotImplementedError("Cannot save back to Kaggle")

    def _describe(self) -> t.Dict[str, t.Any]:
        ...
