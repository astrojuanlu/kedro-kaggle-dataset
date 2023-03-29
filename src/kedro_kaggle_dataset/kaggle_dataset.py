from dataclasses import dataclass
from pathlib import Path
import typing as t
from zipfile import ZipFile

from kedro.io import AbstractDataSet


@dataclass
class KaggleBundle:
    dataset_or_competition: str
    members: t.List[str]
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

    def __load_single_file(self) -> KaggleBundle:
        members = self.__list_members()
        if self._file_name not in members:
            raise ValueError(f"File not found, available files are {members}")

        return KaggleBundle(
            self._dataset_or_competition, [self._file_name], self._is_competition, True
        )

    def __load_whole_dataset(self) -> KaggleBundle:
        members = self.__list_members()
        return KaggleBundle(
            self._dataset_or_competition, members, self._is_competition, False
        )

    def _load(self) -> KaggleBundle:
        if self._file_name is None:
            result = self.__load_whole_dataset()
        else:
            result = self.__load_single_file()

        return result

    def __save_single_file(self, data: KaggleBundle) -> None:
        if data.is_competition:
            self._api.competition_download_file(
                data.dataset_or_competition,
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

            # Extract and remove zip
            # TODO: Refactor
            zip_path = self._directory / (self._file_name + ".zip")
            with ZipFile(str(zip_path)) as zip:
                zip.extractall(str(self._directory), members=data.members)

            zip_path.unlink()

    def __save_whole_dataset(self, data: KaggleBundle) -> None:
        if self._is_competition:
            self._api.competition_download_files(
                data.dataset_or_competition,
                path=str(self._directory),
                force=True,
                quiet=False,
            )

            # Extract and remove zip
            zip_path = self._directory / (data.dataset_or_competition + ".zip")
            with ZipFile(str(zip_path)) as zip:
                zip.extractall(str(self._directory), members=data.members)

            zip_path.unlink()
        else:
            # Always unzip
            self._api.dataset_download_files(
                data.dataset_or_competition,
                path=str(self._directory),
                force=True,
                quiet=False,
                unzip=True,
            )

    def _save(self, data: KaggleBundle) -> None:
        if data.single_file:
            self.__save_single_file(data)
        else:
            self.__save_whole_dataset(data)

    def _describe(self) -> t.Dict[str, t.Any]:
        ...
