import io

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile


def _augment_with_directories(info_dict):
    # Build all directories from top to bottom
    # until we reach a point in which we cannot go deeper
    all_directory_names = set()
    level = 1
    while True:
        new_directories = set(
            partial_path
            for file_dict in info_dict
            if "/" in file_dict["name"]
            and (partial_path := "/".join(file_dict["name"].split("/")[:level]))
            != file_dict["name"]
        )
        if not new_directories:
            break

        all_directory_names = all_directory_names.union(new_directories)
        level += 1

    info_dict = info_dict + [
        {
            "name": dir_name,
            "size": sum(
                file_dict["size"]
                for file_dict in info_dict
                if file_dict["name"].startswith(dir_name)
            ),
            "type": "directory",
        }
        for dir_name in all_directory_names
    ]

    return info_dict


def _filter_by_path(info_dict, segments):
    # If there are no segments, return only top level
    if not segments:
        info_dict = [
            member_dict for member_dict in info_dict if "/" not in member_dict["name"]
        ]
    else:
        # First, try to see if there is a file with the requested name
        full_path = "/".join(segments)
        matching_files = [
            member_dict
            for member_dict in info_dict
            if member_dict["name"] == full_path and member_dict["type"] == "file"
        ]
        if matching_files:
            # We assume we have exactly one matching file,
            # but one can never fully trust computers
            info_dict = [matching_files[0]]
        else:
            # Exact path was not found,
            # let's assume the path represents a directory
            info_dict = [
                member_dict
                for member_dict in info_dict
                if (
                    (member_dict["name"].split("/")[: len(segments)] == segments)
                    and len(member_dict["name"].split("/")) == (len(segments) + 1)
                )
            ]

    return info_dict


class KaggleCompetitionFileSystem(AbstractFileSystem):
    def __init__(self, username, password, **kwargs):
        super().__init__(**kwargs)

        self._username = username
        self._password = password

        self._api = None

    @property
    def api(self):
        if self._api is not None:
            return self._api

        try:
            # Trigger possibly failed authentication
            # because kaggle.__init__ has an unavoidable .authenticate() call in it
            import kaggle
        except Exception:
            pass

        # Since Python modules are singletons, now we can proceed
        # Why Kaggle has to make it so hard?
        from kaggle.api.kaggle_api_extended import KaggleApi
        from kaggle.api_client import ApiClient
        from kaggle.configuration import Configuration

        # This is concise but does not allow easy addition of Configuration values
        configuration = Configuration()
        configuration.username = self._username
        configuration.password = self._password

        self._api = KaggleApi(ApiClient(configuration))

        return self._api

    def _list_all_files(self, path):
        path = self._strip_protocol(path)

        competition_name, *rest = path.split("/", maxsplit=2)
        if not competition_name:
            raise ValueError("Must specify competition name")
        result_files = self.api.competition_list_files(competition=competition_name)

        return result_files, rest

    # Might use:
    # def cat_file(self, path, start=None, end=None, **kwargs) -> str:
    # def exists(self, path, **kwargs) -> bool:
    # def get_file(self, rpath, lpath, callback=_DEFAULT_CALLBACK, outfile=None, **kwargs):
    # def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs) -> t.Union[t.List[str], t.Dict[str, t.Any]]:
    # FIXME: info of a file turns it into directory
    # def info(self, path):
    # FIXME: size of a file returns zero
    # def size(self, path):

    # FIXME: How should I download the whole bundle with this scheme?

    # Need
    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ) -> AbstractBufferedFile:
        """Return raw bytes-mode file-like from the file-system"""

    def open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        compression=None,
        **kwargs,
    ) -> io.TextIOWrapper:
        """
        Return a file-like object from the filesystem

        The resultant instance must function correctly in a context ``with``
        block.

        Parameters
        ----------
        path: str
            Target file
        mode: str like 'rb', 'w'
            See builtin ``open()``
        block_size: int
            Some indication of buffering - this is a value in bytes
        cache_options : dict, optional
            Extra arguments to pass through to the cache.
        compression: string or None
            If given, open file using compression codec. Can either be a compression
            name (a key in ``fsspec.compression.compr``) or "infer" to guess the
            compression from the filename suffix.
        encoding, errors, newline: passed on to TextIOWrapper for text mode
        """

    def ls(self, path, detail=True, **kwargs):
        """List objects at path.

        This should include subdirectories and files at that location. The
        difference between a file and a directory must be clear when details
        are requested.

        The specific keys, or perhaps a FileInfo class, or similar, is TBD,
        but must be consistent across implementations.
        Must include:

        - full path to the entry (without protocol)
        - size of the entry, in bytes. If the value cannot be determined, will
          be ``None``.
        - type of entry, "file", "directory" or other

        Additional information
        may be present, appropriate to the file-system, e.g., generation,
        checksum, etc.

        May use refresh=True|False to allow use of self._ls_from_cache to
        check for a saved listing and avoid calling the backend. This would be
        common where listing may be expensive.

        Parameters
        ----------
        path: str
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        kwargs: may have additional backend-specific options, such as version
            information

        Returns
        -------
        List of strings if detail is False, or list of directory information
        dicts if detail is True.
        """
        result_files, rest = self._list_all_files(path)

        all_members = _augment_with_directories(
            [
                {"name": file.name, "size": file.totalBytes, "type": "file"}
                for file in result_files
            ]
        )

        info_dict = _filter_by_path(all_members, rest)
        if not info_dict:
            raise ValueError("Path not found")

        if detail:
            return info_dict
        else:
            return [member_dict["name"] for member_dict in info_dict]
