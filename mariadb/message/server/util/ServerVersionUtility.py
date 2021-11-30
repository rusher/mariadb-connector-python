class ServerVersionUtility:

    def __init__(self, server_version: str, mariadb_server: bool):
        self.__version = server_version
        major = 0
        minor = 0
        patch = 0
        qualif = ""

        length = len(server_version)

        parse_type = 0
        val = 0
        for offset in range(length):
            car = ord(server_version[offset])
            if ord('0') > car or car > ord('9'):
                if parse_type == 0:
                    major = val
                elif parse_type == 1:
                    minor = val
                elif parse_type == 2:
                    patch = val
                    qualif = server_version[offset:]
                    offset = length
                    break
                else:
                    break
                parse_type += 1
                val = 0
            else:
                val = val * 10 + car - 48

        if parse_type == 2:
            patch = val

        self.__major_version = major
        self.__minor_version = minor
        self.__patch_version = patch
        self.__qualifier = qualif
        self.__mariadb_server = mariadb_server

    @property
    def mariadb_server(self) -> bool:
        return self.__mariadb_server

    @property
    def version(self) -> str:
        return self.__version

    @property
    def major_version(self) -> int:
        return self.__major_version

    @property
    def minor_version(self) -> int:
        return self.__minor_version

    @property
    def patch_version(self) -> int:
        return self.__patch_version

    @property
    def qualifier(self) -> str:
        return self.__qualifier

    def version_greater_or_equal(self, major, minor, patch) -> bool:
        if self.__major_version > major:
            return True

        if self.__major_version < major:
            return False

        if self.__minor_version > minor:
            return True

        if self.__minor_version < minor:
            return False

        # Minor versions are equal, compare patch version.
        return self.__patch_version >= patch
