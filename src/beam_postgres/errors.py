class BeamPostgresError(Exception):
    """Base class for all Beam Postgres errors."""

    pretext = ""

    def __init__(self, msg, *args):
        if self.pretext:
            msg = f"{self.pretext}: {msg}"
        super().__init__(msg, *args)


class BeamPostgresClientError(BeamPostgresError):
    """An error in the postgres client object (e.g. failed to execute query)."""

    pretext = "Beam Postgres error"
