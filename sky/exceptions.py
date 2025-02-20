"""Exceptions."""
import enum
from typing import List, Optional

# Return code for keyboard interruption and SIGTSTP
KEYBOARD_INTERRUPT_CODE = 130
SIGTSTP_CODE = 146
RSYNC_FILE_NOT_FOUND_CODE = 23


class ResourcesUnavailableError(Exception):
    """Raised when resources are unavailable.

    This is mainly used for the APIs in sky.execution; please refer to
    the docstring of sky.launch for more details about how the
    failover_history will be set.
    """

    def __init__(self,
                 *args: object,
                 no_failover: bool = False,
                 failover_history: Optional[List[Exception]] = None) -> None:
        super().__init__(*args)
        self.no_failover = no_failover
        if failover_history is None:
            failover_history = []
        # Copy the list to avoid modifying from outside.
        self.failover_history: List[Exception] = list(failover_history)

    def with_failover_history(self, failover_history: List[Exception]) -> None:
        # Copy the list to avoid modifying from outside.
        self.failover_history = list(failover_history)
        return self


class ProvisionPrechecksError(Exception):
    """Raised when a spot job fails prechecks before provision.
    Developer note: For now this should only be used by managed
    spot code path (technically, this can/should be raised by the
    lower-level sky.launch()). Please refer to the docstring of
    `spot.recovery_strategy._launch` for more details about when
    the error will be raised.

    Args:
        reasons: (List[Exception]) The reasons why the prechecks failed.
    """

    def __init__(self, *args: object, reasons: List[Exception]) -> None:
        super().__init__(*args)
        self.reasons = list(reasons)


class SpotJobReachedMaxRetriesError(Exception):
    """Raised when a spot job fails to be launched after maximum retries.

    Developer note: For now this should only be used by managed spot code
    path. Please refer to the docstring of `spot.recovery_strategy._launch`
    for more details about when the error will be raised.
    """
    pass


class ResourcesMismatchError(Exception):
    """Raised when resources are mismatched."""
    pass


class CommandError(Exception):
    """Raised when a command fails.

    Args:
    returncode: The returncode of the command.
    command: The command that was run.
    error_message: The error message to print.
    """

    def __init__(self, returncode: int, command: str, error_msg: str) -> None:
        self.returncode = returncode
        self.command = command
        self.error_msg = error_msg
        message = (f'Command {command} failed with return code {returncode}.'
                   f'\n{error_msg}')
        super().__init__(message)


class ClusterNotUpError(Exception):
    """Raised when a cluster is not up."""
    pass


class ClusterSetUpError(Exception):
    """Raised when a cluster has setup error."""
    pass


class NotSupportedError(Exception):
    """Raised when a feature is not supported."""
    pass


class StorageError(Exception):
    pass


class StorageSpecError(ValueError):
    # Errors raised due to invalid specification of the Storage object
    pass


class StorageInitError(StorageError):
    # Error raised when Initialization fails - either due to permissions,
    # unavailable name, or other reasons.
    pass


class StorageBucketCreateError(StorageInitError):
    # Error raised when bucket creation fails.
    pass


class StorageBucketGetError(StorageInitError):
    # Error raised if attempt to fetch an existing bucket fails.
    pass


class StorageBucketDeleteError(StorageError):
    # Error raised if attempt to delete an existing bucket fails.
    pass


class StorageUploadError(StorageError):
    # Error raised when bucket is successfully initialized, but upload fails,
    # either due to permissions, ctrl-c, or other reasons.
    pass


class StorageSourceError(StorageSpecError):
    # Error raised when the source of the storage is invalid. E.g., does not
    # exist, malformed path, or other reasons.
    pass


class StorageNameError(StorageSpecError):
    # Error raised when the source of the storage is invalid. E.g., does not
    # exist, malformed path, or other reasons.
    pass


class StorageModeError(StorageSpecError):
    # Error raised when the storage mode is invalid or does not support the
    # requested operation (e.g., passing a file as source to MOUNT mode)
    pass


class FetchIPError(Exception):
    """Raised when fetching the IP fails."""

    class Reason(enum.Enum):
        HEAD = 'HEAD'
        WORKER = 'WORKER'

    def __init__(self, reason: Reason) -> None:
        super().__init__()
        self.reason = reason


class NetworkError(Exception):
    """Raised when network fails."""
    pass


class ClusterStatusFetchingError(Exception):
    """Raised when fetching the cluster status fails."""
    pass


class SpotUserCancelledError(Exception):
    """Raised when a spot user cancels the job."""
    pass


class InvalidClusterNameError(Exception):
    """Raised when the cluster name is invalid."""
    pass


class CloudUserIdentityError(Exception):
    """Raised when the cloud identity is invalid."""
    pass


class ClusterOwnerIdentityMismatchError(Exception):
    """The cluster's owner identity does not match the current user identity."""
    pass
