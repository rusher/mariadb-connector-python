from enum import Enum


class SslMode(Enum):
    DISABLE = "disable"
    TRUST = "trust"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"
