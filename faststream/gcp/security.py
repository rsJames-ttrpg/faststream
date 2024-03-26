from typing import Optional

from google.auth.credentials import Credentials
from google.auth.transport.requests import Request

from faststream.security import BaseSecurity, SASLPlaintext
from faststream.types import AnyDict


def parse_security(security: Optional[BaseSecurity]) -> AnyDict:
    if security is None:
        return {}
    elif isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    elif isinstance(security, BaseSecurity):
        return _parse_base_security(security)
    else:
        raise NotImplementedError(f"PubSubBroker does not support {type(security)}")

def _parse_base_security(security: BaseSecurity) -> AnyDict:
    if security.use_ssl:
        class TokenCredentials(Credentials):
            def __init__(
                self,
                _security: BaseSecurity = security,
            ) -> None:
                self._security = _security

            def refresh(self, request: Optional[Request] = None) -> None:
                # Implement token refresh logic if needed
                pass

            def apply(self, headers: dict, token: Optional[str] = None) -> None:
                headers["Authorization"] = f"Bearer {token}"

        return {"credentials": TokenCredentials()}
    else:
        return {}

def _parse_sasl_plaintext(security: SASLPlaintext) -> AnyDict:
    return {
        **_parse_base_security(security),
        "credentials": security.credentials,
    }
