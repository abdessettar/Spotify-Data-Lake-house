from typing import Dict, Any, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, model_validator
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.blob import BlobServiceClient


class Settings(BaseSettings):
    """
    Application Configuration.
    Reads from environment variables or .env file.

    Three environment modes:
      DEV   — Azurite emulator through Docker (for development/testing)
      LOCAL — Real Azure Storage via `az login` (we can schedule this instead of giving more money to Microslop)
      PROD  — Real Azure Storage via managed identity (Container Apps)
    """

    # extra="ignore" to have extra env vars in .env without Pydantic raising errors
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Environment ---
    ENVIRONMENT: str = Field(default="LOCAL", description="DEV, LOCAL, or PROD")

    # --- Spotify (loaded from .env locally, from Key Vault in PROD) ---
    SPOTIFY_CLIENT_ID: Optional[str] = None
    SPOTIFY_CLIENT_SECRET: Optional[str] = None
    SPOTIFY_REFRESH_TOKEN: Optional[str] = None

    # --- Last.fm (loaded from .env locally) ---
    # Noc loud : this enrichment runs locally only considering how long it takes.
    LASTFM_API_KEY: Optional[str] = None

    # --- Azure Storage ---
    AZURE_STORAGE_ACCOUNT: str
    DATA_CONTAINER: str = "spotify-data"
    AZURITE_HOST: str = "127.0.0.1"

    # --- Azure Identity (Production) ---
    AZURE_CLIENT_ID: Optional[str] = None
    KEY_VAULT_URI: Optional[str] = None

    @model_validator(mode="after")
    def _load_secrets_from_key_vault(self) -> "Settings":
        """In PROD, fetch Spotify credentials from Key Vault if not already set."""
        if self.ENVIRONMENT == "PROD" and self.KEY_VAULT_URI:
            missing = [
                f
                for f in (
                    "SPOTIFY_CLIENT_ID",
                    "SPOTIFY_CLIENT_SECRET",
                    "SPOTIFY_REFRESH_TOKEN",
                )
                if getattr(self, f) is None
            ]
            if missing:
                print(
                    f"Loading Spotify secrets from Key Vault ({self.KEY_VAULT_URI})..."
                )
                from azure.keyvault.secrets import SecretClient

                credential = DefaultAzureCredential()
                client = SecretClient(
                    vault_url=self.KEY_VAULT_URI, credential=credential
                )
                # Key Vault secret names use hyphens; env vars use underscores
                for field_name in missing:
                    secret_name = field_name.replace("_", "-")
                    value = client.get_secret(secret_name).value
                    # Pydantic models are quasi-immutable; normal assignment is blocked
                    # after validation, so we bypass it via object.__setattr__
                    object.__setattr__(self, field_name, value)
        # Validate that Spotify creds are available (from .env or Key Vault)
        for f in (
            "SPOTIFY_CLIENT_ID",
            "SPOTIFY_CLIENT_SECRET",
            "SPOTIFY_REFRESH_TOKEN",
        ):
            if getattr(self, f) is None:
                raise ValueError(
                    f"{f} is required. Set it in .env (LOCAL/DEV) or Key Vault (PROD)."
                )
        return self

    @property
    def _is_dev(self) -> bool:
        return self.ENVIRONMENT == "DEV"

    # --- COMPUTED PROPERTIES ---

    @property
    def _AZURITE_CONN_STR(self) -> str:
        """Azurite connection string (DEV mode only)."""
        return (
            "DefaultEndpointsProtocol=http;"
            f"AccountName={self.AZURE_STORAGE_ACCOUNT};"
            # This is the Azurite dev storage key published Microslop
            # https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            f"BlobEndpoint=http://{self.AZURITE_HOST}:10000/{self.AZURE_STORAGE_ACCOUNT};"
        )

    @property
    def STORAGE_ACCOUNT_URL(self) -> str:
        """Storage endpoint URL."""
        if self._is_dev:
            return f"http://{self.AZURITE_HOST}:10000/{self.AZURE_STORAGE_ACCOUNT}"
        return f"https://{self.AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"

    def get_blob_service_client(self) -> BlobServiceClient:
        """Returns the low-level Azure Blob Client."""
        if self._is_dev:
            print("Connecting to Local Azurite (BlobServiceClient)...")
            return BlobServiceClient.from_connection_string(self._AZURITE_CONN_STR)

        print(
            f"Connecting to Azure Storage: {self.AZURE_STORAGE_ACCOUNT} ({self.ENVIRONMENT})..."
        )
        # LOCAL uses AzureCliCredential (`az login` locally).
        # PROD uses DefaultAzureCredential (auto-discovers managed identity).
        credential = (
            AzureCliCredential()
            if self.ENVIRONMENT == "LOCAL"
            else DefaultAzureCredential()
        )
        return BlobServiceClient(
            account_url=self.STORAGE_ACCOUNT_URL, credential=credential
        )

    def _azure_credential(self):
        """Returns the appropriate Azure credential for the current environment."""
        return (
            AzureCliCredential()
            if self.ENVIRONMENT == "LOCAL"
            else DefaultAzureCredential()
        )

    @property
    def fsspec_storage_options(self) -> Dict[str, Any]:
        """Returns options for fsspec.open() / adlfs. Credential must be an object."""
        if self._is_dev:
            return {
                "connection_string": self._AZURITE_CONN_STR,
                "account_name": self.AZURE_STORAGE_ACCOUNT,
                "use_emulator": "true",
            }
        return {
            "account_name": self.AZURE_STORAGE_ACCOUNT,
            "credential": self._azure_credential(),
        }

    @property
    def polars_storage_options(self) -> Dict[str, Any]:
        """Returns options for pl.scan_parquet() / Polars object_store. Token must be a string."""
        if self._is_dev:
            return {
                "connection_string": self._AZURITE_CONN_STR,
                "account_name": self.AZURE_STORAGE_ACCOUNT,
                "use_emulator": "true",
            }
        # Polars' object_store backend can't use Python credential objects, so we
        # call .get_token() to obtain a bearer token string it can pass in HTTP headers
        token = (
            self._azure_credential()
            .get_token("https://storage.azure.com/.default")
            .token
        )
        return {
            "account_name": self.AZURE_STORAGE_ACCOUNT,
            "bearer_token": token,
        }


# Singleton instance
settings = Settings() 
