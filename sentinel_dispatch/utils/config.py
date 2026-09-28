"""Configuration loader for Sentinel Dispatch."""
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import dotenv_values


class Config:
    """Configuration manager for development and production modes."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration.

        Args:
            config_path: Path to config file. If None, looks for config/config.{mode}.json
                         where mode is read from SENTINEL_MODE environment variable.
        """
        # Load .env file with variable expansion support
        self._load_env_file()

        if config_path is None:
            # Read mode from environment variable only
            mode = os.getenv("SENTINEL_MODE", "development")
            if mode not in ["development", "production"]:
                raise ValueError(f"Invalid SENTINEL_MODE: {mode}. Must be 'development' or 'production'")
            config_path = f"config/config.{mode}.json"

        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self.load()

    def _load_env_file(self) -> None:
        """Load .env file from project root with variable expansion."""
        # Find project root (where .env file should be)
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent  # Go up from utils/ to project root

        env_file = project_root / ".env"
        if not env_file.exists():
            return

        # Load .env file values
        env_vars = dotenv_values(env_file)

        # Expand variables in multiple passes to handle dependencies
        # e.g., VAR1=${VAR2}, VAR2=value
        import re
        expanded_vars = {}

        # First pass: load all non-expanding values
        for key, value in env_vars.items():
            if value and isinstance(value, str) and "${" not in value:
                expanded_vars[key] = value
                if key not in os.environ:
                    os.environ[key] = value

        # Second pass: expand variables that reference already-loaded values
        max_iterations = 10  # Prevent infinite loops
        for _ in range(max_iterations):
            for key, value in env_vars.items():
                if key in expanded_vars:
                    continue  # Already expanded

                if value and isinstance(value, str) and "${" in value:
                    # Expand ${VAR} references
                    def expand_var(match):
                        var_name = match.group(1)
                        # Check both expanded_vars and os.environ
                        expanded_value = expanded_vars.get(var_name) or os.environ.get(var_name)
                        return expanded_value if expanded_value else match.group(0)

                    expanded = re.sub(r'\$\{(\w+)\}', expand_var, value)
                    if expanded != value:
                        expanded_vars[key] = expanded
                        if key not in os.environ:
                            os.environ[key] = expanded
                elif value is not None:
                    expanded_vars[key] = value
                    if key not in os.environ:
                        os.environ[key] = value

        # Final pass: set any remaining unexpanded variables
        for key, value in env_vars.items():
            if key not in expanded_vars and value is not None:
                expanded_vars[key] = value
                if key not in os.environ:
                    os.environ[key] = value

    def load(self) -> None:
        """Load configuration from file and merge with environment variables."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}. "
                f"Please copy config.example.json to {self.config_path} and update with your settings."
            )

        with open(self.config_path, "r") as f:
            self._config = json.load(f)

        # Override API keys from environment variables (never store in JSON)
        self._merge_env_vars()

    @property
    def mode(self) -> str:
        """Get current mode (development or production)."""
        return self._config.get("mode", "development")

    @property
    def kafka(self) -> Dict[str, Any]:
        """Get Kafka configuration."""
        if self.mode == "development":
            return self._config.get("kafka", {})
        else:
            return self._config.get("confluent", {})

    def _merge_env_vars(self) -> None:
        """Merge API keys from environment variables into config."""
        # Gemini API key
        if "gemini" not in self._config:
            self._config["gemini"] = {}
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if gemini_api_key:
            self._config["gemini"]["api_key"] = gemini_api_key

        # Google Maps API key
        if "google_maps" not in self._config:
            self._config["google_maps"] = {}
        google_maps_key = os.getenv("GOOGLE_MAPS_API_KEY")
        if google_maps_key:
            self._config["google_maps"]["api_key"] = google_maps_key

        # FIRMS API key (optional)
        if "apis" not in self._config:
            self._config["apis"] = {}
        if "firms" not in self._config["apis"]:
            self._config["apis"]["firms"] = {}
        firms_api_key = os.getenv("FIRMS_API_KEY")
        if firms_api_key:
            self._config["apis"]["firms"]["api_key"] = firms_api_key

        # Confluent Cloud credentials (for production)
        if "confluent" not in self._config:
            self._config["confluent"] = {}
        confluent_api_key = os.getenv("CONFLUENT_API_KEY")
        confluent_api_secret = os.getenv("CONFLUENT_API_SECRET")
        if confluent_api_key:
            self._config["confluent"]["api_key"] = confluent_api_key
        if confluent_api_secret:
            self._config["confluent"]["api_secret"] = confluent_api_secret

    @property
    def gemini(self) -> Dict[str, Any]:
        """Get Gemini API configuration."""
        return self._config.get("gemini", {})

    @property
    def apis(self) -> Dict[str, Any]:
        """Get external API configurations."""
        return self._config.get("apis", {})

    @property
    def google_maps(self) -> Dict[str, Any]:
        """Get Google Maps API configuration."""
        return self._config.get("google_maps", {})

    @property
    def location(self) -> Dict[str, Any]:
        """Get location event configuration."""
        return self._config.get("location", {})

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return self._config.get(key, default)


def load_config() -> Config:
    """
    Load configuration using SENTINEL_MODE environment variable.

    Returns:
        Config instance
    """
    return Config()

