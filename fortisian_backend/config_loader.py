"""
Configuration loader for backend server.
Loads configuration from JSON files in the config folder.
"""

from __future__ import annotations

import json
from pathlib import Path
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

# Default config path
CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class ServerConfigData:
    """Server configuration from JSON."""
    host: str = "0.0.0.0"
    port: int = 8080
    session_max_age: int = 86400
    allowed_origins: list[str] = field(default_factory=lambda: ["*"])
    require_origin: bool = False


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    orders_per_second: float = 2.0
    orders_per_minute: int = 30
    burst: int = 5


@dataclass
class AnomalyConfig:
    """Anomaly detection configuration."""
    window_size: int = 20
    timing_threshold: float = 0.05


@dataclass
class BookCacheConfigData:
    """Book cache configuration."""
    requests_per_second: float = 10.0
    requests_burst: int = 20
    max_depth: int = 50
    snapshot_interval: float = 5.0


@dataclass
class MarketConfigData:
    """Market configuration from JSON."""
    market_id: str
    title: str
    description: str = ""
    tick_size: str = "0.01"
    lot_size: int = 1
    max_position: int | None = None


class ConfigLoader:
    """Loads configuration from JSON files."""
    
    def __init__(self, config_dir: Path | None = None):
        self.config_dir = config_dir or CONFIG_DIR
        self._backend_config: dict[str, Any] | None = None
    
    def load_backend_config(self) -> dict[str, Any]:
        """Load backend configuration from JSON."""
        if self._backend_config is not None:
            return self._backend_config
        
        config_path = self.config_dir / "backend.json"
        
        if not config_path.exists():
            # Return defaults
            return {
                "server": {
                    "host": "0.0.0.0",
                    "port": 8080,
                    "session_max_age": 86400,
                    "allowed_origins": ["*"],
                    "require_origin": False,
                },
                "rate_limiting": {
                    "orders_per_second": 2.0,
                    "orders_per_minute": 30,
                    "burst": 5,
                },
                "anomaly_detection": {
                    "window_size": 20,
                    "timing_threshold": 0.05,
                },
                "book_cache": {
                    "requests_per_second": 10.0,
                    "requests_burst": 20,
                    "max_depth": 50,
                    "snapshot_interval": 5.0,
                },
                "markets": [],
            }
        
        with open(config_path, 'r') as f:
            self._backend_config = json.load(f)
        
        return self._backend_config
    
    def get_server_config(self) -> ServerConfigData:
        """Get server configuration."""
        config = self.load_backend_config()
        server = config.get("server", {})
        return ServerConfigData(
            host=server.get("host", "0.0.0.0"),
            port=server.get("port", 8080),
            session_max_age=server.get("session_max_age", 86400),
            allowed_origins=server.get("allowed_origins", ["*"]),
            require_origin=server.get("require_origin", False),
        )
    
    def get_rate_limit_config(self) -> RateLimitConfig:
        """Get rate limiting configuration."""
        config = self.load_backend_config()
        rl = config.get("rate_limiting", {})
        return RateLimitConfig(
            orders_per_second=rl.get("orders_per_second", 2.0),
            orders_per_minute=rl.get("orders_per_minute", 30),
            burst=rl.get("burst", 5),
        )
    
    def get_anomaly_config(self) -> AnomalyConfig:
        """Get anomaly detection configuration."""
        config = self.load_backend_config()
        ad = config.get("anomaly_detection", {})
        return AnomalyConfig(
            window_size=ad.get("window_size", 20),
            timing_threshold=ad.get("timing_threshold", 0.05),
        )
    
    def get_book_cache_config(self) -> BookCacheConfigData:
        """Get book cache configuration."""
        config = self.load_backend_config()
        bc = config.get("book_cache", {})
        return BookCacheConfigData(
            requests_per_second=bc.get("requests_per_second", 10.0),
            requests_burst=bc.get("requests_burst", 20),
            max_depth=bc.get("max_depth", 50),
            snapshot_interval=bc.get("snapshot_interval", 5.0),
        )
    
    def get_markets_config(self) -> list[MarketConfigData]:
        """Get markets configuration."""
        config = self.load_backend_config()
        markets = config.get("markets", [])
        return [
            MarketConfigData(
                market_id=m.get("market_id", ""),
                title=m.get("title", ""),
                description=m.get("description", ""),
                tick_size=m.get("tick_size", "0.01"),
                lot_size=m.get("lot_size", 1),
                max_position=m.get("max_position"),
            )
            for m in markets
        ]


# Global instance
_config_loader: ConfigLoader | None = None


def get_config_loader() -> ConfigLoader:
    """Get the global config loader instance."""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
    return _config_loader

