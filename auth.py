"""
User Management and Authentication System.

Provides:
- User storage with password hashing
- Session management
- Admin verification
- User provisioning by admins

This is designed for a classroom setting where:
1. An admin creates all user accounts with passwords
2. Admin distributes credentials to students
3. Students login with their credentials
4. Only admins can access admin APIs
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path
import threading


# ─────────────────────────────────────────────────────────────────────────────
# Password Hashing
# ─────────────────────────────────────────────────────────────────────────────

def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    """
    Hash a password with salt using PBKDF2.
    
    Returns:
        (password_hash, salt) both as hex strings
    """
    if salt is None:
        salt = secrets.token_bytes(32)
    
    # Use PBKDF2 with SHA256, 100k iterations
    key = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt,
        iterations=100_000,
    )
    
    return key.hex(), salt.hex()


def verify_password(password: str, password_hash: str, salt: str) -> bool:
    """Verify a password against stored hash and salt."""
    try:
        expected_key = bytes.fromhex(password_hash)
        salt_bytes = bytes.fromhex(salt)
        
        actual_key = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt_bytes,
            iterations=100_000,
        )
        
        return hmac.compare_digest(actual_key, expected_key)
    except (ValueError, TypeError):
        return False


# ─────────────────────────────────────────────────────────────────────────────
# User Model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class User:
    """A registered user."""
    user_id: str
    password_hash: str
    password_salt: str
    display_name: str
    is_admin: bool = False
    is_active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_login: datetime | None = None
    
    # Optional metadata
    email: str | None = None
    team: str | None = None  # For team-based games
    
    def to_dict(self, include_sensitive: bool = False) -> dict:
        """Convert to dictionary."""
        d = {
            "user_id": self.user_id,
            "display_name": self.display_name,
            "is_admin": self.is_admin,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat(),
            "last_login": self.last_login.isoformat() if self.last_login else None,
            "email": self.email,
            "team": self.team,
        }
        if include_sensitive:
            d["password_hash"] = self.password_hash
            d["password_salt"] = self.password_salt
        return d
    
    @classmethod
    def from_dict(cls, data: dict) -> "User":
        """Create from dictionary."""
        return cls(
            user_id=data["user_id"],
            password_hash=data["password_hash"],
            password_salt=data["password_salt"],
            display_name=data["display_name"],
            is_admin=data.get("is_admin", False),
            is_active=data.get("is_active", True),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(timezone.utc),
            last_login=datetime.fromisoformat(data["last_login"]) if data.get("last_login") else None,
            email=data.get("email"),
            team=data.get("team"),
        )


# ─────────────────────────────────────────────────────────────────────────────
# User Store
# ─────────────────────────────────────────────────────────────────────────────

class UserStore:
    """
    Storage for user accounts.
    
    Supports file-based persistence for simplicity.
    Thread-safe for concurrent access.
    """
    
    def __init__(self, storage_path: str | Path | None = None):
        """
        Initialize user store.
        
        Args:
            storage_path: Path to JSON file for persistence. None for in-memory only.
        """
        self._users: dict[str, User] = {}
        self._lock = threading.RLock()
        self._storage_path = Path(storage_path) if storage_path else None
        
        if self._storage_path and self._storage_path.exists():
            self._load()
    
    def _load(self) -> None:
        """Load users from storage."""
        try:
            with open(self._storage_path, 'r') as f:
                data = json.load(f)
            
            for user_data in data.get("users", []):
                user = User.from_dict(user_data)
                self._users[user.user_id] = user
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load users: {e}")
    
    def _save(self) -> None:
        """Save users to storage."""
        if not self._storage_path:
            return
        
        data = {
            "users": [u.to_dict(include_sensitive=True) for u in self._users.values()],
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # Write atomically
        temp_path = self._storage_path.with_suffix('.tmp')
        with open(temp_path, 'w') as f:
            json.dump(data, f, indent=2)
        temp_path.replace(self._storage_path)
    
    def create_user(
        self,
        user_id: str,
        password: str,
        display_name: str | None = None,
        is_admin: bool = False,
        email: str | None = None,
        team: str | None = None,
    ) -> User:
        """
        Create a new user.
        
        Raises:
            ValueError: If user already exists or validation fails
        """
        with self._lock:
            # Validate user_id
            if not user_id or not user_id.strip():
                raise ValueError("user_id cannot be empty")
            if len(user_id) > 32:
                raise ValueError("user_id cannot exceed 32 characters")
            if not user_id.replace("_", "").replace("-", "").isalnum():
                raise ValueError("user_id can only contain alphanumeric, underscore, and hyphen")
            
            # Check for duplicates
            if user_id in self._users:
                raise ValueError(f"User {user_id} already exists")
            
            # Validate password
            if not password or len(password) < 4:
                raise ValueError("Password must be at least 4 characters")
            
            # Hash password
            password_hash, password_salt = hash_password(password)
            
            user = User(
                user_id=user_id,
                password_hash=password_hash,
                password_salt=password_salt,
                display_name=display_name or user_id,
                is_admin=is_admin,
                email=email,
                team=team,
            )
            
            self._users[user_id] = user
            self._save()
            
            return user
    
    def get_user(self, user_id: str) -> User | None:
        """Get a user by ID."""
        with self._lock:
            return self._users.get(user_id)
    
    def authenticate(self, user_id: str, password: str) -> User | None:
        """
        Authenticate a user.
        
        Returns the user if authentication succeeds, None otherwise.
        """
        with self._lock:
            user = self._users.get(user_id)
            
            if not user:
                # Timing attack mitigation - do a fake hash
                hash_password(password)
                return None
            
            if not user.is_active:
                return None
            
            if verify_password(password, user.password_hash, user.password_salt):
                user.last_login = datetime.now(timezone.utc)
                self._save()
                return user
            
            return None
    
    def update_password(self, user_id: str, new_password: str) -> bool:
        """Update a user's password."""
        with self._lock:
            user = self._users.get(user_id)
            if not user:
                return False
            
            if len(new_password) < 4:
                raise ValueError("Password must be at least 4 characters")
            
            user.password_hash, user.password_salt = hash_password(new_password)
            self._save()
            return True
    
    def set_active(self, user_id: str, is_active: bool) -> bool:
        """Enable or disable a user account."""
        with self._lock:
            user = self._users.get(user_id)
            if not user:
                return False
            
            user.is_active = is_active
            self._save()
            return True
    
    def set_admin(self, user_id: str, is_admin: bool) -> bool:
        """Set admin status for a user."""
        with self._lock:
            user = self._users.get(user_id)
            if not user:
                return False
            
            user.is_admin = is_admin
            self._save()
            return True
    
    def delete_user(self, user_id: str) -> bool:
        """Delete a user account."""
        with self._lock:
            if user_id in self._users:
                del self._users[user_id]
                self._save()
                return True
            return False
    
    def list_users(self, include_inactive: bool = False) -> list[User]:
        """List all users."""
        with self._lock:
            users = list(self._users.values())
            if not include_inactive:
                users = [u for u in users if u.is_active]
            return users
    
    def get_admins(self) -> list[User]:
        """Get all admin users."""
        with self._lock:
            return [u for u in self._users.values() if u.is_admin and u.is_active]
    
    def bulk_create_users(self, users_data: list[dict]) -> list[tuple[str, str | None]]:
        """
        Create multiple users at once.
        
        Args:
            users_data: List of dicts with keys: user_id, password, display_name (optional)
        
        Returns:
            List of (user_id, error_message) tuples. error_message is None if successful.
        """
        results = []
        
        for data in users_data:
            try:
                self.create_user(
                    user_id=data["user_id"],
                    password=data["password"],
                    display_name=data.get("display_name"),
                    email=data.get("email"),
                    team=data.get("team"),
                )
                results.append((data["user_id"], None))
            except (ValueError, KeyError) as e:
                results.append((data.get("user_id", "unknown"), str(e)))
        
        return results
    
    def export_credentials(self) -> list[dict]:
        """
        Export user credentials for distribution.
        
        Note: This returns password hashes, not plaintext passwords.
        For distributing credentials, you should track passwords separately
        or regenerate them.
        """
        with self._lock:
            return [
                {
                    "user_id": u.user_id,
                    "display_name": u.display_name,
                    "is_admin": u.is_admin,
                    "team": u.team,
                }
                for u in self._users.values()
                if u.is_active
            ]
    
    def user_count(self) -> int:
        """Get total user count."""
        return len(self._users)
    
    def admin_count(self) -> int:
        """Get admin count."""
        return len([u for u in self._users.values() if u.is_admin])


# ─────────────────────────────────────────────────────────────────────────────
# Session Management (Enhanced)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Session:
    """An authenticated user session."""
    session_id: str
    user_id: str
    user: User
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime = field(default=None)
    ip_address: str = ""
    user_agent: str = ""
    
    def __post_init__(self):
        if self.expires_at is None:
            self.expires_at = self.created_at + timedelta(hours=24)
    
    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at
    
    @property
    def is_admin(self) -> bool:
        return self.user.is_admin
    
    def touch(self) -> None:
        """Update last active time."""
        self.last_active = datetime.now(timezone.utc)
    
    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "is_admin": self.is_admin,
            "display_name": self.user.display_name,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }


class SessionManager:
    """
    Manages authenticated sessions.
    
    Thread-safe for concurrent access.
    """
    
    def __init__(self, user_store: UserStore, session_ttl_hours: int = 24):
        self._user_store = user_store
        self._sessions: dict[str, Session] = {}
        self._user_sessions: dict[str, str] = {}  # user_id -> session_id
        self._lock = threading.RLock()
        self._session_ttl = timedelta(hours=session_ttl_hours)
        
        # Banned users (separate from inactive)
        self._banned_users: set[str] = set()
        self._ban_reasons: dict[str, str] = {}
    
    def login(
        self,
        user_id: str,
        password: str,
        ip_address: str = "",
        user_agent: str = "",
    ) -> Session | tuple[None, str]:
        """
        Authenticate and create session.
        
        Returns:
            Session on success, or (None, error_message) on failure.
        """
        with self._lock:
            # Check if banned
            if user_id in self._banned_users:
                return None, f"User is banned: {self._ban_reasons.get(user_id, 'No reason given')}"
            
            # Authenticate
            user = self._user_store.authenticate(user_id, password)
            if not user:
                return None, "Invalid credentials"
            
            # Invalidate existing session
            if user_id in self._user_sessions:
                old_session_id = self._user_sessions[user_id]
                self._sessions.pop(old_session_id, None)
            
            # Create new session
            session_id = secrets.token_urlsafe(32)
            session = Session(
                session_id=session_id,
                user_id=user_id,
                user=user,
                ip_address=ip_address,
                user_agent=user_agent,
                expires_at=datetime.now(timezone.utc) + self._session_ttl,
            )
            
            self._sessions[session_id] = session
            self._user_sessions[user_id] = session_id
            
            return session
    
    def get_session(self, session_id: str) -> Session | None:
        """Get session by ID, checking expiration."""
        with self._lock:
            session = self._sessions.get(session_id)
            
            if not session:
                return None
            
            if session.is_expired:
                self._invalidate_session(session_id)
                return None
            
            # Check if user is now banned
            if session.user_id in self._banned_users:
                self._invalidate_session(session_id)
                return None
            
            session.touch()
            return session
    
    def get_session_by_user(self, user_id: str) -> Session | None:
        """Get session by user ID."""
        with self._lock:
            session_id = self._user_sessions.get(user_id)
            if session_id:
                return self.get_session(session_id)
            return None
    
    def logout(self, session_id: str) -> bool:
        """End a session."""
        with self._lock:
            return self._invalidate_session(session_id)
    
    def _invalidate_session(self, session_id: str) -> bool:
        """Internal session invalidation."""
        session = self._sessions.pop(session_id, None)
        if session:
            self._user_sessions.pop(session.user_id, None)
            return True
        return False
    
    def ban_user(self, user_id: str, reason: str = "") -> bool:
        """Ban a user and invalidate their session."""
        with self._lock:
            self._banned_users.add(user_id)
            self._ban_reasons[user_id] = reason
            
            # Invalidate session
            session_id = self._user_sessions.get(user_id)
            if session_id:
                self._invalidate_session(session_id)
            
            return True
    
    def unban_user(self, user_id: str) -> bool:
        """Unban a user."""
        with self._lock:
            self._banned_users.discard(user_id)
            self._ban_reasons.pop(user_id, None)
            return True
    
    def is_banned(self, user_id: str) -> bool:
        """Check if user is banned."""
        return user_id in self._banned_users
    
    def get_banned_users(self) -> list[dict]:
        """Get list of banned users."""
        with self._lock:
            return [
                {"user_id": uid, "reason": self._ban_reasons.get(uid, "")}
                for uid in self._banned_users
            ]
    
    def cleanup_expired(self) -> int:
        """Remove expired sessions. Returns count removed."""
        with self._lock:
            now = datetime.now(timezone.utc)
            expired = [
                sid for sid, s in self._sessions.items()
                if s.expires_at < now
            ]
            
            for sid in expired:
                self._invalidate_session(sid)
            
            return len(expired)
    
    def get_all_sessions(self) -> list[Session]:
        """Get all active sessions."""
        with self._lock:
            return list(self._sessions.values())
    
    def get_active_user_count(self) -> int:
        """Get count of users with active sessions."""
        return len(self._sessions)
    
    def require_admin(self, session: Session | None) -> bool:
        """Check if session belongs to an admin."""
        return session is not None and session.is_admin


# ─────────────────────────────────────────────────────────────────────────────
# Factory Functions
# ─────────────────────────────────────────────────────────────────────────────

def create_auth_system(
    storage_path: str | Path | None = None,
    create_default_admin: bool = True,
    default_admin_password: str = "admin123",
) -> tuple[UserStore, SessionManager]:
    """
    Create and initialize the authentication system.
    
    Args:
        storage_path: Path to user storage file
        create_default_admin: Whether to create a default admin user
        default_admin_password: Password for default admin
    
    Returns:
        (user_store, session_manager)
    """
    user_store = UserStore(storage_path)
    session_manager = SessionManager(user_store)
    
    # Create default admin if needed
    if create_default_admin and user_store.admin_count() == 0:
        try:
            user_store.create_user(
                user_id="admin",
                password=default_admin_password,
                display_name="Administrator",
                is_admin=True,
            )
            print(f"Created default admin user: admin / {default_admin_password}")
        except ValueError:
            pass  # Already exists
    
    return user_store, session_manager


# ─────────────────────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Test the auth system
    print("Testing Authentication System")
    print("=" * 60)
    
    user_store, sessions = create_auth_system(create_default_admin=True)
    
    # Test user creation
    print("\n1. Creating users...")
    user_store.create_user("student1", "pass1234", "Alice")
    user_store.create_user("student2", "pass5678", "Bob")
    print(f"   Created {user_store.user_count()} users")
    
    # Test authentication
    print("\n2. Testing authentication...")
    result = sessions.login("student1", "pass1234")
    if isinstance(result, Session):
        print(f"   ✓ Login successful: {result.user.display_name}")
    
    result = sessions.login("student1", "wrongpass")
    if result[0] is None:
        print(f"   ✓ Wrong password rejected: {result[1]}")
    
    # Test admin login
    print("\n3. Testing admin...")
    result = sessions.login("admin", "admin123")
    if isinstance(result, Session):
        print(f"   ✓ Admin login: is_admin={result.is_admin}")
    
    # Test banning
    print("\n4. Testing ban...")
    sessions.ban_user("student2", "Testing ban functionality")
    result = sessions.login("student2", "pass5678")
    if result[0] is None:
        print(f"   ✓ Banned user rejected: {result[1]}")
    
    sessions.unban_user("student2")
    result = sessions.login("student2", "pass5678")
    if isinstance(result, Session):
        print(f"   ✓ Unbanned user can login")
    
    # Test bulk creation
    print("\n5. Testing bulk creation...")
    results = user_store.bulk_create_users([
        {"user_id": "team1_player1", "password": "abc123", "team": "Team Alpha"},
        {"user_id": "team1_player2", "password": "abc123", "team": "Team Alpha"},
        {"user_id": "team2_player1", "password": "xyz789", "team": "Team Beta"},
    ])
    success = sum(1 for _, err in results if err is None)
    print(f"   Created {success}/{len(results)} users")
    
    print("\n" + "=" * 60)
    print("All tests passed!")
