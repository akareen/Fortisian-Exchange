#!/usr/bin/env python3
"""
Fortisian Exchange - User Provisioning Script

This script creates user accounts for the trading exchange.
Reads from user_config.json and writes to users.json (used by server).

Usage:
    python create_users.py [--config path/to/config.json] [--output path/to/users.json]

The script will:
1. Read admin and user accounts from config JSON
2. Create all accounts in the users.json file
3. Print credentials for distribution
"""

import sys
import os
import json
import argparse

# Add exchange directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

try:
    from auth import create_auth_system, UserStore
except ImportError:
    print("ERROR: Could not import auth module.")
    print("Make sure you're running this from the exchange directory.")
    print("The auth.py file should be in the same directory as this script.")
    sys.exit(1)


def load_config(config_path: str) -> dict:
    """Load user configuration from JSON file."""
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        print()
        print("Expected JSON format:")
        print(json.dumps({
            "admins": [
                {"user_id": "trader_test_1", "password": "pass123", "display_name": "Trader Test 1"}
            ],
            "users": [
                {"user_id": "user_test_1", "password": "pass123", "display_name": "User Test 1", "team": "Team Alpha"}
            ]
        }, indent=2))
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate structure
    if "admins" not in config:
        config["admins"] = []
    if "users" not in config:
        config["users"] = []
    
    return config


def main():
    parser = argparse.ArgumentParser(description="Provision users for Fortisian Exchange")
    parser.add_argument(
        "--config", "-c",
        default="user_config.json",
        help="Path to user config JSON file (default: user_config.json)"
    )
    parser.add_argument(
        "--output", "-o",
        default="users.json",
        help="Path to output users.json file (default: users.json)"
    )
    args = parser.parse_args()
    
    print("=" * 70)
    print("FORTISIAN EXCHANGE - USER PROVISIONING")
    print("=" * 70)
    print()
    
    # Load configuration
    print(f"Loading config from: {args.config}")
    config = load_config(args.config)
    
    admin_count = len(config.get("admins", []))
    user_count = len(config.get("users", []))
    print(f"Found {admin_count} admin(s) and {user_count} user(s) in config")
    print()
    
    # Initialize auth system (creates empty users.json if needed)
    print(f"Initializing auth system (output: {args.output})...")
    user_store, session_manager = create_auth_system(
        storage_path=args.output,
        create_default_admin=False,  # We'll create admins from config
    )
    
    created_admins = []
    created_users = []
    skipped = []
    
    # Create admin accounts
    print()
    print("-" * 70)
    print("CREATING ADMIN ACCOUNTS")
    print("-" * 70)
    
    for admin in config.get("admins", []):
        user_id = admin.get("user_id")
        password = admin.get("password")
        display_name = admin.get("display_name", user_id)
        
        if not user_id or not password:
            print(f"  ✗ Invalid admin entry (missing user_id or password): {admin}")
            continue
        
        try:
            user_store.create_user(
                user_id=user_id,
                password=password,
                display_name=display_name,
                is_admin=True,
            )
            created_admins.append((user_id, password, display_name))
            print(f"  ✓ Created admin: {user_id}")
        except ValueError as e:
            skipped.append((user_id, str(e)))
            print(f"  - Skipped: {user_id} ({e})")
    
    # Create regular user accounts
    print()
    print("-" * 70)
    print("CREATING USER ACCOUNTS")
    print("-" * 70)
    
    for user in config.get("users", []):
        user_id = user.get("user_id")
        password = user.get("password")
        display_name = user.get("display_name", user_id)
        team = user.get("team")  # Can be None
        
        if not user_id or not password:
            print(f"  ✗ Invalid user entry (missing user_id or password): {user}")
            continue
        
        try:
            user_store.create_user(
                user_id=user_id,
                password=password,
                display_name=display_name,
                is_admin=False,
                team=team,
            )
            created_users.append((user_id, password, display_name, team))
            print(f"  ✓ Created user: {user_id}")
        except ValueError as e:
            skipped.append((user_id, str(e)))
            print(f"  - Skipped: {user_id} ({e})")
    
    # Print credential summary
    print()
    print("-" * 70)
    print("CREDENTIAL SUMMARY (for distribution)")
    print("-" * 70)
    print()
    
    # Admins table
    if created_admins:
        print("ADMINS:")
        print(f"  {'Username':<20} {'Password':<15} {'Display Name':<25}")
        print("  " + "-" * 60)
        for user_id, password, display_name in created_admins:
            print(f"  {user_id:<20} {password:<15} {display_name:<25}")
        print()
    
    # Users table
    if created_users:
        print("USERS:")
        print(f"  {'Username':<20} {'Password':<15} {'Display Name':<20} {'Team':<15}")
        print("  " + "-" * 70)
        for user_id, password, display_name, team in created_users:
            team_str = team if team else "-"
            print(f"  {user_id:<20} {password:<15} {display_name:<20} {team_str:<15}")
        print()
    
    # Summary
    print("-" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Total users in system: {user_store.user_count()}")
    print(f"  Total admins: {user_store.admin_count()}")
    print(f"  Created this run: {len(created_admins) + len(created_users)}")
    print(f"    - Admins: {len(created_admins)}")
    print(f"    - Users: {len(created_users)}")
    print(f"  Skipped (already exist or invalid): {len(skipped)}")
    print()
    
    print(f"Users saved to: {os.path.abspath(args.output)}")
    print()
    print("=" * 70)
    print("SETUP COMPLETE")
    print("=" * 70)
    print()
    print("Next steps:")
    print("  1. Start the server:  python server.py")
    print("  2. Open the frontend: http://localhost:8080/fortisian_trading.html")
    print("  3. Login with credentials above")
    print()
    
    # Return exit code based on success
    if len(created_admins) + len(created_users) > 0:
        return 0
    else:
        print("WARNING: No users were created!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
