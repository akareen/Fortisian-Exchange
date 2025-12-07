#!/usr/bin/env python3
"""
Fortisian Exchange - User Provisioning Script

This script creates user accounts for the trading exchange.
Run this AFTER starting the server at least once (to create users.json).

Usage:
    python create_users.py

The script will:
1. Create a default admin account (if not exists)
2. Create sample student accounts
3. Print credentials for distribution
"""

import sys
import os

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


def main():
    print("=" * 60)
    print("FORTISIAN EXCHANGE - USER PROVISIONING")
    print("=" * 60)
    print()
    
    # Configuration
    ADMIN_PASSWORD = "admin123"  # Change this for production!
    STORAGE_PATH = "users.json"
    
    # Initialize auth system
    print(f"Initializing auth system (storage: {STORAGE_PATH})...")
    user_store, session_manager = create_auth_system(
        storage_path=STORAGE_PATH,
        create_default_admin=True,
        default_admin_password=ADMIN_PASSWORD,
    )
    
    print()
    print("-" * 60)
    print("ADMIN ACCOUNT")
    print("-" * 60)
    print(f"  Username: admin")
    print(f"  Password: {ADMIN_PASSWORD}")
    print()
    
    # Define student accounts to create
    # Format: (user_id, password, display_name, team)
    students = [
        ("student1", "alpha123", "Alice Anderson", "Team Alpha"),
        ("student2", "alpha456", "Bob Bradley", "Team Alpha"),
        ("student3", "beta123", "Charlie Chen", "Team Beta"),
        ("student4", "beta456", "Diana Davis", "Team Beta"),
        ("student5", "gamma123", "Eve Evans", "Team Gamma"),
        ("student6", "gamma456", "Frank Foster", "Team Gamma"),
        ("trader1", "trade111", "Pro Trader 1", None),
        ("trader2", "trade222", "Pro Trader 2", None),
        ("demo", "demo1234", "Demo User", None),
    ]
    
    print("-" * 60)
    print("CREATING USER ACCOUNTS")
    print("-" * 60)
    
    created = []
    skipped = []
    
    for user_id, password, display_name, team in students:
        try:
            user_store.create_user(
                user_id=user_id,
                password=password,
                display_name=display_name,
                is_admin=False,
                team=team,
            )
            created.append((user_id, password, display_name, team))
            print(f"  ✓ Created: {user_id}")
        except ValueError as e:
            skipped.append((user_id, str(e)))
            print(f"  - Skipped: {user_id} ({e})")
    
    print()
    print("-" * 60)
    print("CREDENTIAL SUMMARY (for distribution)")
    print("-" * 60)
    print()
    print(f"{'Username':<15} {'Password':<15} {'Display Name':<20} {'Team':<15}")
    print("-" * 65)
    print(f"{'admin':<15} {ADMIN_PASSWORD:<15} {'Administrator':<20} {'(admin)':<15}")
    
    for user_id, password, display_name, team in created:
        team_str = team if team else "-"
        print(f"{user_id:<15} {password:<15} {display_name:<20} {team_str:<15}")
    
    print()
    print("-" * 60)
    print("SUMMARY")
    print("-" * 60)
    print(f"  Total users in system: {user_store.user_count()}")
    print(f"  Admins: {user_store.admin_count()}")
    print(f"  Created this run: {len(created)}")
    print(f"  Skipped (already exist): {len(skipped)}")
    print()
    
    # Save is automatic (UserStore saves on create)
    print(f"Users saved to: {os.path.abspath(STORAGE_PATH)}")
    print()
    print("=" * 60)
    print("SETUP COMPLETE")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. Start the server:  python server.py")
    print("  2. Open the frontend: http://localhost:3000/fortisian_exchange.html")
    print("  3. Login with credentials above")
    print()


if __name__ == "__main__":
    main()
