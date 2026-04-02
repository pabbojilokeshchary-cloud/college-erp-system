# Kaveri University Fees Management System (Local • Free • Windows)

## What this is
A real working Fees + Receipt system:
- Permanent local database (SQLite)
- Students CRUD
- Fee heads and due calculation
- Payments with fee-purpose tick boxes
- Auto receipt numbering: KU/2025-26/RCPT/0001...
- Printable receipt with Kaveri University logo
- Manual DB backup button

## Requirements
- Python 3.10+ (Windows)
- pip

## Install
Open CMD in this folder:
```bash
pip install flask
```

## Run
```bash
python app.py
```
Open in browser:
- http://127.0.0.1:5000

## LAN access (later)
Run is already set to host 0.0.0.0. Other PCs can open:
- http://<YOUR_LAPTOP_IP>:5000

## Database file
- database.db (in same folder)

## Backups
Click "Backup DB" on Payments page.
Backup DB copies will be in /backup
