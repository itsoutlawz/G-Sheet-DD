# DamaDam Target Bot (Single-File) — v3.2.1

- Processes targets from Target sheet (only pending variants) and writes to ProfilesTarget.
- Inserts new/updated rows at Row 2, highlights changed cells, adds notes.
- On failure/cancel, reverts to "⚡ Pending" with a remark.

## Secrets (GitHub Actions)

```ini
DAMADAM_USERNAME=...
DAMADAM_PASSWORD=...
DAMADAM_USERNAME_2=... (optional)
DAMADAM_PASSWORD_2=... (optional)
GOOGLE_SHEET_URL=https://docs.google.com/spreadsheets/d/<sheet-id>
GOOGLE_CREDENTIALS_JSON={ ... service account json ... }
```

## Run locally

```bash
pip install -r requirements.txt
python Scraper.py
```

## GitHub Actions
- Manual dispatch and schedule supported.
- The workflow writes GOOGLE_CREDENTIALS_JSON to google_credentials.json and sets GOOGLE_APPLICATION_CREDENTIALS.
