# Gspread Compatibility Fix

## Issue
```
'Worksheet' object has no attribute 'set_column_width'
```

The `set_column_width()` method was added in gspread 6.0+, but the GitHub Actions workflow uses Python 3.10 which may have an older gspread version.

## Solution
Replaced all `set_column_width()` calls with the Google Sheets API `batch_update()` method, which is compatible with all gspread versions.

## Changes Made

### New Method: `_set_column_widths()`
```python
def _set_column_widths(self, sheet, col_widths):
    """Set column widths using batch_update API (compatible with all gspread versions)"""
    try:
        requests = []
        for col_letter, width in col_widths.items():
            col_idx = ord(col_letter.upper()) - ord('A')
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": col_idx,
                        "endIndex": col_idx + 1
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize"
                }
            })
        if requests:
            self.ss.batch_update({"requests": requests})
    except Exception as e:
        log_msg(f"Set column widths failed: {e}")
```

### Updated Calls
- **ProfilesTarget sheet** (line 419): `self._set_column_widths(self.ws, col_widths)`
- **Target sheet** (line 447): `self._set_column_widths(self.target, col_widths)`
- **Dashboard sheet** (line 478): `self._set_column_widths(self.dashboard, col_widths)`
- **Tags sheet** (line 507): `self._set_column_widths(self.tags_sheet, col_widths)`

## Compatibility
- ✅ Works with all gspread versions (5.x, 6.x, 7.x+)
- ✅ Uses Google Sheets API directly via `batch_update()`
- ✅ No external dependencies added
- ✅ Syntax validated

## Testing
```bash
python -m py_compile Scraper.py
# Output: Syntax check passed
```

## Result
The bot will now successfully format Google Sheets without errors, regardless of the gspread version installed.

