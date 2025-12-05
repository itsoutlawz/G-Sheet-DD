# Auto-Resize Sheet Fix

## Problem
```
APIError: [400]: Range (ProfilesTarget!A1000) exceeds grid limits. Max rows: 999
```

Existing sheets had only 999 rows, but the bot tried to append beyond that limit.

## Solution
Added automatic sheet resizing when initializing the bot:

### New Method: `_ensure_sheet_size()`
```python
def _ensure_sheet_size(self, sheet, required_rows):
    """Resize sheet if current rows < required rows"""
    try:
        if sheet.row_count < required_rows:
            log_msg(f"Resizing {sheet.title}: {sheet.row_count} -> {required_rows} rows")
            self.ss.batch_update({"requests": [{"updateSheetProperties": {
                "fields": "gridProperties.rowCount", 
                "properties": {"gridProperties": {"rowCount": required_rows}, "sheetId": sheet.id}
            }}]})
            time.sleep(1)
    except Exception as e:
        log_msg(f"Sheet resize failed: {e}")
```

### Initialization
```python
# ProfilesTarget: Auto-resize to 10,000 rows
self.ws=self._get_or_create("ProfilesTarget", cols=len(COLUMN_ORDER), rows=10000)
self._ensure_sheet_size(self.ws, 10000)

# Target: Auto-resize to 5,000 rows
self.target=self._get_or_create("Target", cols=4, rows=5000)
self._ensure_sheet_size(self.target, 5000)
```

## How It Works

1. **On first run**: Creates new sheets with 10,000 rows
2. **On subsequent runs**: Detects existing sheets and resizes them if needed
3. **Automatic**: No manual intervention required
4. **Logged**: Prints resize operations to console

## Expected Output
```
[07:27:53] Resizing ProfilesTarget: 999 -> 10000 rows
[07:27:54] Resizing Target: 500 -> 5000 rows
```

## Verification

Run the bot and verify:
- ✅ No "exceeds grid limits" errors
- ✅ Resize messages appear in logs
- ✅ All profiles process successfully

## Files Changed

- `Scraper.py`:
  - Lines 316, 318: Added `_ensure_sheet_size()` calls
  - Lines 349-357: New `_ensure_sheet_size()` method

