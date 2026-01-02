# AP Assist Marcone Bill Credits - Script Modification Summary

## Overview
Successfully duplicated and modified "New Marcone Product Warranty CSV Processing.js" to create a new JSON-based processor: **"AP Assist Marcone Bill Credits.js"**

## Purpose
Process JSON files extracted by Claude AI from Marcone credit memo PDFs, creating Vendor Credits and Journal Entries automatically.

## Data Flow
1. Node.js email-poller extracts PDFs with Claude API
2. RESTlet saves PDF + JSON to NetSuite folders
3. **THIS SCRIPT** processes JSON files in bulk
4. Creates transactions (Vendor Credits or Journal Entries)
5. Moves processed files to appropriate folders
6. Emails summary report

---

## Modifications Made (6 Chunks)

### Chunk 1: Configuration Updates
**Lines Modified:** 1-55

**Changes:**
- Updated script name and description
- Added `CUSTOM_RECORD_ID: 1` for dynamic folder loading
- Changed folder structure to null placeholders (loaded from custom record)
- Updated limit to `MAX_JSON_FILES_PER_RUN: 75`
- Removed CSV-specific folder IDs

**Key Additions:**
```javascript
var CONFIG = {
    CUSTOM_RECORD_ID: 1,
    FOLDERS: {
        JSON_SOURCE: null,
        JSON_PROCESSED: null,
        JSON_SKIPPED: null,
        PDF_SOURCE: null,
        PDF_PROCESSED: null,
        PDF_SKIPPED: null
    }
}
```

---

### Chunk 2: Execute Function Start
**Lines Modified:** 55-130

**Changes:**
- Added `loadFolderConfiguration()` call at script start
- Updated stats tracking for JSON files instead of OrderNos
- Added `searchJsonFiles()` call to find unprocessed files
- Removed CSV-specific parameters
- Updated logging labels

**Key Logic:**
1. Load folder config from custom record ID 1
2. Search JSON source folder for unprocessed files
3. Process up to 75 files per run (governance limit)
4. Early exit if no files found

---

### Chunk 3: Helper Functions Added
**New Functions Added:** 6 functions inserted before `return { execute }`

1. **`loadFolderConfiguration()`**
   - Loads custom record ID 1
   - Populates all 6 folder IDs into CONFIG.FOLDERS
   - Validates all required folders configured
   - Returns `{success: true/false, error: string}`

2. **`searchJsonFiles(folderId)`**
   - Searches folder for .json files
   - Returns array of `{id, name, created, modified}`
   - Filters by file type JSON

3. **`loadAndParseJsonFile(fileId)`**
   - Loads JSON file by ID
   - Parses JSON content
   - Validates structure
   - Checks for `validationError` from Claude
   - Converts to CSV-compatible structure
   - Returns `{success, fileName, data, error, skipReason}`

4. **`validateJsonStructure(jsonData)`**
   - Validates required fields present
   - Checks `lineItems` is non-empty array
   - Validates each line item has required fields
   - Returns `{valid: true/false, error: string}`

5. **`convertJsonToCsvStructure(jsonData, fileName)`**
   - Maps JSON fields to CSV column structure
   - Enables reuse of existing transaction creation functions
   - One JSON file = one invoice number (like one OrderNo)
   - Returns CSV-compatible data object

6. **Helper structure consistency**
   - All functions return standardized result objects
   - Consistent error handling and logging
   - Skip reasons for tracking failure types

---

### Chunk 4: Main Processing Loop
**Lines Modified:** 120-285

**Changes:**
- Replaced OrderNo iteration with JSON file iteration
- Added JSON parsing per file (call `loadAndParseJsonFile()`)
- Added validation failure handling
- Added file movement after processing (success/skip)
- Updated governance tracking per file
- Removed CSV filtering logic
- Updated stats tracking

**Key Processing Flow:**
```javascript
for each jsonFile in jsonFilesToProcess:
    1. Check governance before processing
    2. Load and parse JSON file
    3. If validation fails → move to skipped, continue
    4. Process transactions (reuses existing functions)
    5. If success → move to processed folders
    6. If failed → move to skipped folders
    7. Track governance per file
```

**File Movement:**
- Success: JSON → JSON_PROCESSED, PDF → PDF_PROCESSED
- Failed/Skipped: JSON → JSON_SKIPPED, PDF → PDF_SKIPPED

---

### Chunk 5: File Movement Functions
**New Functions Added:** 2 functions after `moveProcessedCSV()`

1. **`moveJsonAndPdfFiles(jsonFileId, jsonFileName, jsonTargetFolder, pdfTargetFolder)`**
   - Orchestrates moving both JSON and matching PDF
   - Searches for PDF by replacing .json with .pdf in filename
   - Calls `moveFileToFolder()` for each file
   - Logs warnings if PDF not found

2. **`moveFileToFolder(fileId, fileName, targetFolder, fileType)`**
   - Generic helper to move single file
   - Copies file to target folder
   - Deletes original file
   - Handles errors gracefully (don't stop execution)

**PDF Matching Logic:**
```javascript
var pdfFileName = jsonFileName.replace(/\.json$/i, '.pdf');
// Search PDF_SOURCE folder for matching filename
```

---

### Chunk 6: Email Summary Updates
**Lines Modified:** 3407-3520

**Changes:**
1. **`buildEmailSubject()`**
   - Changed title: "AP Assist Marcone Bill Credits"
   - Updated count: "X of Y files" (not orders)

2. **`buildEmailBody()`**
   - Updated header: "AP ASSIST MARCONE BILL CREDITS - JSON PROCESSING RESULTS"
   - Removed unprocessed file logic (JSON processes all files)

3. **`buildSummarySection()`**
   - Added "Total JSON Files Found"
   - Added "JSON Files Processed"
   - Renamed "OrderNos" to "Invoice Numbers"
   - Removed unprocessed file info
   - Added note about file movement

**Example Output:**
```
AP ASSIST MARCONE BILL CREDITS - JSON PROCESSING RESULTS
=======================================================

PROCESSING SUMMARY
------------------
Total JSON Files Found:      10
JSON Files Processed:        10
Invoice Numbers Processed:   10
Journal Entries Created:     3
Vendor Credits Created:      7
Validation Failures:         0
Skipped Transactions:        0
Failed Entries:              0

Note: JSON and PDF files have been moved to processed/skipped folders.
```

---

## Configuration Requirements

### Custom Record: `customrecord_ap_assist_vend_config` (ID: 1)
Must have these fields populated:

| Field ID | Field Name | Example Value |
|----------|------------|---------------|
| `custrecord_ap_assist_vendor` | Vendor | 2106 (Marcone) |
| `custrecord_ap_asssist_pdf_folder_id` | PDF Source Folder | 2920210 |
| `custrecord_ap_assist_json_folder_id` | JSON Source Folder | 2920211 |
| `custrecord_ap_assist_pdf_processed_fold` | PDF Processed Folder | TBD |
| `custrecord_ap_assist_json_processed_fold` | JSON Processed Folder | TBD |
| `custrecord_ap_assist_pdf_skipped_fold` | PDF Skipped Folder | TBD |
| `custrecord_ap_assist_json_skipped_fold` | JSON Skipped Folder | TBD |

---

## JSON Structure Expected

```json
{
  "isCreditMemo": true,
  "creditType": "Warranty Credit",
  "invoiceNumber": "67718510",
  "invoiceDate": "09/11/2025",
  "poNumber": "12345",
  "deliveryAmount": "$0.00",
  "documentTotal": "($94.58)",
  "lineItems": [
    {
      "nardaNumber": "NF",
      "partNumber": "WR49X10322",
      "totalAmount": "($94.58)",
      "originalBillNumber": "66811026",
      "salesOrderNumber": "SOASER15386"
    }
  ],
  "validationError": ""
}
```

### Required Fields
- `invoiceNumber` - Credit memo number
- `invoiceDate` - Date of credit
- `deliveryAmount` - Freight charges (if any)
- `documentTotal` - Total credit amount
- `lineItems[]` - Array of line items
  - `nardaNumber` - Transaction type identifier
  - `totalAmount` - Line credit amount
  - `originalBillNumber` - Original bill to credit (for vendor credits)

### Validation Logic
- If `validationError` is not empty → skip file
- If any required field missing → skip file
- If `lineItems` is empty → skip file

---

## Transaction Creation Logic (UNCHANGED)

The script **reuses ALL existing transaction creation functions** from the CSV version:

### Vendor Credits
- Match to VRA by `originalBillNumber` in memo field
- Apply to specific AP lines on VRA
- Account: 111 (Accounts Payable)
- Entity: 2106 (Marcone)

### Journal Entries
- Find customer by `nardaNumber` lookup
- Debit: Account 119 (AR), Credit: Account 111 (AP)
- Optional: Account 367 (Freight) if deliveryAmount > $0
- Entity: Depends on customer

### NARDA Patterns
- **JOURNAL_ENTRY:** `/^(J\d+|INV\d+)$/i` → Create JE
- **VENDOR_CREDIT:** `/^(CONCDA|CONCDAM|NF|CORE|CONCESSION)$/i` → Create VC
- **SKIP:** `/^(SHORT|BOX|REBATE)$/i` → Move to skipped

---

## Governance Management

### Limits
- **Max files per run:** 75
- **Min governance reserve:** 100 units

### Tracking
- Governance per JSON file processed
- Governance per Journal Entry created
- Governance per Vendor Credit created
- Total governance used

### Early Termination
If governance drops below 100 units:
1. Stop processing remaining files
2. Files remain in source folder
3. Next scheduled run will process them

---

## File Naming Convention

Files saved by RESTlet follow this pattern:
```
2026-01-02T12-00-00-000Z_marcone_67718694.json
2026-01-02T12-00-00-000Z_marcone_67718694.pdf
```

Format: `<timestamp>_<vendor>_<invoiceNumber>.<extension>`

This ensures:
- JSON and PDF have matching basenames
- Chronological sorting
- Unique filenames

---

## Error Handling

### Validation Failures
- Claude extraction error → Move to skipped
- Missing required fields → Move to skipped
- Invalid JSON structure → Move to skipped

### Transaction Creation Failures
- VRA not found → Skip line, log error
- Customer not found → Skip line, log error
- Record creation error → Move to skipped

### File Movement Errors
- Copy successful, delete fails → Log warning, continue
- Copy fails → Log error, leave in source

---

## Testing Checklist

Before deploying to production:

1. **Configuration**
   - [ ] Custom record ID 1 exists with all folder IDs populated
   - [ ] All 6 folders exist in File Cabinet
   - [ ] Folders have appropriate permissions

2. **JSON Files**
   - [ ] Place test JSON files in source folder
   - [ ] Verify matching PDFs exist in PDF source folder
   - [ ] Test with various NARDA types (JE, VC, SKIP)

3. **Processing**
   - [ ] Run script manually
   - [ ] Verify transactions created correctly
   - [ ] Check files moved to correct folders
   - [ ] Review email summary

4. **Error Cases**
   - [ ] Test with missing validationError
   - [ ] Test with missing required fields
   - [ ] Test with invalid JSON
   - [ ] Test with missing PDF

5. **Schedule**
   - [ ] Deploy as scheduled script
   - [ ] Set appropriate schedule (e.g., every 15 minutes)
   - [ ] Monitor execution logs

---

## Next Steps

1. **Create Folder Structure**
   - Create "JSON Processed" folder
   - Create "JSON Skipped" folder
   - Create "PDF Processed" folder
   - Create "PDF Skipped" folder
   - Get internal IDs for each

2. **Update Custom Record**
   - Populate processed/skipped folder fields
   - Verify all 6 folders configured

3. **Test Script**
   - Upload test JSON + PDF files
   - Run script manually
   - Verify results

4. **Deploy Script**
   - Upload to NetSuite File Cabinet
   - Create Scheduled Script record
   - Set deployment parameters
   - Schedule execution

5. **Monitor**
   - Check execution logs
   - Review email summaries
   - Verify transactions correct
   - Monitor file movements

---

## Key Differences from CSV Version

| Aspect | CSV Version | JSON Version |
|--------|-------------|--------------|
| **Data Source** | Single CSV file with many orders | Multiple JSON files (one per invoice) |
| **Iteration** | Loop through OrderNos in CSV | Loop through JSON files |
| **Parsing** | Parse CSV lines | Parse JSON objects |
| **Validation** | CSV totals validation | JSON structure + Claude validation |
| **File Movement** | Move CSV only | Move JSON + matching PDF |
| **Configuration** | Hardcoded folder IDs | Dynamic from custom record |
| **Batch Logic** | Save unprocessed CSV | Files remain in folder for next run |

---

## Success Metrics

Script is working correctly if:
- ✅ JSON files processed successfully
- ✅ Matching transactions created in NetSuite
- ✅ Files moved to processed/skipped folders
- ✅ Email summary received
- ✅ No errors in execution log
- ✅ Governance usage acceptable

---

## Contact & Support

**Script Created:** 2026-01-02  
**Author:** GitHub Copilot (Claude Sonnet 4.5)  
**Based On:** New Marcone Product Warranty CSV Processing.js

For issues or questions, review:
1. Execution logs in NetSuite
2. Email summary reports
3. This documentation
4. Original CSV script (authoritative for transaction logic)
