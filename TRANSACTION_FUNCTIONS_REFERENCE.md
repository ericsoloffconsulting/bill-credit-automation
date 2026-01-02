# Transaction Creation Functions - Reference

This document contains all the functions needed for creating Journal Entries and Vendor Credits. These functions work with transaction data and are **independent of how the data is sourced** (PDF.co, Claude API, etc.).

## Overview

The transaction creation layer expects data in a specific format (`splitPart` object) and handles:
- Journal Entry creation for J#### and INV#### NARDA numbers
- Vendor Credit creation for CONCDA, NF, CORE, CONCESSION NARDA numbers  
- Duplicate detection
- Entity lookup
- File attachments
- Error handling and skipped transactions

---

## Core Data Structure Expected

```javascript
// The splitPart object that transaction functions expect:
var splitPart = {
    fileName: "credit_memo_12345.pdf",
    fileId: 12345,  // NetSuite file cabinet ID
    partNumber: "1",
    invoiceNumber: "70362469",
    invoiceDate: "12/23/2025",
    documentTotal: "($22.35)",
    deliveryAmount: "$0.00",
    lineItemsByNarda: {
        "SHORT": [
            {
                nardaNumber: "SHORT",
                totalAmount: "($22.35)",
                originalBillNumber: "70327394",
                partNumber: "WD12X34182",
                salesOrderNumber: "SOASER19837"
            }
        ],
        "J1234": [
            {
                nardaNumber: "J1234",
                totalAmount: "($100.00)",
                originalBillNumber: "70327394"
            }
        ]
    }
};

// Note: The old PDF.co version used extractedData.groupedLineItems with a different structure
// We need to adapt our Claude JSON to match the nardaGroup structure expected below
```

---

## 1. Main Orchestrator Function

### `createJournalEntriesFromLineItems(splitPart, recordId)`

**Purpose**: Routes line items to appropriate creation functions based on NARDA type

**Input**:
- `splitPart` - Object containing invoice data and line items grouped by NARDA
- `recordId` - Custom record ID for tracking (optional for Claude version)

**Process**:
1. Validates that `splitPart.lineItemsByNarda` exists and has data
2. Classifies each NARDA group:
   - **Journal Entry NARDA**: J#### or INV#### patterns
   - **Vendor Credit NARDA**: CONCDA, CONCDAM, NF, CORE, CONCESSION
   - **Short Ship NARDA**: SHORT, BOX (skipped for manual processing)
   - **Unknown NARDA**: Everything else (skipped for manual review)
3. For Journal Entries:
   - Multiple J####/INV#### groups → calls `createSingleJournalEntryWithMultipleLines()`
   - Single J####/INV#### group → calls `createJournalEntryFromNardaGroup()`
4. For Vendor Credits:
   - Calls `consolidateVendorCreditGroups()` to group by original bill number
   - Searches for matching VRA (Vendor Return Authorization)
   - Calls `createVendorCreditFromVRA()` for each bill number group

**Output**:
```javascript
{
    success: true,
    journalEntries: [...],      // Array of created JE results
    vendorCredits: [...],       // Array of created VC results
    skippedTransactions: [...], // Array of skipped items with reasons
    totalNARDAGroups: 5
}
```

**Key Adaptations Needed for Claude JSON**:
- Old code expects `extractedData.groupedLineItems` with structure: `{ nardaNumber, lineItems: [], totalAmount }`
- Need to transform `splitPart.lineItemsByNarda` (just arrays) into the expected nardaGroup structure
- Add `totalAmount` calculation per NARDA group
- Add `originalBillNumbers` array per NARDA group

---

## 2. Journal Entry Functions

### `createJournalEntryFromNardaGroup(splitPart, recordId, nardaGroup, nardaNumber)`

**Purpose**: Creates a single Journal Entry for one NARDA group

**Input**:
- `splitPart` - Contains `invoiceNumber`, `invoiceDate`, `fileId`
- `recordId` - Custom record ID (optional)
- `nardaGroup` - Object with `{ nardaNumber, lineItems, totalAmount, originalBillNumbers }`
- `nardaNumber` - The NARDA identifier (e.g., "J1234")

**Process**:
1. Creates tranid as `invoiceNumber + ' CM'` (e.g., "70362469 CM")
2. Checks for duplicates via `checkForDuplicateJournalEntry()`
3. Finds customer entity via `findCreditLineEntity()` using the NARDA number
4. Creates Journal Entry with 2 lines:
   - **Debit Line**: Account 111 (A/P), Entity = Marcone (2106), Amount = totalAmount
   - **Credit Line**: Account 119 (A/R), Entity = Customer (from lookup), Amount = totalAmount
5. Attaches PDF via `attachFileToRecord()`

**Memo Format**: `"MARCONE CM{invoiceNumber} {nardaNumber}"`

**Returns**: `{ success, journalEntryId, tranid, nardaGroups: [nardaNumber], totalAmount, attachmentResult }`

---

### `createSingleJournalEntryWithMultipleLines(splitPart, recordId, journalEntryGroups)`

**Purpose**: Creates ONE Journal Entry with multiple credit lines for different NARDA numbers

**Input**:
- `splitPart` - Contains `invoiceNumber`, `invoiceDate`, `fileId`
- `recordId` - Custom record ID (optional)
- `journalEntryGroups` - Array of `{ nardaNumber, nardaGroup }` objects

**Process**:
1. Creates single tranid as `invoiceNumber + ' CM'`
2. Checks for duplicates
3. Calculates grand total across all NARDA groups
4. Creates Journal Entry with:
   - **1 Debit Line**: Account 111 (A/P), Entity = Marcone (2106), Amount = grand total
   - **Multiple Credit Lines**: One per NARDA group
     * Account 119 (A/R)
     * Entity = Customer (looked up per NARDA)
     * Amount = that NARDA's total
5. Attaches PDF

**Memo Logic**:
- Multiple different NARDA values: `"MARCONE CM{invoiceNumber} Multi-NARDA Groups"`
- Single NARDA with multiple lines: `"MARCONE CM{invoiceNumber} Consolidated {nardaNumber}"`
- Single NARDA single line: `"MARCONE CM{invoiceNumber} {nardaNumber}"`

**Returns**: `{ success, journalEntryId, tranid, nardaGroups: [...], grandTotal, attachmentResult }`

---

## 3. Duplicate Detection

### `checkForDuplicateJournalEntry(tranid, recordId)`

**Purpose**: Searches for existing Journal Entries with same tranid

**Search Criteria**:
```javascript
type = 'Journal'
AND tranid = tranid
```

**Returns**:
- Success: `{ success: true, tranid }`
- Duplicate found: `{ success: false, existingEntry: {...}, allDuplicates: [...], duplicateCount }`

---

### `checkForDuplicateVendorCredit(tranid, recordId)`

**Purpose**: Searches for existing Vendor Credits with same tranid

**Search Criteria**:
```javascript
type = 'VendCred'
AND tranid = tranid
```

**Returns**:
- Success: `{ success: true }`
- Duplicate found: `{ success: false, existingEntry: {...} }`

---

## 4. Entity Lookup

### `findCreditLineEntity(nardaNumber, recordId)`

**Purpose**: Finds customer entity for Journal Entry credit line using NARDA number

**Search Strategy**:
1. **Primary Search**: Open invoices where `custbody_f4n_job_id = nardaNumber`
2. **Fallback Search**: Open invoices where `tranid = nardaNumber`

**Filters**:
```javascript
type = 'CustInvc'
AND status = 'CustInvc:A' (Open)
AND (custbody_f4n_job_id = nardaNumber OR tranid = nardaNumber)
```

**Returns**:
- Success: `{ success: true, entityId, invoiceTranid, tranDate, searchResultCount }`
- Not found: `{ success: false, error, reason: 'NO_MATCHING_OPEN_INVOICE', nardaNumber }`
- Error: `{ success: false, error, reason: 'SEARCH_ERROR', nardaNumber }`

**Important**: When NO_MATCHING_OPEN_INVOICE is returned, the calling function treats this as a skip (not an error) because we can't determine which customer to credit without an open invoice.

---

## 5. Vendor Credit Functions

### `consolidateVendorCreditGroups(lineItemsByNarda)`

**Purpose**: Groups vendor credit NARDA types (CONCDA, NF, CORE) by original bill number

**Why**: Multiple NARDA types (e.g., NF + CORE) referencing the same original bill should create ONE vendor credit

**Input**: `lineItemsByNarda` object with NARDA groups

**Process**:
1. Filters to only vendor credit types (CONCDA, CONCDAM, NF, CORE, CONCESSION)
2. Extracts all unique original bill numbers from line items
3. Groups line items by original bill number across all NARDA types
4. Calculates total amount per bill number
5. Tracks all NARDA types that contributed to each bill

**Output**:
```javascript
{
    "70327394": {
        originalBillNumber: "70327394",
        nardaTypes: ["NF", "CORE"],
        lineItems: [...],  // All line items for this bill
        totalAmount: 150.00,
        allNardaNumbers: ["NF", "CORE"]
    }
}
```

---

### `createVendorCreditFromVRA(splitPart, recordId, nardaGroup, vraResults, originalBillNumber)`

**Purpose**: Creates Vendor Credit by transforming a Vendor Return Authorization (VRA)

**Input**:
- `splitPart` - Contains invoice data and fileId
- `recordId` - Custom record ID (optional)
- `nardaGroup` - Object with line items, NARDA types, and amounts
- `vraResults` - Array of matching VRA records from search
- `originalBillNumber` - The bill number to match

**Process**:
1. Groups VRA lines by parent VRA internal ID
2. For each VRA, attempts transformation:
   - Loads VRA record
   - Validates status (not Closed/Rejected/Cancelled)
   - Matches PDF line items to VRA line items by amount
   - Transforms VRA → Vendor Credit
   - Filters to only keep matched lines
   - Adds delivery expense line if applicable
   - Saves and attaches PDF
3. If one VRA fails, tries next VRA until success or all exhausted

**Key Validations**:
- VRA status must allow transformation
- Line amounts must match PDF amounts (within $0.01)
- VRA must not be fully credited already

**Returns**:
- Success: `{ success: true, isVendorCredit: true, vendorCreditId, vendorCreditTranid, nardaNumber, totalAmount, matchedLineCount, originalBillNumber, matchingVRA: {...}, attachmentResult, deliveryAmountProcessed }`
- Skipped: `{ success: true, isSkipped: true, skipReason, skipType, ... }`
- Failed: `{ success: false, error }`

---

### `searchForMatchingVRA(billNumber, recordId)`

**Purpose**: Finds Vendor Return Authorizations that reference the original bill number

**Search Criteria**:
```javascript
type = 'VendAuth'
AND memo CONTAINS billNumber
```

**Returns**: Array of VRA records with line details

---

### `matchPDFLinesToVRALines(pdfLines, vraLines, originalBillNumber)`

**Purpose**: Matches PDF credit line items to VRA lines by amount

**Algorithm**:
1. For each PDF line amount
2. Find unused VRA line with matching amount (within $0.01 tolerance)
3. Create matched pair
4. Mark VRA line as used
5. Move to next PDF line

**Returns**: Array of `{ pdfLine, vraLine, amount }` pairs

---

### `createGroupedVendorCredit(splitPart, recordId, nardaGroup, matchedPairs, originalBillNumber)`

**Purpose**: Creates the actual Vendor Credit record from matched VRA lines

**Process**:
1. Validates invoice date
2. Creates tranid as `invoiceNumber` (no suffix)
3. Checks for duplicates
4. Transforms VRA → Vendor Credit
5. Sets header fields (tranid, date, memo)
6. Removes all VC lines except matched ones
7. Adds delivery expense line if > $0.00:
   - Account: 367 (Freight In)
   - Department: 13 (Service Department)
8. Saves and attaches PDF

**Memo Format**: `"{nardaTypes} Credit - {invoiceNumber} - Bill: {billNumber} - VRA: {vraTranid}"`

Example: `"NF+CORE Credit - 70362469 - Bill: 70327394 - VRA: RMA12345"`

---

### `groupLineItemsByOriginalBillNumber(lineItems)`

**Purpose**: Simple utility to group line items by their originalBillNumber field

**Returns**: Object keyed by bill number, values are arrays of line items

---

## 6. File Attachment

### `attachFileToRecord(recordId, fileId, customRecordId, recordType)`

**Purpose**: Attaches PDF file to created transaction

**Parameters**:
- `recordId` - Transaction ID (JE or VC)
- `fileId` - NetSuite file cabinet ID of PDF
- `customRecordId` - Original custom record ID (optional)
- `recordType` - Optional record type (defaults to JOURNAL_ENTRY)

**Note**: Implementation details not shown in excerpt, but this function uses `record.attach()` or similar

---

## 7. Helper Functions

### `updateCustomRecordMemo(recordId, processingData)`

**Purpose**: Updates the original custom record with processing results (PDF.co specific, may not be needed for Claude version)

### `sendResultsEmail(stats)`

**Purpose**: Sends summary email of processing results

### `savePDFToFailedFolder(fileId, fileName, recordId, errorData)`

**Purpose**: Moves PDF to failed folder when processing fails (PDF.co specific)

---

## Configuration Constants

```javascript
var CONFIG = {
    FOLDERS: {
        // Source folder IDs will differ for Claude version
        JSON_SOURCE: 2921327,     // Railway output
        JSON_PROCESSED: 2921328,
        JSON_FAILED: 2921329
    },
    ACCOUNTS: {
        ACCOUNTS_PAYABLE: 111,    // Journal Entry debit line
        ACCOUNTS_RECEIVABLE: 119, // Journal Entry credit line
        FREIGHT_IN: 367           // Vendor Credit delivery expense
    },
    ENTITIES: {
        MARCONE: 2106,            // Vendor for A/P debit
        SERVICE_DEPARTMENT: 13    // Department for delivery expense
    }
};
```

---

## Summary: What Needs to Change for Claude JSON

### Keep Unchanged (100% reusable):
1. ✅ All Journal Entry creation functions
2. ✅ All Vendor Credit creation functions  
3. ✅ All duplicate detection functions
4. ✅ Entity lookup function
5. ✅ File attachment function
6. ✅ Configuration constants

### Need to Adapt:
1. 🔄 `createJournalEntriesFromLineItems()` - Expects different data structure
   - Old: `extractedData.groupedLineItems[narda] = { nardaNumber, lineItems: [], totalAmount, originalBillNumbers: [] }`
   - New: `splitPart.lineItemsByNarda[narda] = [lineItem1, lineItem2, ...]`
   - **Fix**: Transform array to nardaGroup structure with calculated totalAmount

2. 🔄 Data source layer (completely replace):
   - Remove: All PDF.co API calls (~2000 lines)
   - Remove: Coordinate-based extraction functions
   - Remove: PDF splitting logic
   - Add: Direct JSON parsing from Claude output
   - Add: Simple transformation function

### Transformation Function Needed:

```javascript
function transformClaudeJSONToSplitPart(claudeJSON, fileName) {
    // Input: Claude's simple JSON
    // Output: splitPart object with nardaGroup structure
    
    var lineItemsByNarda = {};
    
    // Group by NARDA and calculate totals
    for (var i = 0; i < claudeJSON.lineItems.length; i++) {
        var item = claudeJSON.lineItems[i];
        var narda = item.nardaNumber;
        
        if (!lineItemsByNarda[narda]) {
            lineItemsByNarda[narda] = [];
        }
        
        lineItemsByNarda[narda].push(item);
    }
    
    // Transform to nardaGroup structure with calculated totals
    var groupedLineItems = {};
    for (var narda in lineItemsByNarda) {
        var items = lineItemsByNarda[narda];
        var totalAmount = 0;
        var originalBillNumbers = [];
        
        for (var i = 0; i < items.length; i++) {
            var amt = parseFloat(items[i].totalAmount.replace(/[()$,-]/g, ''));
            if (!isNaN(amt)) {
                totalAmount += amt;
            }
            
            if (items[i].originalBillNumber && 
                originalBillNumbers.indexOf(items[i].originalBillNumber) === -1) {
                originalBillNumbers.push(items[i].originalBillNumber);
            }
        }
        
        groupedLineItems[narda] = {
            nardaNumber: narda,
            lineItems: items,
            totalAmount: totalAmount,
            originalBillNumbers: originalBillNumbers
        };
    }
    
    return {
        fileName: fileName,
        fileId: null, // Set when saved to NetSuite
        partNumber: "1",
        invoiceNumber: claudeJSON.invoiceNumber,
        invoiceDate: claudeJSON.invoiceDate,
        documentTotal: claudeJSON.documentTotal,
        deliveryAmount: claudeJSON.deliveryAmount || "$0.00",
        lineItemsByNarda: lineItemsByNarda,
        // For transaction functions, wrap in jsonResult structure
        jsonResult: {
            success: true,
            extractedData: {
                success: true,
                invoiceNumber: claudeJSON.invoiceNumber,
                invoiceDate: claudeJSON.invoiceDate,
                deliveryAmount: claudeJSON.deliveryAmount || "$0.00",
                lineItems: claudeJSON.lineItems,
                groupedLineItems: groupedLineItems,
                fileName: fileName,
                extractionSuccessful: true,
                allFieldsFound: true
            }
        }
    };
}
```

---

## Key Takeaways

1. **~80% of code can stay unchanged** - All transaction creation logic is data-source agnostic
2. **~20% needs replacement** - Only the data extraction/sourcing layer
3. **Main change needed** - Transform Claude's flat JSON structure into the nardaGroup format expected by transaction functions
4. **No business logic changes** - All duplicate detection, entity lookup, NARDA routing stays the same

The transaction creation functions are well-designed and don't care whether data came from PDF.co coordinate extraction or Claude API JSON parsing. We just need to present the data in the format they expect.
