# AP Assist Marcone Bill Credits - Deployment Checklist

## 📋 Pre-Deployment Tasks

### 1. Create Folder Structure in NetSuite

Navigate to **Documents > Files > File Cabinet**

Create these 4 new folders (or use existing):

| Folder Name | Purpose | Parent Folder |
|-------------|---------|---------------|
| JSON Processed | Successfully processed JSON files | SuiteScripts/bill-credit-automation |
| JSON Skipped | Failed/skipped JSON files | SuiteScripts/bill-credit-automation |
| PDF Processed | Successfully processed PDF files | SuiteScripts/bill-credit-automation |
| PDF Skipped | Failed/skipped PDF files | SuiteScripts/bill-credit-automation |

**Record the internal IDs for each folder:**
- JSON Processed: ________
- JSON Skipped: ________
- PDF Processed: ________
- PDF Skipped: ________

---

### 2. Update Custom Record

Navigate to **Customization > Lists, Records, & Fields > Custom Records**

Find record: **AP Assist Vendor Configuration** (ID: customrecord_ap_assist_vend_config)

Edit record **ID: 1** (Marcone configuration)

Update these fields with folder IDs from step 1:

| Field Name | Internal ID | Value |
|------------|-------------|-------|
| JSON Processed Folder | custrecord_ap_assist_json_processed_fold | ________ |
| JSON Skipped Folder | custrecord_ap_assist_json_skipped_fold | ________ |
| PDF Processed Folder | custrecord_ap_assist_pdf_processed_fold | ________ |
| PDF Skipped Folder | custrecord_ap_assist_pdf_skipped_fold | ________ |

Verify existing fields are still correct:
- ✅ Vendor: 2106 (Marcone)
- ✅ PDF Source Folder: 2920210
- ✅ JSON Source Folder: 2920211

**Save the record.**

---

### 3. Upload Script to NetSuite

1. Go to **Documents > Files > File Cabinet**
2. Navigate to: `SuiteScripts/bill-credit-automation/`
3. Upload: `AP Assist Marcone Bill Credits.js`
4. **Record the script's internal ID:** ________

---

### 4. Create Scheduled Script Record

1. Go to **Customization > Scripting > Scripts > New**
2. Select file: `AP Assist Marcone Bill Credits.js`
3. Click **Create Script Record**

#### Script Record Settings:

| Field | Value |
|-------|-------|
| **Name** | AP Assist Marcone Bill Credits |
| **ID** | customscript_ap_assist_marcone_bills |
| **Status** | Testing (for now) |

**Save the script.**

---

### 5. Create Script Deployment

On the script record, click **Deploy Script**

#### Deployment Settings:

| Field | Value |
|-------|-------|
| **Title** | AP Assist Marcone Bill Credits - Prod |
| **ID** | customdeploy_ap_assist_marcone_prod |
| **Status** | Testing (for now) |
| **Log Level** | Debug (for initial testing) |
| **Execute As** | Administrator |

#### Schedule Settings:

| Field | Value |
|-------|-------|
| **Repeat** | Hourly (for testing) |
| **Every** | 1 hour |
| **Start Date** | Today |
| **Preferred Start Time** | Current hour |

**Save the deployment.**

---

## 🧪 Testing Phase

### Test 1: Place Sample Files

1. Create test JSON file with valid structure
2. Upload to JSON Source folder (ID: 2920211)
3. Upload matching PDF to PDF Source folder (ID: 2920210)
4. Ensure filenames match (except extension)

**Test JSON Structure:**
```json
{
  "isCreditMemo": true,
  "creditType": "Warranty Credit",
  "invoiceNumber": "TEST12345",
  "invoiceDate": "01/02/2026",
  "poNumber": "",
  "deliveryAmount": "$0.00",
  "documentTotal": "($50.00)",
  "lineItems": [{
    "nardaNumber": "NF",
    "partNumber": "TEST123",
    "totalAmount": "($50.00)",
    "originalBillNumber": "12345678",
    "salesOrderNumber": ""
  }],
  "validationError": ""
}
```

### Test 2: Run Script Manually

1. Go to script deployment
2. Click **Run Now**
3. Wait for execution to complete

### Test 3: Verify Results

Check these items:

#### ✅ Execution Log
- No errors in execution log
- "Script Complete" message with stats
- Governance usage reasonable

#### ✅ Transactions Created
- Vendor Credit or Journal Entry created
- Correct amounts
- Correct accounts/entities
- Files attached to transaction

#### ✅ File Movement
- JSON moved from source to processed/skipped
- PDF moved from source to processed/skipped
- Files in correct folders based on success/failure

#### ✅ Email Summary
- Email received at configured address
- Subject line correct
- Summary statistics accurate
- Transaction links work

---

### Test 4: Error Cases

Test these scenarios:

1. **Invalid JSON**
   - Upload malformed JSON
   - Verify moves to skipped
   - Verify error logged

2. **Missing Required Field**
   - JSON missing `invoiceNumber`
   - Verify moves to skipped
   - Verify validation failure counted

3. **Claude Validation Error**
   - JSON with `validationError: "Some error"`
   - Verify moves to skipped
   - Verify logged correctly

4. **Missing PDF**
   - Upload JSON without matching PDF
   - Verify JSON processed
   - Verify warning logged

---

## 🚀 Production Deployment

Once testing is successful:

### 1. Update Deployment Settings

| Field | Change To |
|-------|-----------|
| **Status** | Released |
| **Log Level** | Audit (reduce verbosity) |
| **Schedule** | Every 15 minutes |

### 2. Update Script Status

| Field | Change To |
|-------|-----------|
| **Status** | Released |

### 3. Monitor First Day

Check these throughout first day:

- **Hourly:** Review execution logs
- **Hourly:** Check email summaries
- **Hourly:** Verify files moving correctly
- **End of day:** Review all transactions created
- **End of day:** Check governance usage

### 4. Verify Railway Integration

Confirm end-to-end flow:

1. Email arrives with Marcone PDF
2. Railway processes with Claude
3. RESTlet saves JSON + PDF
4. This script creates transaction
5. Files moved to processed
6. Email summary sent

---

## 📊 Monitoring Checklist

### Daily Checks

- [ ] Review email summaries
- [ ] Check for any failed files in skipped folders
- [ ] Verify transaction amounts reasonable
- [ ] Monitor governance usage

### Weekly Checks

- [ ] Review all transactions created
- [ ] Verify no files stuck in source folders
- [ ] Check for duplicate transactions
- [ ] Review error logs

### Monthly Checks

- [ ] Audit processed folders (consider archiving old files)
- [ ] Review governance trends
- [ ] Verify custom record configuration still correct
- [ ] Check for any pattern changes in Claude extractions

---

## 🚨 Troubleshooting

### Files Not Processing

**Check:**
1. Script deployment is Released
2. Schedule is active
3. Files are in correct source folder
4. Files have .json extension
5. Custom record configuration correct

**Review:**
- Execution log for errors
- File permissions
- Governance limits

### Transactions Not Created

**Check:**
1. JSON validation passed
2. NARDA patterns recognized
3. VRA exists (for vendor credits)
4. Customer exists (for journal entries)
5. Accounts/entities correct

**Review:**
- processOrderNo() results in log
- Transaction creation errors
- Original CSV script for reference

### Files Not Moving

**Check:**
1. Target folders exist
2. Folder IDs correct in custom record
3. File permissions correct
4. Governance sufficient

**Review:**
- moveJsonAndPdfFiles() errors
- File movement section in log

### Email Not Received

**Check:**
1. Email module loaded
2. Sender has permission
3. Recipient address correct
4. Email server functioning

**Review:**
- sendResultsEmail() function
- Email runtime errors

---

## 📞 Support Contacts

**NetSuite Administrator:** ___________________  
**Script Developer:** GitHub Copilot  
**Railway Integration:** Node.js email-poller

**Key Documentation:**
- This checklist
- `AP_ASSIST_SCRIPT_MODIFICATIONS.md`
- `New Marcone Product Warranty CSV Processing.js` (reference)

---

## ✅ Final Sign-Off

Script deployed and verified by:

**Name:** ___________________  
**Date:** ___________________  
**Signature:** ___________________

**Verified Items:**
- [ ] All folders created
- [ ] Custom record updated
- [ ] Script uploaded
- [ ] Deployment created
- [ ] Testing completed
- [ ] Production deployed
- [ ] Monitoring in place
- [ ] Documentation reviewed

---

**Last Updated:** 2026-01-02  
**Version:** 1.0  
**Status:** Ready for Deployment
