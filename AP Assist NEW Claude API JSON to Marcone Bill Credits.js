/**
 * @NApiVersion 2.1
 * @NScriptType ScheduledScript
 * @NModuleScope SameAccount
 * 
 * AP Assist - Claude API JSON to Marcone Bill Credits Processor
 * 
 * This script processes JSON files created by Claude AI (via Railway email processor)
 * and creates Journal Entries and Vendor Credits in NetSuite.
 * 
 * Data Flow:
 * 1. Email arrives at Ethereal → Railway Node.js app
 * 2. Claude API extracts data → Clean JSON created
 * 3. Railway uploads JSON to NetSuite folder 2921327
 * 4. This script processes JSON files → Creates transactions
 * 
 * NO PDF.co API calls - ALL data comes pre-extracted from Claude
 */

define(['N/search', 'N/log', 'N/file', 'N/record', 'N/email', 'N/runtime'], 
function (search, log, file, record, email, runtime) {

    // ===========================
    // CONFIGURATION
    // ===========================
    
    function loadConfiguration() {
        try {
            // Load configuration from custom record
            var configRecord = record.load({
                type: 'customrecord_ap_assist_vend_config',
                id: 1
            });

            return {
                FOLDERS: {
                    JSON_SOURCE: configRecord.getValue('custrecord_ap_assist_json_folder_id'),
                    JSON_PROCESSED: configRecord.getValue('custrecord_ap_assist_json_processed_fold'),
                    JSON_SKIPPED: configRecord.getValue('custrecord_ap_assist_json_skipped_fold'),
                    PDF_UNPROCESSED: configRecord.getValue('custrecord_ap_asssist_pdf_folder_id'),
                    PDF_PROCESSED: configRecord.getValue('custrecord_ap_assist_pdf_processed_fold'),
                    PDF_SKIPPED: configRecord.getValue('custrecord_ap_assist_pdf_skipped_fold')
                },
                ACCOUNTS: {
                    ACCOUNTS_PAYABLE: 111,     // Journal Entry debit line
                    ACCOUNTS_RECEIVABLE: 119,  // Journal Entry credit line
                    FREIGHT_IN: 367            // Vendor Credit delivery expense
                },
                ENTITIES: {
                    MARCONE: 2106,             // Vendor for A/P debit
                    SERVICE_DEPARTMENT: 13     // Department for delivery expense
                },
                EMAIL: {
                    RECIPIENT: configRecord.getValue('custrecord_ap_assist_tran_summ_email_rec')
                }
            };
        } catch (error) {
            log.error('Error loading configuration from custom record', {
                error: error.toString()
            });
            throw error;
        }
    }

    // ===========================
    // MAIN EXECUTION FUNCTION
    // ===========================
    
    function execute(context) {
        try {
            // Load configuration from custom record
            var CONFIG = loadConfiguration();
            
            log.audit('AP Assist Claude JSON Processor Started', {
                sourceFolderId: CONFIG.FOLDERS.JSON_SOURCE,
                timestamp: new Date().toISOString()
            });

            var stats = {
                filesFound: 0,
                filesProcessed: 0,
                journalEntriesCreated: 0,
                vendorCreditsCreated: 0,
                filesSkipped: 0,
                filesFailed: 0,
                processedFiles: [],
                skippedFiles: [],
                failedFiles: [],
                processedDetails: [],  // Detailed transaction info for email
                skippedEntries: []     // Detailed skip info for email
            };

            // Search for JSON files in source folder
            var jsonFiles = findJSONFilesInFolder(CONFIG.FOLDERS.JSON_SOURCE);
            stats.filesFound = jsonFiles.length;

            log.audit('JSON Files Found', {
                count: jsonFiles.length,
                files: jsonFiles.map(function(f) { return f.name; })
            });

            // Process each JSON file
            for (var i = 0; i < jsonFiles.length; i++) {
                var jsonFile = jsonFiles[i];
                
                log.debug('Processing JSON File', {
                    fileName: jsonFile.name,
                    fileId: jsonFile.id,
                    fileNumber: i + 1,
                    totalFiles: jsonFiles.length
                });

                var result = processJSONFile(jsonFile);
                
                // Check if file only created skipped transactions (no actual JEs or VCs)
                var hasOnlySkippedTransactions = result.success && 
                    (result.journalEntriesCreated === 0 && result.vendorCreditsCreated === 0) &&
                    (result.skippedTransactions > 0);
                
                if (result.success && !hasOnlySkippedTransactions) {
                    stats.filesProcessed++;
                    if (result.journalEntriesCreated) {
                        stats.journalEntriesCreated += result.journalEntriesCreated;
                    }
                    if (result.vendorCreditsCreated) {
                        stats.vendorCreditsCreated += result.vendorCreditsCreated;
                    }
                    stats.processedFiles.push(result);
                    
                    // Add detailed transaction info for email
                    if (result.details && result.details.journalEntries) {
                        stats.processedDetails = stats.processedDetails.concat(result.details.journalEntries);
                    }
                    if (result.details && result.details.vendorCredits) {
                        stats.processedDetails = stats.processedDetails.concat(result.details.vendorCredits);
                    }
                    if (result.details && result.details.skippedTransactions) {
                        stats.skippedEntries = stats.skippedEntries.concat(result.details.skippedTransactions);
                    }
                    
                    // Move to processed folder (JSON only - PDF stays attached to transaction)
                    moveFileToFolder(jsonFile.id, CONFIG.FOLDERS.JSON_PROCESSED);
                    
                    // Move corresponding PDF to processed folder
                    if (result.details && result.details.pdfFileId) {
                        moveFileToFolder(result.details.pdfFileId, CONFIG.FOLDERS.PDF_PROCESSED);
                        log.debug('Moved PDF to processed folder', {
                            jsonFileName: jsonFile.name,
                            pdfFileId: result.details.pdfFileId
                        });
                    } else {
                        // Try to find PDF even if not in result
                        var pdfFileId = findMatchingPDFFile(jsonFile.name, CONFIG.FOLDERS.PDF_UNPROCESSED);
                        if (pdfFileId) {
                            moveFileToFolder(pdfFileId, CONFIG.FOLDERS.PDF_PROCESSED);
                            log.debug('Moved PDF to processed folder (found separately)', {
                                jsonFileName: jsonFile.name,
                                pdfFileId: pdfFileId
                            });
                        }
                    }
                    
                } else if (result.isSkipped || hasOnlySkippedTransactions) {
                    stats.filesSkipped++;
                    
                    // Add skipped transaction details to email
                    if (hasOnlySkippedTransactions && result.details && result.details.skippedTransactions) {
                        stats.skippedEntries = stats.skippedEntries.concat(result.details.skippedTransactions);
                    }
                    
                    // Determine skip reason
                    var skipReason = result.skipReason || 'File skipped during processing';
                    if (hasOnlySkippedTransactions && result.details && result.details.skippedTransactions && result.details.skippedTransactions.length > 0) {
                        // Use the skip reason from the first skipped transaction
                        skipReason = result.details.skippedTransactions[0].skipReason || 'All transactions were skipped';
                    }
                    
                    stats.skippedFiles.push({
                        fileName: result.fileName,
                        skipReason: skipReason,
                        skippedTransactions: result.skippedTransactions || 0
                    });
                    
                    // Add skip reason to JSON file and move to skipped folder
                    var skipMetadata = {
                        processingStatus: 'SKIPPED',
                        skipReason: skipReason,
                        processedDate: new Date().toISOString(),
                        originalFileName: result.fileName,
                        skippedTransactionCount: result.skippedTransactions || 0
                    };
                    addMetadataToJSONAndMove(jsonFile.id, skipMetadata, CONFIG.FOLDERS.JSON_SKIPPED);
                    
                    // Move corresponding PDF to skipped folder
                    var pdfFileId = findMatchingPDFFile(jsonFile.name, CONFIG.FOLDERS.PDF_UNPROCESSED);
                    if (pdfFileId) {
                        moveFileToFolder(pdfFileId, CONFIG.FOLDERS.PDF_SKIPPED);
                        log.debug('Moved PDF to skipped folder', {
                            jsonFileName: jsonFile.name,
                            pdfFileId: pdfFileId
                        });
                    }
                    
                } else {
                    stats.filesFailed++;
                    stats.failedFiles.push(result);
                    
                    // Add error details to JSON file and move to skipped folder (same as skipped)
                    var failMetadata = {
                        processingStatus: 'FAILED',
                        error: result.error || 'Unknown error during processing',
                        processedDate: new Date().toISOString(),
                        originalFileName: result.fileName
                    };
                    addMetadataToJSONAndMove(jsonFile.id, failMetadata, CONFIG.FOLDERS.JSON_SKIPPED);
                    
                    // Move corresponding PDF to skipped folder
                    var pdfFileId = findMatchingPDFFile(jsonFile.name, CONFIG.FOLDERS.PDF_UNPROCESSED);
                    if (pdfFileId) {
                        moveFileToFolder(pdfFileId, CONFIG.FOLDERS.PDF_SKIPPED);
                        log.debug('Moved PDF to skipped folder (failed)', {
                            jsonFileName: jsonFile.name,
                            pdfFileId: pdfFileId
                        });
                    }
                }
            }

            // Send summary email
            sendProcessingSummaryEmail(stats);

            log.audit('AP Assist Claude JSON Processor Completed', stats);

        } catch (error) {
            log.error('Fatal Error in AP Assist Processor', {
                error: error.toString(),
                stack: error.stack
            });
            throw error;
        }
    }

    // ===========================
    // FILE DISCOVERY & PARSING
    // ===========================

    function findJSONFilesInFolder(folderId) {
        var jsonFiles = [];
        
        try {
            var fileSearch = search.create({
                type: 'file',
                filters: [
                    ['folder', 'anyof', folderId],
                    'AND',
                    ['filetype', 'anyof', 'JSON']
                ],
                columns: [
                    'name',
                    'internalid',
                    'created',
                    'modified'
                ]
            });

            fileSearch.run().each(function(result) {
                jsonFiles.push({
                    id: result.getValue('internalid'),
                    name: result.getValue('name'),
                    created: result.getValue('created'),
                    modified: result.getValue('modified')
                });
                return true;
            });

        } catch (error) {
            log.error('Error searching for JSON files', {
                error: error.toString(),
                folderId: folderId
            });
        }

        return jsonFiles;
    }

    function processJSONFile(jsonFile) {
        try {
            log.debug('Loading and parsing JSON file', {
                fileName: jsonFile.name,
                fileId: jsonFile.id
            });

            // Load the JSON file
            var fileObj = file.load({ id: jsonFile.id });
            var fileContent = fileObj.getContents();

            // Parse JSON
            var claudeData = JSON.parse(fileContent);

            log.debug('Claude JSON Parsed', {
                fileName: jsonFile.name,
                isCreditMemo: claudeData.isCreditMemo,
                invoiceNumber: claudeData.invoiceNumber,
                lineItemCount: claudeData.lineItems ? claudeData.lineItems.length : 0
            });

            // Validate it's a credit memo
            if (!claudeData.isCreditMemo) {
                return {
                    success: false,
                    isSkipped: true,
                    skipReason: 'Not a credit memo',
                    fileName: jsonFile.name,
                    fileId: jsonFile.id
                };
            }

            // Transform Claude JSON to splitPart format expected by transaction functions
            var splitPart = transformClaudeJSONToSplitPart(claudeData, jsonFile.name, jsonFile.id);

            // Find matching PDF file for attachment to transactions
            var CONFIG = loadConfiguration();
            var pdfFileId = findMatchingPDFFile(jsonFile.name, CONFIG.FOLDERS.PDF_UNPROCESSED);
            if (pdfFileId) {
                splitPart.pdfFileId = pdfFileId;
                log.debug('Found matching PDF for attachment', {
                    jsonFileName: jsonFile.name,
                    pdfFileId: pdfFileId
                });
            }

            // Call transaction creation orchestrator
            var transactionResult = createJournalEntriesFromLineItems(splitPart, jsonFile.id, CONFIG);

            if (transactionResult.success) {
                return {
                    success: true,
                    fileName: jsonFile.name,
                    fileId: jsonFile.id,
                    invoiceNumber: claudeData.invoiceNumber,
                    journalEntriesCreated: transactionResult.journalEntries ? transactionResult.journalEntries.length : 0,
                    vendorCreditsCreated: transactionResult.vendorCredits ? transactionResult.vendorCredits.length : 0,
                    skippedTransactions: transactionResult.skippedTransactions ? transactionResult.skippedTransactions.length : 0,
                    details: transactionResult,
                    pdfFileId: splitPart.pdfFileId
                };
            } else {
                return {
                    success: false,
                    fileName: jsonFile.name,
                    fileId: jsonFile.id,
                    error: transactionResult.error || 'Transaction creation failed'
                };
            }

        } catch (error) {
            log.error('Error processing JSON file', {
                error: error.toString(),
                fileName: jsonFile.name,
                fileId: jsonFile.id
            });

            return {
                success: false,
                fileName: jsonFile.name,
                fileId: jsonFile.id,
                error: error.toString()
            };
        }
    }

    // ===========================
    // DATA TRANSFORMATION
    // ===========================

    function transformClaudeJSONToSplitPart(claudeData, fileName, fileId) {
        try {
            log.debug('Transforming Claude JSON to splitPart format', {
                fileName: fileName,
                lineItems: claudeData.lineItems.length
            });

            // Group line items by NARDA number
            var lineItemsByNarda = {};
            
            for (var i = 0; i < claudeData.lineItems.length; i++) {
                var item = claudeData.lineItems[i];
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
                    // Parse amount - remove ($, ), commas
                    var amountStr = items[i].totalAmount.replace(/[()$,]/g, '');
                    var amt = parseFloat(amountStr);
                    if (!isNaN(amt)) {
                        totalAmount += Math.abs(amt); // Use absolute value
                    }
                    
                    // Collect unique original bill numbers
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

            log.debug('Data transformation complete', {
                fileName: fileName,
                nardaGroups: Object.keys(groupedLineItems),
                totalGroups: Object.keys(groupedLineItems).length
            });

            return {
                fileName: fileName,
                fileId: fileId,
                partNumber: '1',
                invoiceNumber: claudeData.invoiceNumber,
                invoiceDate: claudeData.invoiceDate,
                documentTotal: claudeData.documentTotal,
                deliveryAmount: claudeData.deliveryAmount || '$0.00',
                lineItemsByNarda: lineItemsByNarda,
                // Wrap in structure expected by transaction functions
                jsonResult: {
                    success: true,
                    extractedData: {
                        success: true,
                        invoiceNumber: claudeData.invoiceNumber,
                        invoiceDate: claudeData.invoiceDate,
                        deliveryAmount: claudeData.deliveryAmount || '$0.00',
                        lineItems: claudeData.lineItems,
                        groupedLineItems: groupedLineItems,
                        fileName: fileName,
                        extractionSuccessful: true,
                        allFieldsFound: true
                    }
                }
            };

        } catch (error) {
            log.error('Error transforming Claude JSON', {
                error: error.toString(),
                fileName: fileName
            });
            throw error;
        }
    }

    // ===========================
    // FILE MANAGEMENT
    // ===========================

    function moveFileToFolder(fileId, targetFolderId) {
        try {
            var fileObj = file.load({ id: fileId });
            fileObj.folder = targetFolderId;
            fileObj.save();
            
            log.debug('File moved to folder', {
                fileId: fileId,
                targetFolderId: targetFolderId
            });
            
            return true;
        } catch (error) {
            log.error('Error moving file to folder', {
                error: error.toString(),
                fileId: fileId,
                targetFolderId: targetFolderId
            });
            return false;
        }
    }

    function addMetadataToJSONAndMove(fileId, metadata, targetFolderId) {
        try {
            // Load the JSON file
            var fileObj = file.load({ id: fileId });
            var fileContent = fileObj.getContents();
            
            // Parse existing JSON
            var jsonData = JSON.parse(fileContent);
            
            // Add processing metadata
            jsonData._processingMetadata = metadata;
            
            // Convert back to JSON string with formatting
            var updatedContent = JSON.stringify(jsonData, null, 2);
            
            // Create new file with updated content in target folder
            var newFile = file.create({
                name: fileObj.name,
                fileType: file.Type.JSON,
                contents: updatedContent,
                folder: targetFolderId
            });
            var newFileId = newFile.save();
            
            // Delete original file
            file.delete({ id: fileId });
            
            log.debug('JSON file updated with metadata and moved', {
                originalFileId: fileId,
                newFileId: newFileId,
                targetFolderId: targetFolderId,
                metadata: JSON.stringify(metadata)
            });
            
            return newFileId;
            
        } catch (error) {
            log.error('Error adding metadata to JSON and moving', {
                error: error.toString(),
                fileId: fileId,
                targetFolderId: targetFolderId
            });
            
            // Fallback: just move the file without editing
            try {
                moveFileToFolder(fileId, targetFolderId);
            } catch (moveError) {
                log.error('Fallback move also failed', { error: moveError.toString() });
            }
            
            return false;
        }
    }

    function findMatchingPDFFile(jsonFileName, pdfFolderId) {
        try {
            // Convert JSON filename to PDF filename
            // Example: "invoice_123.json" -> "invoice_123.pdf"
            var pdfFileName = jsonFileName.replace(/\.json$/i, '.pdf');

            log.debug('Searching for matching PDF file', {
                jsonFileName: jsonFileName,
                pdfFileName: pdfFileName,
                pdfFolderId: pdfFolderId
            });

            // Search for PDF file with matching name in PDF folder
            var pdfSearch = search.create({
                type: 'file',
                filters: [
                    ['folder', 'anyof', pdfFolderId],
                    'AND',
                    ['name', 'is', pdfFileName]
                ],
                columns: ['internalid', 'name']
            });

            var pdfFileId = null;
            pdfSearch.run().each(function(result) {
                pdfFileId = result.getValue('internalid');
                return false; // Take first match
            });

            if (pdfFileId) {
                log.debug('Found matching PDF file', {
                    pdfFileName: pdfFileName,
                    pdfFileId: pdfFileId
                });
                return pdfFileId;
            } else {
                log.debug('No matching PDF file found', {
                    pdfFileName: pdfFileName,
                    pdfFolderId: pdfFolderId
                });
                return null;
            }

        } catch (error) {
            log.error('Error finding matching PDF file', {
                error: error.toString(),
                jsonFileName: jsonFileName,
                pdfFolderId: pdfFolderId
            });
            return null;
        }
    }

    // ===========================
    // EMAIL REPORTING
    // ===========================

    function sendProcessingSummaryEmail(stats) {
        try {
            // Load configuration to get email recipient
            var CONFIG = loadConfiguration();
            var recipientEmail = CONFIG.EMAIL.RECIPIENT;

            if (!recipientEmail) {
                log.debug('No email recipient configured, skipping summary email');
                return;
            }

            // Build email subject
            var subject = buildEmailSubject(stats);

            // Build comprehensive email body
            var emailBody = buildEmailBody(stats);

            // Send email
            email.send({
                author: 151135,
                recipients: [recipientEmail],
                subject: subject,
                body: emailBody
            });

            log.audit('Summary email sent', { 
                recipient: recipientEmail,
                subject: subject,
                journalEntries: stats.journalEntriesCreated,
                vendorCredits: stats.vendorCreditsCreated,
                skipped: stats.skippedEntries.length
            });

        } catch (error) {
            log.error('Error sending summary email', {
                error: error.toString()
            });
        }
    }

    function buildEmailSubject(stats) {
        var status = 'SUCCESS';

        if (stats.failedFiles.length > 0 || stats.skippedEntries.length > 0) {
            status = 'PARTIAL';
        }

        if (stats.filesProcessed === 0) {
            status = 'FAILED';
        }

        return 'AP Assist - Claude JSON Processing - ' + status + 
               ' (' + stats.filesProcessed + ' of ' + stats.filesFound + ' files)';
    }

    function buildEmailBody(stats) {
        var body = '';

        // Header
        body += 'AP ASSIST - CLAUDE API JSON PROCESSING RESULTS\n';
        body += '='.repeat(55) + '\n\n';

        // Summary Section
        body += buildSummarySection(stats);

        // Journal Entries Section
        if (stats.journalEntriesCreated > 0) {
            body += '\n\n' + buildJournalEntriesSection(stats.processedDetails);
        }

        // Vendor Credits Section
        if (stats.vendorCreditsCreated > 0) {
            body += '\n\n' + buildVendorCreditsSection(stats.processedDetails);
        }

        // Skipped Transactions Section
        if (stats.skippedEntries.length > 0) {
            body += '\n\n' + buildSkippedTransactionsSection(stats.skippedEntries);
        }

        // Failed Files Section
        if (stats.failedFiles.length > 0) {
            body += '\n\n' + buildFailedFilesSection(stats.failedFiles);
        }

        // Next Steps Section
        body += '\n\n' + buildNextStepsSection(stats);

        // Footer
        body += '\n\n' + buildEmailFooter();

        return body;
    }

    function buildSummarySection(stats) {
        var summary = 'PROCESSING SUMMARY\n';
        summary += '-'.repeat(50) + '\n';
        summary += 'JSON Files Found:            ' + stats.filesFound + '\n';
        summary += 'Files Processed:             ' + stats.filesProcessed + '\n';
        summary += 'Journal Entries Created:     ' + stats.journalEntriesCreated + '\n';
        summary += 'Vendor Credits Created:      ' + stats.vendorCreditsCreated + '\n';
        summary += 'Files Skipped:               ' + stats.filesSkipped + '\n';
        summary += 'Files Failed:                ' + stats.failedFiles.length + '\n';
        summary += 'Skipped Transactions:        ' + stats.skippedEntries.length + '\n';

        return summary;
    }

    function buildJournalEntriesSection(processedDetails) {
        var section = 'JOURNAL ENTRIES CREATED\n';
        section += '-'.repeat(50) + '\n';

        // Filter to only Journal Entries
        var journalEntries = [];
        for (var i = 0; i < processedDetails.length; i++) {
            if (processedDetails[i].journalEntryId) {
                journalEntries.push(processedDetails[i]);
            }
        }

        if (journalEntries.length === 0) {
            return '';
        }

        // Get NetSuite domain for URLs
        var domain = runtime.accountId.toLowerCase().replace('_', '-');
        var nsUrl = 'https://' + domain + '.app.netsuite.com';

        for (var i = 0; i < journalEntries.length; i++) {
            var je = journalEntries[i];

            section += '\n' + (i + 1) + '. Journal Entry\n';
            section += '   Transaction ID:  ' + je.journalEntryTranid + '\n';
            section += '   Internal ID:     ' + je.journalEntryId + '\n';
            section += '   URL:             ' + nsUrl + '/app/accounting/transactions/journal.nl?id=' +
                je.journalEntryId + '\n';

            // Show NARDA classification details
            if (je.nardaNumbers && je.nardaNumbers.length > 1) {
                section += '   NARDA Type:      Multiple Journal Entry NARDAs (Consolidated)\n';
                section += '   NARDA Numbers:   ' + je.nardaNumbers.join(', ') + '\n';
                section += '   NARDA Count:     ' + je.nardaNumbers.length + '\n';
            } else if (je.nardaNumber) {
                section += '   NARDA Type:      Journal Entry (J# or INV#)\n';
                section += '   NARDA Number:    ' + je.nardaNumber + '\n';
            }

            // Show dollar amount
            if (je.totalAmount) {
                section += '   Total Amount:    ' + formatCurrency(je.totalAmount) + '\n';
            }
            
            // Show documentTotal from PDF if available
            if (je.extractedData && je.extractedData.documentTotal) {
                section += '   Document Total:  ' + je.extractedData.documentTotal + '\n';
            }
            
            // Show delivery amount from PDF
            if (je.extractedData && je.extractedData.deliveryAmount) {
                section += '   Delivery Amount: ' + je.extractedData.deliveryAmount + '\n';
            }

            // Show PDF file link if available
            if (je.pdfFileId) {
                try {
                    var pdfFile = file.load({ id: je.pdfFileId });
                    var pdfUrl = 'https://system.netsuite.com' + pdfFile.url;
                    section += '   PDF File URL:    ' + pdfUrl + '\n';
                    log.debug('Adding PDF URL to JE email section', {
                        journalEntryId: je.journalEntryId,
                        pdfFileId: je.pdfFileId,
                        pdfUrl: pdfUrl
                    });
                } catch (e) {
                    log.error('Error loading PDF file for URL', {
                        journalEntryId: je.journalEntryId,
                        pdfFileId: je.pdfFileId,
                        error: e.toString()
                    });
                }
            } else {
                log.debug('No PDF file ID for JE', {
                    journalEntryId: je.journalEntryId,
                    jeObject: JSON.stringify(je)
                });
            }
        }

        return section;
    }

    function buildVendorCreditsSection(processedDetails) {
        var section = 'VENDOR CREDITS CREATED\n';
        section += '-'.repeat(50) + '\n';

        // Filter to only Vendor Credits
        var vendorCredits = [];
        for (var i = 0; i < processedDetails.length; i++) {
            if (processedDetails[i].vendorCreditId || processedDetails[i].isVendorCredit) {
                vendorCredits.push(processedDetails[i]);
            }
        }

        if (vendorCredits.length === 0) {
            return '';
        }

        // Get NetSuite domain for URLs
        var domain = runtime.accountId.toLowerCase().replace('_', '-');
        var nsUrl = 'https://' + domain + '.app.netsuite.com';

        for (var i = 0; i < vendorCredits.length; i++) {
            var vc = vendorCredits[i];

            section += '\n' + (i + 1) + '. Vendor Credit\n';
            section += '   Transaction ID:        ' + vc.vendorCreditTranid + '\n';
            section += '   Internal ID:           ' + vc.vendorCreditId + '\n';
            section += '   URL:                   ' + nsUrl + '/app/accounting/transactions/vendcred.nl?id=' +
                vc.vendorCreditId + '\n';

            // Show NARDA classification
            if (vc.nardaTypes && vc.nardaTypes.length > 0) {
                section += '   NARDA Type:            Vendor Credit (' + vc.nardaTypes.join('+') + ')\n';
                if (vc.nardaTypes.length > 1) {
                    section += '   NARDA Count:           ' + vc.nardaTypes.length + ' types consolidated\n';
                }
            } else if (vc.nardaNumber) {
                section += '   NARDA Type:            Vendor Credit\n';
                section += '   NARDA Number:          ' + vc.nardaNumber + '\n';
            }

            if (vc.originalBillNumber) {
                section += '   Original Bill Number:  ' + vc.originalBillNumber + '\n';
            }
            
            if (vc.matchedLineCount) {
                section += '   Lines Matched:         ' + vc.matchedLineCount + '\n';
            }

            // Show dollar amount
            if (vc.totalAmount) {
                section += '   Total Amount:          ' + formatCurrency(vc.totalAmount) + '\n';
            }
            
            // Show documentTotal from PDF if available
            if (vc.extractedData && vc.extractedData.documentTotal) {
                section += '   Document Total:        ' + vc.extractedData.documentTotal + '\n';
            }
            
            // Show delivery amount from PDF
            if (vc.extractedData && vc.extractedData.deliveryAmount) {
                section += '   Delivery Amount:       ' + vc.extractedData.deliveryAmount + '\n';
            }

            // Add VRMA reference if available
            if (vc.matchingVRMA) {
                section += '   Source VRMA ID:         ' + vc.matchingVRMA.internalId + '\n';
                section += '   Source VRMA #:          ' + vc.matchingVRMA.tranid + '\n';
                section += '   VRMA URL:               ' + nsUrl + '/app/accounting/transactions/vendauth.nl?id=' +
                    vc.matchingVRMA.internalId + '\n';
            }

            // Show PDF file link if available
            if (vc.pdfFileId) {
                try {
                    var pdfFile = file.load({ id: vc.pdfFileId });
                    var pdfUrl = 'https://system.netsuite.com' + pdfFile.url;
                    section += '   PDF File URL:           ' + pdfUrl + '\n';
                    log.debug('Adding PDF URL to VC email section', {
                        vendorCreditId: vc.vendorCreditId,
                        pdfFileId: vc.pdfFileId,
                        pdfUrl: pdfUrl
                    });
                } catch (e) {
                    log.error('Error loading PDF file for URL', {
                        vendorCreditId: vc.vendorCreditId,
                        pdfFileId: vc.pdfFileId,
                        error: e.toString()
                    });
                }
            } else {
                log.debug('No PDF file ID for VC', {
                    vendorCreditId: vc.vendorCreditId,
                    vcObject: JSON.stringify(vc)
                });
            }
        }

        return section;
    }

    function buildSkippedTransactionsSection(skippedEntries) {
        var section = 'SKIPPED TRANSACTIONS (Manual Processing Required)\n';
        section += '-'.repeat(50) + '\n';

        // Get NetSuite domain for URLs
        var domain = runtime.accountId.toLowerCase().replace('_', '-');
        var nsUrl = 'https://' + domain + '.app.netsuite.com';

        // Group by skip type
        var skipGroups = {};

        for (var i = 0; i < skippedEntries.length; i++) {
            var entry = skippedEntries[i];
            var skipType = entry.skipType || 'UNKNOWN';

            if (!skipGroups[skipType]) {
                skipGroups[skipType] = [];
            }

            skipGroups[skipType].push(entry);
        }

        // Output each skip type group
        var skipTypes = Object.keys(skipGroups);
        for (var i = 0; i < skipTypes.length; i++) {
            var skipType = skipTypes[i];
            var entries = skipGroups[skipType];

            section += '\n' + getSkipTypeDescription(skipType) + ' (' + entries.length + '):  \n';

            for (var j = 0; j < entries.length; j++) {
                var entry = entries[j];
                
                if (entry.nardaNumber) {
                    section += '  - NARDA: ' + entry.nardaNumber;
                }

                if (entry.originalBillNumber) {
                    section += ', Original Bill: ' + entry.originalBillNumber;
                }

                if (entry.totalAmount) {
                    section += ', Amount: ' + formatCurrency(entry.totalAmount);
                }

                section += '\n    Reason: ' + entry.skipReason + '\n';
                
                // Add PDF file link if available
                if (entry.pdfFileId) {
                    try {
                        var pdfFile = file.load({ id: entry.pdfFileId });
                        var pdfUrl = 'https://system.netsuite.com' + pdfFile.url;
                        section += '    PDF URL: ' + pdfUrl + '\n';
                        log.debug('Adding PDF URL to skipped transaction email section', {
                            skipType: entry.skipType,
                            nardaNumber: entry.nardaNumber,
                            pdfFileId: entry.pdfFileId,
                            fileName: entry.fileName,
                            pdfUrl: pdfUrl
                        });
                    } catch (e) {
                        log.error('Error loading PDF file for URL', {
                            skipType: entry.skipType,
                            pdfFileId: entry.pdfFileId,
                            error: e.toString()
                        });
                    }
                } else {
                    log.debug('No PDF file ID for skipped transaction', {
                        skipType: entry.skipType,
                        nardaNumber: entry.nardaNumber,
                        fileName: entry.fileName,
                        entryObject: JSON.stringify(entry)
                    });
                }
                
                // Add filename for reference
                if (entry.fileName) {
                    section += '    File: ' + entry.fileName + '\n';
                }
            }
        }

        return section;
    }

    function getSkipTypeDescription(skipType) {
        var descriptions = {
            'SHORT_SHIP': 'SHORT/BOX - Requires Short Ship Processing',
            'UNIDENTIFIED_NARDA': 'Unidentified NARDA - Requires Manual Review',
            'NO_VRMA_MATCH': 'No Vendor Return Authorization Found',
            'DUPLICATE_JOURNAL_ENTRY': 'Duplicate Journal Entry Detected',
            'DUPLICATE_VENDOR_CREDIT': 'Duplicate Vendor Credit Detected',
            'UNKNOWN': 'Unknown/Other Issues'
        };

        return descriptions[skipType] || skipType;
    }

    function buildFailedFilesSection(failedFiles) {
        var section = 'FAILED FILES (Require Investigation)\n';
        section += '-'.repeat(50) + '\n';

        for (var i = 0; i < failedFiles.length; i++) {
            var fileEntry = failedFiles[i];

            section += '\n' + (i + 1) + '. File: ' + fileEntry.fileName + '\n';
            section += '   Error: ' + fileEntry.error + '\n';
            
            // Try to get PDF URL if available
            if (fileEntry.fileName) {
                try {
                    var CONFIG = loadConfiguration();
                    var pdfFileId = findMatchingPDFFile(fileEntry.fileName, CONFIG.FOLDERS.PDF_UNPROCESSED);
                    if (pdfFileId) {
                        var pdfFile = file.load({ id: pdfFileId });
                        var pdfUrl = 'https://system.netsuite.com' + pdfFile.url;
                        section += '   PDF URL: ' + pdfUrl + '\n';
                    }
                } catch (e) {
                    log.debug('Could not get PDF URL for failed file', {
                        fileName: fileEntry.fileName,
                        error: e.toString()
                    });
                }
            }
        }

        return section;
    }

    function buildNextStepsSection(stats) {
        var steps = 'NEXT STEPS\n';
        steps += '-'.repeat(50) + '\n';

        var actionItems = [];

        // Manual review for skipped transactions
        if (stats.skippedEntries.length > 0) {
            actionItems.push('Review ' + stats.skippedEntries.length +
                ' skipped transactions listed above and process manually as needed');
            actionItems.push('Common skip reasons: SHORT/BOX patterns, unidentified NARDA, ' +
                'no matching VRMA found, or duplicate detection');
        }

        // Investigation for failed files
        if (stats.failedFiles.length > 0) {
            actionItems.push('Investigate ' + stats.failedFiles.length +
                ' failed files and correct underlying issues');
        }

        // Success message
        if (actionItems.length === 0) {
            actionItems.push('All files processed successfully - no action required');
        }

        for (var i = 0; i < actionItems.length; i++) {
            steps += '\n' + (i + 1) + '. ' + actionItems[i];
        }

        return steps;
    }

    function buildEmailFooter() {
        var footer = '\n' + '-'.repeat(50) + '\n';
        footer += 'Generated by: AP Assist - Claude API JSON to Marcone Bill Credits\n';
        footer += 'Execution Time: ' + new Date().toString() + '\n';
        footer += 'Script ID: ' + runtime.getCurrentScript().id + '\n';
        footer += 'Deployment ID: ' + runtime.getCurrentScript().deploymentId;

        return footer;
    }

    function formatCurrency(amount) {
        if (amount === null || amount === undefined) {
            return '$0.00';
        }
        
        var num = parseFloat(amount);
        if (isNaN(num)) {
            return '$0.00';
        }
        
        return '$' + num.toFixed(2).replace(/\d(?=(\d{3})+\.)/g, '$&,');
    }

    // ===========================
    // TRANSACTION CREATION FUNCTIONS
    // (UNCHANGED FROM ORIGINAL - Lines 2596-7083)
    // ===========================

    function createJournalEntriesFromLineItems(splitPart, recordId, CONFIG) {
        try {
            // Get extracted line items from JSON results
            var extractedData = null;
            if (splitPart.jsonResult && splitPart.jsonResult.success && splitPart.jsonResult.extractedData) {
                extractedData = splitPart.jsonResult.extractedData;
            }

            if (!extractedData || !extractedData.success || !extractedData.groupedLineItems) {
                return { success: false, error: 'Missing required line item data from JSON results' };
            }

            // Check if groupedLineItems is empty
            var totalNardaGroups = Object.keys(extractedData.groupedLineItems);
            if (totalNardaGroups.length === 0) {
                return { success: false, error: 'No NARDA groups found in extracted data' };
            }

            var journalEntryResults = [];
            var vendorCreditResults = [];
            var skippedTransactions = [];

            log.debug('Processing NARDA groups for journal entries and vendor credits', {
                totalGroups: totalNardaGroups.length,
                groups: totalNardaGroups,
                fileName: splitPart.fileName,
                recordId: recordId
            });

            // Separate J####/INV#### groups from vendor credit groups
            var journalEntryGroups = [];
            var vendorCreditNardaNumbers = [];

            for (var i = 0; i < totalNardaGroups.length; i++) {
                var nardaNumber = totalNardaGroups[i];
                var nardaGroup = extractedData.groupedLineItems[nardaNumber];

                // Check if this is a vendor credit NARDA
                if (nardaNumber && (nardaNumber.toUpperCase() === 'CONCDA' ||
                    nardaNumber.toUpperCase() === 'CONCDAM' ||
                    nardaNumber.toUpperCase() === 'NF' ||
                    nardaNumber.toUpperCase() === 'CORE' ||
                    nardaNumber.toUpperCase() === 'CONCESSION')) {
                    vendorCreditNardaNumbers.push(nardaNumber);
                } else if (nardaNumber && (nardaNumber.toUpperCase().match(/^J\d{4,6}$/) ||
                    nardaNumber.toUpperCase().match(/^INV\d+$/))) {
                    journalEntryGroups.push({ nardaNumber: nardaNumber, nardaGroup: nardaGroup });
                } else if (nardaNumber && (nardaNumber.toUpperCase() === 'SHORT' ||
                    nardaNumber.toUpperCase() === 'BOX')) {
                    // Handle SHORT and BOX separately
                    log.debug('SHORT/BOX NARDA detected - skipping for manual short ship processing', {
                        nardaNumber: nardaNumber,
                        totalAmount: nardaGroup.totalAmount,
                        fileName: splitPart.fileName,
                        recordId: recordId
                    });

                    skippedTransactions.push({
                        success: true,
                        isSkipped: true,
                        skipReason: nardaNumber.toUpperCase() + ' NARDA - requires manual short ship processing',
                        skipType: 'SHORT_SHIP',
                        nardaNumber: nardaNumber,
                        totalAmount: nardaGroup.totalAmount,
                        extractedData: extractedData,
                        pdfFileId: splitPart.pdfFileId,
                        fileName: splitPart.fileName
                    });
                } else {
                    // Unidentified NARDA value - skip for manual review
                    log.debug('Unidentified NARDA value - skipping for manual review', {
                        nardaNumber: nardaNumber,
                        totalAmount: nardaGroup.totalAmount,
                        fileName: splitPart.fileName,
                        recordId: recordId
                    });

                    skippedTransactions.push({
                        success: true,
                        isSkipped: true,
                        skipReason: 'Unidentified NARDA value: ' + nardaNumber + ' - requires manual review',
                        skipType: 'UNIDENTIFIED_NARDA',
                        nardaNumber: nardaNumber,
                        totalAmount: nardaGroup.totalAmount,
                        extractedData: extractedData,
                        pdfFileId: splitPart.pdfFileId,
                        fileName: splitPart.fileName
                    });
                }
            }

            // JOURNAL ENTRY PROCESSING (unchanged existing logic)
            if (journalEntryGroups.length > 1) {
                // Multiple J####/INV#### groups - create single consolidated journal entry
                log.debug('Multiple journal entry groups detected - creating consolidated journal entry', {
                    groupCount: journalEntryGroups.length,
                    groups: journalEntryGroups.map(function (g) { return g.nardaNumber; }),
                    fileName: splitPart.fileName,
                    recordId: recordId
                });

                var consolidatedResult = createSingleJournalEntryWithMultipleLines(splitPart, recordId, extractedData, journalEntryGroups);

                if (consolidatedResult.success) {
                    if (consolidatedResult.isSkipped) {
                        skippedTransactions.push(consolidatedResult);
                    } else {
                        journalEntryResults.push(consolidatedResult);
                        log.debug('Added consolidated JE to results array', {
                            journalEntryId: consolidatedResult.journalEntryId,
                            pdfFileId: consolidatedResult.pdfFileId,
                            hasPdfFileId: !!consolidatedResult.pdfFileId,
                            resultObject: JSON.stringify(consolidatedResult)
                        });
                    }
                } else {
                    // Handle consolidation failure
                    if (consolidatedResult.isDuplicate) {
                        skippedTransactions.push({
                            success: true,
                            isSkipped: true,
                            skipReason: consolidatedResult.error,
                            skipType: 'DUPLICATE_JOURNAL_ENTRY',
                            nardaNumber: 'Multiple: ' + journalEntryGroups.map(function (g) { return g.nardaNumber; }).join(', '),
                            totalAmount: journalEntryGroups.reduce(function (sum, g) { return sum + g.nardaGroup.totalAmount; }, 0),
                            extractedData: extractedData,
                            existingJournalEntry: consolidatedResult.existingJournalEntry,
                            pdfFileId: splitPart.pdfFileId,
                            fileName: splitPart.fileName
                        });
                    } else {
                        return {
                            success: false,
                            error: consolidatedResult.error
                        };
                    }
                }
            } else if (journalEntryGroups.length === 1) {
                // Single J####/INV#### group - process individually
                var singleGroup = journalEntryGroups[0];

                log.debug('Processing individual NARDA group', {
                    nardaNumber: singleGroup.nardaNumber,
                    totalAmount: singleGroup.nardaGroup.totalAmount,
                    fileName: splitPart.fileName,
                    recordId: recordId
                });

                var jeResult = createJournalEntryFromNardaGroup(splitPart, recordId, extractedData, singleGroup.nardaGroup, singleGroup.nardaNumber, CONFIG);

                if (jeResult.success) {
                    if (jeResult.isSkipped) {
                        skippedTransactions.push(jeResult);
                    } else {
                        journalEntryResults.push(jeResult);
                        log.debug('Added single NARDA JE to results array', {
                            journalEntryId: jeResult.journalEntryId,
                            nardaNumber: singleGroup.nardaNumber,
                            pdfFileId: jeResult.pdfFileId,
                            hasPdfFileId: !!jeResult.pdfFileId,
                            resultObject: JSON.stringify(jeResult)
                        });
                    }
                } else {
                    // Journal entry creation failed
                    if (jeResult.isDuplicate) {
                        skippedTransactions.push({
                            success: true,
                            isSkipped: true,
                            skipReason: jeResult.error,
                            skipType: 'DUPLICATE_JOURNAL_ENTRY',
                            nardaNumber: singleGroup.nardaNumber,
                            totalAmount: singleGroup.nardaGroup.totalAmount,
                            extractedData: extractedData,
                            existingJournalEntry: jeResult.existingJournalEntry,
                            pdfFileId: splitPart.pdfFileId,
                            fileName: splitPart.fileName
                        });
                    } else {
                        return {
                            success: false,
                            error: jeResult.error
                        };
                    }
                }
            }

            // VENDOR CREDIT PROCESSING - Group by original bill number FIRST
            var consolidatedVCGroups = consolidateVendorCreditGroups(extractedData.groupedLineItems);

            log.debug('Processing consolidated vendor credit groups', {
                totalBillNumbers: Object.keys(consolidatedVCGroups).length,
                billNumbers: Object.keys(consolidatedVCGroups),
                fileName: splitPart.fileName,
                recordId: recordId
            });

            // Process each bill number group
            var billNumbers = Object.keys(consolidatedVCGroups);
            for (var i = 0; i < billNumbers.length; i++) {
                var billNumber = billNumbers[i];
                var billGroup = consolidatedVCGroups[billNumber];

                log.debug('Processing vendor credit for bill number', {
                    billNumber: billNumber,
                    nardaTypes: billGroup.nardaTypes,
                    lineItemCount: billGroup.lineItems.length,
                    totalAmount: billGroup.totalAmount,
                    fileName: splitPart.fileName,
                    recordId: recordId
                });

                // Create a consolidated NARDA group for this bill number
                var consolidatedNardaGroup = {
                    nardaNumber: billGroup.nardaTypes.join('+'), // e.g., "NF+CORE"
                    lineItems: billGroup.lineItems,
                    totalAmount: billGroup.totalAmount,
                    originalBillNumbers: [billNumber],
                    allNardaTypes: billGroup.nardaTypes
                };

                // Search for matching VRMA (existing logic, unchanged)
                var VRMAResults = searchForMatchingVRMA(billNumber, recordId);

                if (VRMAResults.length > 0) {
                    // Attempt to create vendor credit from VRMA (existing logic, unchanged)
                    var vcResult = createVendorCreditFromVRMA(
                        splitPart,
                        recordId,
                        extractedData,
                        consolidatedNardaGroup,
                        VRMAResults,
                        billNumber
                    );

                    if (vcResult.success) {
                        if (vcResult.isVendorCredit) {
                            var vcResultObj = {
                                success: true,
                                isVendorCredit: true,
                                vendorCreditId: vcResult.vendorCreditId,
                                vendorCreditTranid: vcResult.vendorCreditTranid,
                                nardaNumber: consolidatedNardaGroup.nardaNumber,
                                nardaTypes: billGroup.nardaTypes,
                                totalAmount: vcResult.totalAmount,
                                matchedLineCount: vcResult.matchedLineCount,
                                originalBillNumber: billNumber,
                                matchingVRMA: vcResult.matchingVRMA,
                                extractedData: extractedData,
                                attachmentResult: vcResult.attachmentResult,
                                pdfFileId: splitPart.pdfFileId,
                                fileName: splitPart.fileName
                            };
                            vendorCreditResults.push(vcResultObj);

                            log.debug('Vendor credit created successfully - added to results array', {
                                vendorCreditId: vcResult.vendorCreditId,
                                billNumber: billNumber,
                                nardaTypes: billGroup.nardaTypes,
                                combinedNarda: consolidatedNardaGroup.nardaNumber,
                                pdfFileId: splitPart.pdfFileId,
                                hasPdfFileId: !!splitPart.pdfFileId,
                                fileName: splitPart.fileName,
                                vcResultObj: JSON.stringify(vcResultObj)
                            });
                        } else if (vcResult.isSkipped) {
                            skippedTransactions.push({
                                success: true,
                                isSkipped: true,
                                skipReason: vcResult.skipReason,
                                skipType: 'NO_VRMA_MATCH',
                                nardaNumber: consolidatedNardaGroup.nardaNumber,
                                nardaTypes: billGroup.nardaTypes,
                                totalAmount: billGroup.totalAmount,
                                originalBillNumber: billNumber,
                                extractedData: extractedData,
                                matchingVRMA: vcResult.matchingVRMA,
                                pdfFileId: splitPart.pdfFileId,
                                fileName: splitPart.fileName
                            });
                        }
                    } else {
                        // Handle vendor credit creation failure
                        if (vcResult.isDuplicate) {
                            skippedTransactions.push({
                                success: true,
                                isSkipped: true,
                                skipReason: vcResult.error,
                                skipType: 'DUPLICATE_VENDOR_CREDIT',
                                nardaNumber: consolidatedNardaGroup.nardaNumber,
                                nardaTypes: billGroup.nardaTypes,
                                totalAmount: billGroup.totalAmount,
                                originalBillNumber: billNumber,
                                extractedData: extractedData,
                                existingVendorCredit: vcResult.existingVendorCredit,
                                pdfFileId: splitPart.pdfFileId,
                                fileName: splitPart.fileName
                            });
                        } else {
                            return {
                                success: false,
                                error: vcResult.error
                            };
                        }
                    }
                } else {
                    log.debug('No VRMA found for bill number', {
                        billNumber: billNumber,
                        nardaTypes: billGroup.nardaTypes,
                        fileName: splitPart.fileName,
                        recordId: recordId
                    });

                    skippedTransactions.push({
                        success: true,
                        isSkipped: true,
                        skipReason: 'No VRMA found with matching bill number: ' + billNumber,
                        skipType: 'NO_VRMA_MATCH',
                        nardaNumber: consolidatedNardaGroup.nardaNumber,
                        nardaTypes: billGroup.nardaTypes,
                        totalAmount: billGroup.totalAmount,
                        originalBillNumber: billNumber,
                        extractedData: extractedData,
                        pdfFileId: splitPart.pdfFileId,
                        fileName: splitPart.fileName
                    });
                }
            }

            log.debug('Completed processing all NARDA groups', {
                totalGroups: totalNardaGroups.length,
                journalEntries: journalEntryResults.length,
                vendorCredits: vendorCreditResults.length,
                skipped: skippedTransactions.length,
                fileName: splitPart.fileName,
                recordId: recordId
            });

            return {
                success: true,
                journalEntries: journalEntryResults,
                vendorCredits: vendorCreditResults,
                skippedTransactions: skippedTransactions,
                totalNARDAGroups: totalNardaGroups.length
            };

        } catch (error) {
            log.error('Error creating journal entries from line items', {
                error: error.toString(),
                fileName: splitPart.fileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function checkForDuplicateJournalEntry(tranid, recordId) {
        try {
            log.debug('Checking for duplicate journal entry', {
                tranid: tranid,
                recordId: recordId
            });

            // Search for existing journal entries with the same tranid
            var journalEntrySearch = search.create({
                type: search.Type.JOURNAL_ENTRY,
                filters: [
                    ['type', 'anyof', 'Journal'],
                    'AND',
                    ['tranid', 'is', tranid]
                ],
                columns: [
                    'tranid',
                    'trandate',
                    'memo',
                    'internalid',
                    'entity'
                ]
            });

            var existingEntries = [];
            journalEntrySearch.run().each(function (result) {
                existingEntries.push({
                    internalId: result.getValue('internalid'),
                    tranid: result.getValue('tranid'),
                    trandate: result.getValue('trandate'),
                    memo: result.getValue('memo'),
                    entity: result.getValue('entity')
                });
                return true; // Continue to get all results
            });

            if (existingEntries.length > 0) {
                log.debug('Duplicate journal entry found', {
                    tranid: tranid,
                    existingEntries: existingEntries,
                    totalDuplicates: existingEntries.length,
                    recordId: recordId
                });

                return {
                    success: false,
                    existingEntry: existingEntries[0],
                    allDuplicates: existingEntries,
                    duplicateCount: existingEntries.length
                };
            } else {
                log.debug('No duplicate journal entry found - safe to create', {
                    tranid: tranid,
                    recordId: recordId
                });

                return {
                    success: true,
                    tranid: tranid
                };
            }

        } catch (error) {
            log.error('Error checking for duplicate journal entry', {
                error: error.toString(),
                tranid: tranid,
                recordId: recordId
            });

            // If we can't check for duplicates, err on the side of caution
            return {
                success: false,
                error: 'Could not verify uniqueness due to search error: ' + error.toString()
            };
        }
    }

    function checkForDuplicateVendorCredit(tranid, recordId) {
        try {
            log.debug('Checking for duplicate vendor credit', {
                tranid: tranid,
                recordId: recordId
            });

            // Search for existing vendor credits with the same tranid
            var vendorCreditSearch = search.create({
                type: search.Type.VENDOR_CREDIT,
                filters: [
                    ['type', 'anyof', 'VendCred'],
                    'AND',
                    ['tranid', 'is', tranid]
                ],
                columns: [
                    'tranid',
                    'trandate',
                    'memo',
                    'internalid',
                    'entity'
                ]
            });

            var existingEntries = [];
            vendorCreditSearch.run().each(function (result) {
                existingEntries.push({
                    internalId: result.getValue('internalid'),
                    tranid: result.getValue('tranid'),
                    trandate: result.getValue('trandate'),
                    memo: result.getValue('memo'),
                    entity: result.getValue('entity')
                });
                return true;
            });

            if (existingEntries.length > 0) {
                log.error('Duplicate vendor credit found', {
                    tranid: tranid,
                    existingEntries: existingEntries,
                    recordId: recordId
                });

                return {
                    success: false,
                    existingEntry: existingEntries[0]
                };
            } else {
                log.debug('No duplicate vendor credit found, proceeding', {
                    tranid: tranid,
                    recordId: recordId
                });

                return {
                    success: true
                };
            }

        } catch (error) {
            log.error('Error checking for duplicate vendor credit', {
                error: error.toString(),
                tranid: tranid,
                recordId: recordId
            });

            // If we can't check for duplicates, err on the side of caution
            return {
                success: false,
                error: 'Could not verify uniqueness due to search error: ' + error.toString()
            };
        }
    }

    function findCreditLineEntity(nardaNumber, recordId) {
        try {
            log.debug('Searching for Credit Line Entity', {
                nardaNumber: nardaNumber,
                recordId: recordId
            });

            // Search for open invoices with the NARDA number in custbody_f4n_job_id
            var invoiceSearch = search.create({
                type: search.Type.INVOICE,
                filters: [
                    ['type', 'anyof', 'CustInvc'],
                    'AND',
                    ['status', 'anyof', 'CustInvc:A'], // Open invoices
                    'AND',
                    ['custbody_f4n_job_id', 'is', nardaNumber]
                ],
                columns: [
                    'entity',
                    'trandate',
                    'tranid',
                    'internalid'
                ]
            });

            var searchResults = [];
            invoiceSearch.run().each(function (result) {
                searchResults.push({
                    entityId: result.getValue('entity'),
                    tranDate: new Date(result.getValue('trandate')),
                    tranid: result.getValue('tranid'),
                    internalId: result.getValue('internalid')
                });
                return true;
            });

            // If no results found, search against the invoice tranid
            if (searchResults.length === 0) {
                log.debug('No matches found in custbody_f4n_job_id, searching against invoice tranid', {
                    nardaNumber: nardaNumber,
                    recordId: recordId
                });

                var invoiceSearchByTranid = search.create({
                    type: search.Type.INVOICE,
                    filters: [
                        ['type', 'anyof', 'CustInvc'],
                        'AND',
                        ['status', 'anyof', 'CustInvc:A'], // Open invoices
                        'AND',
                        ['tranid', 'is', nardaNumber]
                    ],
                    columns: [
                        'entity',
                        'trandate',
                        'tranid',
                        'internalid'
                    ]
                });

                invoiceSearchByTranid.run().each(function (result) {
                    searchResults.push({
                        entityId: result.getValue('entity'),
                        tranDate: new Date(result.getValue('trandate')),
                        tranid: result.getValue('tranid'),
                        internalId: result.getValue('internalid')
                    });
                    return true;
                });
            }

            // NO FALLBACK - Return failure if no open invoices found
            if (searchResults.length === 0) {
                log.debug('No open invoices found for NARDA - cannot create journal entry without customer entity', {
                    nardaNumber: nardaNumber,
                    recordId: recordId,
                    searchedStatuses: ['CustInvc:A (Open invoices)'],
                    searchedFields: ['custbody_f4n_job_id', 'tranid']
                });

                return {
                    success: false,
                    error: 'No open invoices found with NARDA number: ' + nardaNumber + ' - Cannot determine customer entity for journal entry credit line',
                    reason: 'NO_MATCHING_OPEN_INVOICE',
                    nardaNumber: nardaNumber
                };
            }

            // Sort by date (most recent first)
            searchResults.sort(function (a, b) {
                return b.tranDate - a.tranDate;
            });

            var mostRecentInvoice = searchResults[0];

            log.debug('Credit Line Entity Found', {
                nardaNumber: nardaNumber,
                entityId: mostRecentInvoice.entityId,
                invoiceTranid: mostRecentInvoice.tranid,
                tranDate: mostRecentInvoice.tranDate,
                totalResults: searchResults.length,
                recordId: recordId
            });

            return {
                success: true,
                entityId: mostRecentInvoice.entityId,
                invoiceTranid: mostRecentInvoice.tranid,
                tranDate: mostRecentInvoice.tranDate,
                searchResultCount: searchResults.length
            };

        } catch (error) {
            log.error('Error finding Credit Line Entity', {
                error: error.toString(),
                nardaNumber: nardaNumber,
                recordId: recordId
            });
            return {
                success: false,
                error: 'Search error: ' + error.toString(),
                reason: 'SEARCH_ERROR',
                nardaNumber: nardaNumber
            };
        }
    }

    function createSingleJournalEntryWithMultipleLines(splitPart, recordId, extractedData, journalEntryGroups) {
        try {
            log.debug('Creating single journal entry with multiple NARDA lines', {
                fileName: splitPart.fileName,
                recordId: recordId,
                nardaGroups: journalEntryGroups.map(function (g) { return g.nardaNumber; }),
                invoiceNumber: extractedData.invoiceNumber,
                invoiceDate: extractedData.invoiceDate
            });

            // Validate required data
            if (!extractedData.invoiceNumber || !extractedData.invoiceDate) {
                return { success: false, error: 'Missing required invoice number or date' };
            }

            var tranid = extractedData.invoiceNumber + ' CM';

            // Check for duplicate journal entry
            var duplicateCheck = checkForDuplicateJournalEntry(tranid, recordId);
            if (!duplicateCheck.success) {
                return {
                    success: false,
                    error: 'Duplicate journal entry exists with tranid: ' + tranid,
                    isDuplicate: true,
                    existingJournalEntry: duplicateCheck.existingEntry
                };
            }

            // Parse the date
            var jeDate = new Date(extractedData.invoiceDate);
            if (isNaN(jeDate.getTime())) {
                return { success: false, error: 'Invalid invoice date: ' + extractedData.invoiceDate };
            }

            // Calculate total amount across all NARDA groups
            var grandTotal = 0;
            var nardaNumbers = [];
            for (var i = 0; i < journalEntryGroups.length; i++) {
                var group = journalEntryGroups[i];
                nardaNumbers.push(group.nardaNumber);
                grandTotal += group.nardaGroup.totalAmount;
            }

            // Create main memo
            var mainMemo;
            if (nardaNumbers.length > 1) {
                mainMemo = 'MARCONE CM' + extractedData.invoiceNumber + ' Multi-NARDA Groups';
            } else {
                var singleNarda = nardaNumbers[0];
                var group = journalEntryGroups[0];
                if (group.nardaGroup.lineItems && group.nardaGroup.lineItems.length > 1) {
                    mainMemo = 'MARCONE CM' + extractedData.invoiceNumber + ' Consolidated ' + singleNarda;
                } else {
                    mainMemo = 'MARCONE CM' + extractedData.invoiceNumber + ' ' + singleNarda;
                }
            }

            // Create Journal Entry
            var journalEntry = record.create({
                type: record.Type.JOURNAL_ENTRY,
                isDynamic: true
            });

            // Set header fields
            journalEntry.setValue({
                fieldId: 'tranid',
                value: tranid
            });

            journalEntry.setValue({
                fieldId: 'trandate',
                value: jeDate
            });

            journalEntry.setValue({
                fieldId: 'memo',
                value: mainMemo
            });

            // First line - Debit to Account 111 (Accounts Payable) for total amount
            journalEntry.selectNewLine({
                sublistId: 'line'
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'account',
                value: CONFIG.ACCOUNTS.ACCOUNTS_PAYABLE
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'debit',
                value: grandTotal
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'memo',
                value: mainMemo
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'entity',
                value: CONFIG.ENTITIES.MARCONE
            });

            journalEntry.commitLine({
                sublistId: 'line'
            });

            // Add credit lines - one for each NARDA group
            for (var i = 0; i < journalEntryGroups.length; i++) {
                var groupData = journalEntryGroups[i];
                var nardaNumber = groupData.nardaNumber;
                var nardaGroup = groupData.nardaGroup;

                // Find Credit Line Entity based on NARDA number
                var creditLineEntity = findCreditLineEntity(nardaNumber, recordId);
                if (!creditLineEntity.success) {
                    if (creditLineEntity.reason === 'NO_MATCHING_OPEN_INVOICE') {
                        log.debug('NARDA has no matching open invoices - skipping journal entry creation', {
                            nardaNumber: nardaNumber,
                            error: creditLineEntity.error,
                            recordId: recordId
                        });

                        return {
                            success: true,
                            isSkipped: true,
                            skipReason: 'No open invoices found for NARDA ' + nardaNumber + ' - cannot determine customer for credit line',
                            skipType: 'NO_MATCHING_OPEN_INVOICE',
                            nardaNumber: nardaNumber,
                            totalAmount: nardaGroup.totalAmount,
                            extractedData: extractedData,
                            pdfFileId: splitPart.pdfFileId,
                            fileName: splitPart.fileName
                        };
                    } else {
                        log.error('Could not find Credit Line Entity due to error', {
                            nardaNumber: nardaNumber,
                            error: creditLineEntity.error,
                            recordId: recordId
                        });
                        return {
                            success: false,
                            error: 'Could not find Credit Line Entity: ' + creditLineEntity.error
                        };
                    }
                }

                // Create credit line for this NARDA
                journalEntry.selectNewLine({
                    sublistId: 'line'
                });

                journalEntry.setCurrentSublistValue({
                    sublistId: 'line',
                    fieldId: 'account',
                    value: CONFIG.ACCOUNTS.ACCOUNTS_RECEIVABLE
                });

                journalEntry.setCurrentSublistValue({
                    sublistId: 'line',
                    fieldId: 'credit',
                    value: nardaGroup.totalAmount
                });

                var lineMemo = 'MARCONE CM' + extractedData.invoiceNumber + ' ' + nardaNumber;
                journalEntry.setCurrentSublistValue({
                    sublistId: 'line',
                    fieldId: 'memo',
                    value: lineMemo
                });

                journalEntry.setCurrentSublistValue({
                    sublistId: 'line',
                    fieldId: 'entity',
                    value: creditLineEntity.entityId
                });

                journalEntry.commitLine({
                    sublistId: 'line'
                });

                log.debug('Added credit line for NARDA', {
                    nardaNumber: nardaNumber,
                    amount: nardaGroup.totalAmount,
                    entity: creditLineEntity.entityId,
                    memo: lineMemo
                });
            }

            // Save the journal entry
            var jeId = journalEntry.save();

            log.debug('Multi-line Journal Entry Created Successfully', {
                journalEntryId: jeId,
                tranid: tranid,
                date: jeDate,
                grandTotal: grandTotal,
                nardaGroups: nardaNumbers,
                fileName: splitPart.fileName,
                recordId: recordId
            });

            // Attach the JSON file to the journal entry
            var attachResult = attachFileToRecord(jeId, splitPart.fileId, recordId);

            // Attach the PDF file to the journal entry if available
            if (splitPart.pdfFileId) {
                var pdfAttachResult = attachFileToRecord(jeId, splitPart.pdfFileId, recordId);
                log.debug('PDF file attached to consolidated journal entry', {
                    jeId: jeId,
                    pdfFileId: splitPart.pdfFileId,
                    attachSuccess: pdfAttachResult.success
                });
            }

            return {
                success: true,
                journalEntryId: jeId,
                tranid: tranid,
                attachmentResult: attachResult,
                nardaGroups: nardaNumbers,
                grandTotal: grandTotal,
                pdfFileId: splitPart.pdfFileId,
                extractedData: extractedData
            };

        } catch (error) {
            log.error('Error creating multi-line journal entry', {
                error: error.toString(),
                fileName: splitPart.fileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function createJournalEntryFromNardaGroup(splitPart, recordId, extractedData, nardaGroup, nardaNumber, CONFIG) {
        try {
            log.debug('Creating journal entry from NARDA group', {
                nardaNumber: nardaNumber,
                totalAmount: nardaGroup.totalAmount,
                lineItemCount: nardaGroup.lineItems ? nardaGroup.lineItems.length : 0,
                fileName: splitPart.fileName,
                recordId: recordId
            });

            // Validate required data
            if (!extractedData.invoiceNumber || !extractedData.invoiceDate) {
                return {
                    success: false,
                    error: 'Missing required invoice number or date for journal entry creation'
                };
            }

            var tranid = extractedData.invoiceNumber + ' CM';

            // Check for duplicate journal entry
            var duplicateCheck = checkForDuplicateJournalEntry(tranid, recordId);
            if (!duplicateCheck.success) {
                return {
                    success: false,
                    error: 'Duplicate journal entry exists with tranid: ' + tranid,
                    isDuplicate: true,
                    existingJournalEntry: duplicateCheck.existingEntry
                };
            }

            // Parse the date
            var jeDate = new Date(extractedData.invoiceDate);
            if (isNaN(jeDate.getTime())) {
                return {
                    success: false,
                    error: 'Invalid invoice date: ' + extractedData.invoiceDate
                };
            }

            // Create memo
            var memo = 'MARCONE CM' + extractedData.invoiceNumber + ' ' + nardaNumber;
            
            // Collect sales order numbers from line items
            var salesOrderNumbers = [];
            if (nardaGroup.lineItems) {
                for (var i = 0; i < nardaGroup.lineItems.length; i++) {
                    var lineItem = nardaGroup.lineItems[i];
                    if (lineItem.salesOrderNumber && salesOrderNumbers.indexOf(lineItem.salesOrderNumber) === -1) {
                        salesOrderNumbers.push(lineItem.salesOrderNumber);
                    }
                }
            }
            
            // Append sales order numbers to memo if present
            if (salesOrderNumbers.length > 0) {
                memo += ' | SOs: ' + salesOrderNumbers.join(', ');
            }

            // Find Credit Line Entity based on NARDA number
            var creditLineEntity = findCreditLineEntity(nardaNumber, recordId);
            if (!creditLineEntity.success) {
                if (creditLineEntity.reason === 'NO_MATCHING_OPEN_INVOICE') {
                    log.debug('NARDA has no matching open invoices - skipping journal entry creation', {
                        nardaNumber: nardaNumber,
                        error: creditLineEntity.error,
                        recordId: recordId
                    });

                    return {
                        success: true,
                        isSkipped: true,
                        skipReason: 'No open invoices found for NARDA ' + nardaNumber + ' - cannot determine customer for credit line',
                        skipType: 'NO_MATCHING_OPEN_INVOICE',
                        nardaNumber: nardaNumber,
                        totalAmount: nardaGroup.totalAmount,
                        extractedData: extractedData,
                        pdfFileId: splitPart.pdfFileId,
                        fileName: splitPart.fileName
                    };
                } else {
                    log.error('Could not find Credit Line Entity due to error', {
                        nardaNumber: nardaNumber,
                        error: creditLineEntity.error,
                        recordId: recordId
                    });
                    return {
                        success: false,
                        error: 'Could not find Credit Line Entity: ' + creditLineEntity.error
                    };
                }
            }

            // Create Journal Entry
            var journalEntry = record.create({
                type: record.Type.JOURNAL_ENTRY,
                isDynamic: true
            });

            // Set header fields
            journalEntry.setValue({
                fieldId: 'tranid',
                value: tranid
            });

            journalEntry.setValue({
                fieldId: 'trandate',
                value: jeDate
            });

            journalEntry.setValue({
                fieldId: 'memo',
                value: memo
            });

            // First line - Debit to Account 111 (Accounts Payable)
            journalEntry.selectNewLine({
                sublistId: 'line'
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'account',
                value: CONFIG.ACCOUNTS.ACCOUNTS_PAYABLE
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'debit',
                value: nardaGroup.totalAmount
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'memo',
                value: memo
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'entity',
                value: CONFIG.ENTITIES.MARCONE
            });

            journalEntry.commitLine({
                sublistId: 'line'
            });

            // Second line - Credit to Account 119 (Accounts Receivable)
            journalEntry.selectNewLine({
                sublistId: 'line'
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'account',
                value: CONFIG.ACCOUNTS.ACCOUNTS_RECEIVABLE
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'credit',
                value: nardaGroup.totalAmount
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'memo',
                value: memo
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'entity',
                value: creditLineEntity.entityId
            });

            journalEntry.commitLine({
                sublistId: 'line'
            });

            // Save the journal entry
            var jeId = journalEntry.save();

            log.debug('Journal Entry Created Successfully', {
                journalEntryId: jeId,
                tranid: tranid,
                nardaNumber: nardaNumber,
                totalAmount: nardaGroup.totalAmount,
                fileName: splitPart.fileName,
                recordId: recordId
            });

            // Attach the PDF file to the journal entry
            var attachResult = attachFileToRecord(jeId, splitPart.fileId, recordId);

            // Attach the PDF file to the journal entry if available
            if (splitPart.pdfFileId) {
                var pdfAttachResult = attachFileToRecord(jeId, splitPart.pdfFileId, recordId);
                log.debug('PDF file attached to single NARDA journal entry', {
                    jeId: jeId,
                    nardaNumber: nardaNumber,
                    pdfFileId: splitPart.pdfFileId,
                    attachSuccess: pdfAttachResult.success
                });
            }

            return {
                success: true,
                journalEntryId: jeId,
                tranid: tranid,
                nardaGroups: [nardaNumber],
                totalAmount: nardaGroup.totalAmount,
                attachmentResult: attachResult,
                pdfFileId: splitPart.pdfFileId,
                extractedData: extractedData
            };

        } catch (error) {
            log.error('Error creating journal entry from NARDA group', {
                error: error.toString(),
                nardaNumber: nardaNumber,
                fileName: splitPart.fileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function createVendorCreditFromVRMA(splitPart, recordId, extractedData, nardaGroup, VRMAResults, originalBillNumber) {
        try {
            log.debug('Creating Vendor Credit from VRMA for CONCDA/NF/CORE - Processing by Original Bill Number', {
                nardaNumber: nardaGroup.nardaNumber,
                originalBillNumber: originalBillNumber,
                VRMAResults: VRMAResults.length,
                totalLineItems: nardaGroup.lineItems ? nardaGroup.lineItems.length : 0,
                fileName: splitPart.fileName,
                recordId: recordId
            });

            // Group line items by original bill number
            var billNumberGroups = groupLineItemsByOriginalBillNumber(nardaGroup.lineItems);

            log.debug('Line items grouped by original bill number', {
                totalGroups: Object.keys(billNumberGroups).length,
                groups: Object.keys(billNumberGroups),
                targetBillNumber: originalBillNumber
            });

            // Process only the group for this specific original bill number
            var billGroup = billNumberGroups[originalBillNumber];
            if (!billGroup || billGroup.length === 0) {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: nardaGroup.nardaNumber + ' NARDA - no line items found for original bill number: ' + originalBillNumber,
                    skipType: 'NO_MATCHING_LINE_ITEMS',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    pdfFileId: splitPart.pdfFileId,
                    fileName: splitPart.fileName
                };
            }

            // Filter VRMA results to only include lines that contain this original bill number
            var matchingVRMALines = VRMAResults.filter(function (VRMALine) {
                return VRMALine.memo && VRMALine.memo.indexOf(originalBillNumber) !== -1;
            });

            log.debug('Filtered VRMA lines for original bill number', {
                originalBillNumber: originalBillNumber,
                totalVRMALines: VRMAResults.length,
                matchingVRMALines: matchingVRMALines.length
            });

            if (matchingVRMALines.length === 0) {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: nardaGroup.nardaNumber + ' NARDA - no VRMA lines found containing bill number: ' + originalBillNumber,
                    skipType: 'NO_VRMA_MATCH',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    pdfFileId: splitPart.pdfFileId,
                    fileName: splitPart.fileName
                };
            }

            // Group VRMA lines by their parent VRMA internal ID
            var VRMAGroups = {};
            for (var i = 0; i < matchingVRMALines.length; i++) {
                var VRMALine = matchingVRMALines[i];
                var VRMAId = VRMALine.internalId;

                if (!VRMAGroups[VRMAId]) {
                    VRMAGroups[VRMAId] = [];
                }
                VRMAGroups[VRMAId].push(VRMALine);
            }

            var VRMAIds = Object.keys(VRMAGroups);
            log.debug('VRMA lines grouped by parent VRMA', {
                originalBillNumber: originalBillNumber,
                totalVRMAs: VRMAIds.length,
                VRMAIds: VRMAIds
            });

            // Try each VRMA until we find one that works
            var lastError = null;
            for (var VRMAIndex = 0; VRMAIndex < VRMAIds.length; VRMAIndex++) {
                var VRMAId = VRMAIds[VRMAIndex];
                var VRMALinesForThisVRMA = VRMAGroups[VRMAId];

                log.debug('Attempting VRMA transformation', {
                    attemptNumber: VRMAIndex + 1,
                    totalAttempts: VRMAIds.length,
                    VRMAId: VRMAId,
                    linesInThisVRMA: VRMALinesForThisVRMA.length,
                    originalBillNumber: originalBillNumber
                });

                // Attempt to match PDF line items to VRMA lines by amount for this specific VRMA
                var matchedPairs = matchPDFLinesToVRMALines(billGroup, VRMALinesForThisVRMA, originalBillNumber);

                if (matchedPairs.length === 0) {
                    log.debug('No amount matches found for this VRMA, trying next VRMA', {
                        VRMAId: VRMAId,
                        attemptNumber: VRMAIndex + 1,
                        originalBillNumber: originalBillNumber
                    });
                    continue; // Try next VRMA
                }

                // Attempt to create vendor credit for this VRMA
                var vcResult = createGroupedVendorCredit(
                    splitPart,
                    recordId,
                    extractedData,
                    nardaGroup,
                    matchedPairs,
                    originalBillNumber
                );

                // Check if this attempt was successful
                if (vcResult.success && !vcResult.isSkipped) {
                    log.debug('Successfully created vendor credit', {
                        VRMAId: VRMAId,
                        attemptNumber: VRMAIndex + 1,
                        vendorCreditId: vcResult.vendorCreditId,
                        originalBillNumber: originalBillNumber
                    });
                    return vcResult;
                } else if (vcResult.isSkipped) {
                    log.debug('VRMA transformation skipped, trying next VRMA', {
                        VRMAId: VRMAId,
                        attemptNumber: VRMAIndex + 1,
                        skipReason: vcResult.skipReason,
                        originalBillNumber: originalBillNumber
                    });
                    lastError = vcResult;
                    continue; // Try next VRMA
                } else {
                    log.error('VRMA transformation failed, trying next VRMA', {
                        VRMAId: VRMAId,
                        attemptNumber: VRMAIndex + 1,
                        error: vcResult.error,
                        originalBillNumber: originalBillNumber
                    });
                    lastError = vcResult;
                    continue; // Try next VRMA
                }
            }

            // If we get here, all VRMAs failed or were skipped
            log.error('All VRMA transformation attempts failed or were skipped', {
                originalBillNumber: originalBillNumber,
                totalVRMAsAttempted: VRMAIds.length,
                nardaNumber: nardaGroup.nardaNumber,
                lastError: lastError
            });

            // Return the last error/skip result
            if (lastError) {
                return lastError;
            } else {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: nardaGroup.nardaNumber + ' NARDA - all ' + VRMAIds.length + ' VRMAs with bill number ' + originalBillNumber + ' failed transformation attempts',
                    skipType: 'ALL_VRMA_ATTEMPTS_FAILED',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    VRMAAttemptsCount: VRMAIds.length,
                    pdfFileId: splitPart.pdfFileId,
                    fileName: splitPart.fileName
                };
            }

        } catch (error) {
            log.error('Error creating vendor credit from VRMA', {
                error: error.toString(),
                nardaNumber: nardaGroup.nardaNumber,
                originalBillNumber: originalBillNumber,
                fileName: splitPart.fileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function groupLineItemsByOriginalBillNumber(lineItems) {
        var groups = {};

        for (var i = 0; i < lineItems.length; i++) {
            var lineItem = lineItems[i];
            var billNumber = lineItem.originalBillNumber;

            if (billNumber) {
                if (!groups[billNumber]) {
                    groups[billNumber] = [];
                }
                groups[billNumber].push(lineItem);
            }
        }

        return groups;
    }

    function consolidateVendorCreditGroups(groupedLineItems) {
        try {
            log.debug('Consolidating vendor credit groups by original bill number', {
                inputGroups: Object.keys(groupedLineItems)
            });

            var billNumberGroups = {};

            // Process each NARDA group
            var nardaNumbers = Object.keys(groupedLineItems);
            for (var i = 0; i < nardaNumbers.length; i++) {
                var nardaNumber = nardaNumbers[i];
                var nardaGroup = groupedLineItems[nardaNumber];

                // Only consolidate vendor credit NARDA types (CONCDA, NF, CORE, CONCESSION)
                var isVendorCreditType = (
                    nardaNumber.toUpperCase() === 'CONCDA' ||
                    nardaNumber.toUpperCase() === 'CONCDAM' ||
                    nardaNumber.toUpperCase() === 'NF' ||
                    nardaNumber.toUpperCase() === 'CORE' ||
                    nardaNumber.toUpperCase() === 'CONCESSION'
                );

                if (!isVendorCreditType) {
                    continue; // Skip journal entry types
                }

                // Get all unique original bill numbers from this NARDA group
                var billNumbers = nardaGroup.originalBillNumbers || [];

                for (var j = 0; j < billNumbers.length; j++) {
                    var billNumber = billNumbers[j];

                    // Initialize bill number group if it doesn't exist
                    if (!billNumberGroups[billNumber]) {
                        billNumberGroups[billNumber] = {
                            originalBillNumber: billNumber,
                            nardaTypes: [],
                            lineItems: [],
                            totalAmount: 0,
                            allNardaNumbers: []
                        };
                    }

                    // Add this NARDA type if not already included
                    if (billNumberGroups[billNumber].nardaTypes.indexOf(nardaNumber) === -1) {
                        billNumberGroups[billNumber].nardaTypes.push(nardaNumber);
                    }

                    // Add all line items from this NARDA group that match this bill number
                    for (var k = 0; k < nardaGroup.lineItems.length; k++) {
                        var lineItem = nardaGroup.lineItems[k];
                        if (lineItem.originalBillNumber === billNumber) {
                            billNumberGroups[billNumber].lineItems.push(lineItem);

                            // Parse and add amount
                            var amount = Math.abs(parseFloat(lineItem.totalAmount.replace(/[()$,-]/g, '')));
                            if (!isNaN(amount)) {
                                billNumberGroups[billNumber].totalAmount += amount;
                            }
                        }
                    }

                    // Track all unique NARDA numbers for this bill
                    if (billNumberGroups[billNumber].allNardaNumbers.indexOf(nardaNumber) === -1) {
                        billNumberGroups[billNumber].allNardaNumbers.push(nardaNumber);
                    }
                }
            }

            log.debug('Vendor credit groups consolidated by bill number', {
                totalBillNumbers: Object.keys(billNumberGroups).length,
                billNumbers: Object.keys(billNumberGroups)
            });

            return billNumberGroups;

        } catch (error) {
            log.error('Error consolidating vendor credit groups', {
                error: error.toString(),
                inputGroups: Object.keys(groupedLineItems)
            });
            return {};
        }
    }

    function matchPDFLinesToVRMALines(pdfLines, VRMALines, originalBillNumber) {
        var matchedPairs = [];
        var usedVRMALines = [];

        log.debug('Attempting to match PDF lines to VRMA lines', {
            originalBillNumber: originalBillNumber,
            pdfLineCount: pdfLines.length,
            VRMALineCount: VRMALines.length
        });

        // Try to match each PDF line to a VRMA line by amount AND part number
        for (var i = 0; i < pdfLines.length; i++) {
            var pdfLine = pdfLines[i];
            var pdfAmount = Math.abs(parseFloat(pdfLine.totalAmount.replace(/[()$,-]/g, '')));
            var pdfPartNumber = pdfLine.partNumber || '';

            // Find matching VRMA line that hasn't been used
            for (var j = 0; j < VRMALines.length; j++) {
                var VRMALine = VRMALines[j];

                // Skip if this VRMA line is already used
                if (usedVRMALines.indexOf(VRMALine.lineNumber) !== -1) {
                    continue;
                }

                var VRMAAmount = Math.abs(parseFloat(VRMALine.amount));

                // Check if amounts match within tolerance
                var amountMatches = Math.abs(VRMAAmount - pdfAmount) < 0.01;
                
                // Check if part numbers match (if PDF has part number)
                var partNumberMatches = true;
                if (pdfPartNumber && VRMALine.itemName) {
                    // Item name is already available from search results - no lookup needed!
                    partNumberMatches = VRMALine.itemName === pdfPartNumber;
                    
                    log.debug('Part number comparison', {
                        pdfPartNumber: pdfPartNumber,
                        VRMAItemName: VRMALine.itemName,
                        matches: partNumberMatches,
                        note: 'Using itemName from search results - no additional lookup needed'
                    });
                }

                // Match if both amount AND part number match
                if (amountMatches && partNumberMatches) {
                    matchedPairs.push({
                        pdfLine: pdfLine,
                        VRMALine: VRMALine,
                        amount: pdfAmount
                    });

                    usedVRMALines.push(VRMALine.lineNumber);

                    log.debug('Matched PDF line to VRMA line', {
                        pdfAmount: pdfAmount,
                        VRMAAmount: VRMAAmount,
                        pdfPartNumber: pdfPartNumber,
                        VRMALineNumber: VRMALine.lineNumber,
                        originalBillNumber: originalBillNumber
                    });

                    break; // Move to next PDF line
                }
            }
        }

        log.debug('Line matching complete', {
            originalBillNumber: originalBillNumber,
            totalMatches: matchedPairs.length,
            unmatchedPDFLines: pdfLines.length - matchedPairs.length
        });

        return matchedPairs;
    }

    function createGroupedVendorCredit(splitPart, recordId, extractedData, nardaGroup, matchedPairs, originalBillNumber) {
        try {
            log.debug('Creating grouped vendor credit for original bill number', {
                originalBillNumber: originalBillNumber,
                nardaNumber: nardaGroup.nardaNumber,
                matchedPairsCount: matchedPairs.length,
                VRMAInternalId: matchedPairs[0].VRMALine.internalId,
                fileName: splitPart.fileName,
                recordId: recordId
            });

            // Parse the invoice date
            var vcDate = new Date(extractedData.invoiceDate);
            if (isNaN(vcDate.getTime())) {
                return {
                    success: false,
                    error: 'Invalid invoice date: ' + extractedData.invoiceDate
                };
            }

            // Create vendor credit tranid (no line suffixes since we're grouping)
            var vendorCreditTranid = extractedData.invoiceNumber;

            // Check for duplicate vendor credit
            var duplicateVCCheck = checkForDuplicateVendorCredit(vendorCreditTranid, recordId);
            if (!duplicateVCCheck.success) {
                return {
                    success: false,
                    error: 'Duplicate vendor credit exists with tranid: ' + vendorCreditTranid,
                    isDuplicate: true,
                    existingVendorCredit: duplicateVCCheck.existingEntry
                };
            }

            // Use the VRMA from the first matched pair (all should be from same VRMA)
            var VRMAInternalId = matchedPairs[0].VRMALine.internalId;

            // Load VRMA record for validation
            var VRMARecord;
            try {
                VRMARecord = record.load({
                    type: record.Type.VENDOR_RETURN_AUTHORIZATION,
                    id: VRMAInternalId,
                    isDynamic: false
                });

                // Check VRMA status
                var VRMAStatus = VRMARecord.getValue('status');
                var VRMATransactionId = VRMARecord.getValue('tranid');
                var statusText = VRMARecord.getText('status') || '';

                log.debug('VRMA basic validation check', {
                    VRMAInternalId: VRMAInternalId,
                    VRMATransactionId: VRMATransactionId,
                    VRMAStatus: VRMAStatus,
                    statusText: statusText,
                    originalBillNumber: originalBillNumber
                });

                // Check if VRMA is in a valid status for transformation
                var invalidStatuses = ['Closed', 'Rejected', 'Cancelled'];
                var isInvalidStatus = false;

                for (var i = 0; i < invalidStatuses.length; i++) {
                    if (statusText.indexOf(invalidStatuses[i]) !== -1) {
                        isInvalidStatus = true;
                        break;
                    }
                }

                if (isInvalidStatus) {
                    log.debug('VRMA cannot be transformed - invalid status', {
                        VRMAInternalId: VRMAInternalId,
                        VRMATransactionId: VRMATransactionId,
                        statusText: statusText,
                        originalBillNumber: originalBillNumber
                    });

                    return {
                        success: true,
                        isSkipped: true,
                        skipReason: nardaGroup.nardaNumber + ' NARDA - VRMA ' + VRMATransactionId + ' cannot be credited (Status: ' + statusText + ')',
                        skipType: 'VRMA_INVALID_STATUS',
                        nardaNumber: nardaGroup.nardaNumber,
                        extractedData: extractedData,
                        matchingVRMA: {
                            internalId: VRMAInternalId,
                            tranid: VRMATransactionId,
                            status: statusText
                        },
                        pdfFileId: splitPart.pdfFileId,
                        fileName: splitPart.fileName
                    };
                }

            } catch (VRMALoadError) {
                log.error('Cannot load VRMA record for validation', {
                    error: VRMALoadError.toString(),
                    VRMAInternalId: VRMAInternalId,
                    originalBillNumber: originalBillNumber
                });

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: nardaGroup.nardaNumber + ' NARDA - Cannot access VRMA ' + VRMAInternalId,
                    skipType: 'VRMA_ACCESS_ERROR',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    VRMAAccessError: VRMALoadError.toString(),
                    pdfFileId: splitPart.pdfFileId,
                    fileName: splitPart.fileName
                };
            }

            // Transform VRMA to Vendor Credit
            var vendorCredit;
            try {
                vendorCredit = record.transform({
                    fromType: record.Type.VENDOR_RETURN_AUTHORIZATION,
                    fromId: VRMAInternalId,
                    toType: record.Type.VENDOR_CREDIT,
                    isDynamic: true
                });

                log.debug('VRMA transformation successful', {
                    VRMAInternalId: VRMAInternalId,
                    VRMATransactionId: VRMATransactionId,
                    originalBillNumber: originalBillNumber
                });

            } catch (transformError) {
                log.error('VRMA transformation failed', {
                    error: transformError.toString(),
                    VRMAInternalId: VRMAInternalId,
                    VRMATransactionId: VRMATransactionId,
                    originalBillNumber: originalBillNumber
                });

                var skipReason;
                var skipType;

                if (transformError.name === 'INVALID_INITIALIZE_REF' ||
                    transformError.message.indexOf('invalid reference') !== -1) {
                    skipReason = nardaGroup.nardaNumber + ' NARDA - VRMA ' + VRMATransactionId + ' cannot be transformed (fully credited or invalid state)';
                    skipType = 'VRMA_FULLY_CREDITED';
                } else if (transformError.name === 'INSUFFICIENT_PERMISSION') {
                    skipReason = nardaGroup.nardaNumber + ' NARDA - Insufficient permissions to transform VRMA ' + VRMATransactionId;
                    skipType = 'VRMA_PERMISSION_ERROR';
                } else {
                    skipReason = nardaGroup.nardaNumber + ' NARDA - VRMA ' + VRMATransactionId + ' transformation failed: ' + transformError.message;
                    skipType = 'VRMA_TRANSFORM_ERROR';
                }

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: skipReason,
                    skipType: skipType,
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    matchingVRMA: {
                        internalId: VRMAInternalId,
                        tranid: VRMATransactionId,
                        status: VRMAStatus
                    },
                    transformError: transformError.toString()
                };
            }

            // Set header fields
            vendorCredit.setValue({
                fieldId: 'tranid',
                value: vendorCreditTranid
            });

            vendorCredit.setValue({
                fieldId: 'trandate',
                value: vcDate
            });

            // Set memo
            var nardaTypesList = nardaGroup.allNardaTypes ? nardaGroup.allNardaTypes.join('+') : nardaGroup.nardaNumber.toUpperCase();
            var vcMemo = nardaTypesList + ' Credit - ' + extractedData.invoiceNumber + ' - Bill: ' + originalBillNumber + ' - VRMA: ' + VRMARecord.getValue('tranid');
            
            // Collect sales order numbers from matched pairs
            var salesOrderNumbers = [];
            for (var i = 0; i < matchedPairs.length; i++) {
                var pdfLine = matchedPairs[i].pdfLine;
                if (pdfLine.salesOrderNumber && salesOrderNumbers.indexOf(pdfLine.salesOrderNumber) === -1) {
                    salesOrderNumbers.push(pdfLine.salesOrderNumber);
                }
            }
            
            // Append sales order numbers to memo if present
            if (salesOrderNumbers.length > 0) {
                vcMemo += ' | SOs: ' + salesOrderNumbers.join(', ');
            }

            vendorCredit.setValue({
                fieldId: 'memo',
                value: vcMemo
            });

            // Remove all lines except the matched ones
            var vcLineCount = vendorCredit.getLineCount({ sublistId: 'item' });
            var targetLineNumbers = matchedPairs.map(function (pair) { return pair.VRMALine.lineNumber; });

            log.debug('Filtering Vendor Credit lines to matched VRMA lines only', {
                totalVCLines: vcLineCount,
                targetLineNumbers: targetLineNumbers,
                VRMAInternalId: VRMAInternalId
            });

            // Remove lines in reverse order to avoid index shifting issues
            for (var j = vcLineCount - 1; j >= 0; j--) {
                var currentLineKey = vendorCredit.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'line',
                    line: j
                });

                // If this line is not in our target list, remove it
                if (targetLineNumbers.indexOf(currentLineKey) === -1) {
                    vendorCredit.removeLine({
                        sublistId: 'item',
                        line: j
                    });
                }
            }

            // Add delivery amount as expense line if it exists and is greater than $0.00
            if (extractedData.deliveryAmount && extractedData.deliveryAmount !== '$0.00') {
                try {
                    var deliveryAmountValue = parseFloat(extractedData.deliveryAmount.replace(/[$(),]/g, ''));

                    if (!isNaN(deliveryAmountValue) && deliveryAmountValue > 0) {
                        log.debug('Adding delivery amount as expense line', {
                            deliveryAmount: extractedData.deliveryAmount,
                            parsedAmount: deliveryAmountValue,
                            account: CONFIG.ACCOUNTS.FREIGHT_IN,
                            department: CONFIG.ENTITIES.SERVICE_DEPARTMENT
                        });

                        vendorCredit.selectNewLine({
                            sublistId: 'expense'
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'account',
                            value: CONFIG.ACCOUNTS.FREIGHT_IN
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'amount',
                            value: deliveryAmountValue
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'department',
                            value: CONFIG.ENTITIES.SERVICE_DEPARTMENT
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'memo',
                            value: 'Delivery - ' + extractedData.invoiceNumber
                        });

                        vendorCredit.commitLine({
                            sublistId: 'expense'
                        });

                        log.debug('Delivery expense line added successfully');
                    }
                } catch (deliveryError) {
                    log.error('Error adding delivery expense line (continuing with vendor credit creation)', {
                        error: deliveryError.toString(),
                        deliveryAmount: extractedData.deliveryAmount
                    });
                }
            }

            // Save the vendor credit
            var vendorCreditId = vendorCredit.save();

            log.debug('Grouped Vendor Credit created successfully', {
                vendorCreditId: vendorCreditId,
                vendorCreditTranid: vendorCreditTranid,
                VRMAInternalId: VRMAInternalId,
                originalBillNumber: originalBillNumber,
                matchedLineCount: matchedPairs.length,
                memo: vcMemo,
                recordId: recordId
            });

            // Attach the JSON file to the vendor credit
            var attachResult = attachFileToRecord(vendorCreditId, splitPart.fileId, recordId, record.Type.VENDOR_CREDIT);

            // Attach the PDF file to the vendor credit if available
            if (splitPart.pdfFileId) {
                var pdfAttachResult = attachFileToRecord(vendorCreditId, splitPart.pdfFileId, recordId, record.Type.VENDOR_CREDIT);
                log.debug('PDF file attached to vendor credit', {
                    vendorCreditId: vendorCreditId,
                    originalBillNumber: originalBillNumber,
                    pdfFileId: splitPart.pdfFileId,
                    attachSuccess: pdfAttachResult.success
                });
            }

            // Calculate total amount from all matched pairs
            var totalAmount = 0;
            for (var i = 0; i < matchedPairs.length; i++) {
                totalAmount += matchedPairs[i].amount;
            }

            return {
                success: true,
                isVendorCredit: true,
                vendorCreditId: vendorCreditId,
                vendorCreditTranid: vendorCreditTranid,
                nardaNumber: nardaGroup.nardaNumber,
                totalAmount: totalAmount,
                matchedLineCount: matchedPairs.length,
                originalBillNumber: originalBillNumber,
                matchingVRMA: {
                    internalId: VRMAInternalId,
                    tranid: VRMARecord.getValue('tranid'),
                    entity: VRMARecord.getValue('entity'),
                    matchedLineNumbers: targetLineNumbers
                },
                extractedData: extractedData,
                attachmentResult: attachResult,
                deliveryAmountProcessed: extractedData.deliveryAmount && extractedData.deliveryAmount !== '$0.00'
            };

        } catch (error) {
            log.error('Error creating grouped Vendor Credit', {
                error: error.toString(),
                originalBillNumber: originalBillNumber,
                nardaNumber: nardaGroup.nardaNumber,
                fileName: splitPart.fileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function searchForMatchingVRMA(originalBillNumber, recordId) {
        try {
            log.debug('Searching for matching VRMA records by line memo', {
                originalBillNumber: originalBillNumber,
                recordId: recordId
            });

            var VRMAResults = [];

            var VRMASearch = search.create({
                type: search.Type.VENDOR_RETURN_AUTHORIZATION,
                filters: [
                    ['type', 'anyof', 'VendAuth'],
                    'AND',
                    ['memo', 'contains', originalBillNumber]
                ],
                columns: [
                    'internalid',
                    'tranid',
                    'trandate',
                    'memo',
                    'entity',
                    'status',
                    'item',
                    'amount',
                    'line'
                ]
            });

            var searchResults = VRMASearch.run();

            searchResults.each(function (result) {
                var lineMemo = result.getValue('memo');

                // Check if this line's memo contains our original bill number
                if (lineMemo && lineMemo.indexOf(originalBillNumber) !== -1) {
                    VRMAResults.push({
                        internalId: result.getValue('internalid'),
                        tranid: result.getValue('tranid'),
                        trandate: result.getValue('trandate'),
                        memo: lineMemo,
                        entity: result.getValue('entity'),
                        status: result.getValue('status'),
                        itemId: result.getValue('item'),
                        itemName: result.getText('item'),  // Get item name for part number validation
                        amount: result.getValue('amount'),
                        lineNumber: result.getValue('line')
                    });

                    log.debug('VRMA match found in line memo', {
                        VRMAInternalId: result.getValue('internalid'),
                        VRMATransactionId: result.getValue('tranid'),
                        lineNumber: result.getValue('line'),
                        lineMemo: lineMemo,
                        itemName: result.getText('item'),
                        amount: result.getValue('amount'),
                        originalBillNumber: originalBillNumber
                    });
                }

                return true; // Continue processing results
            });

            log.debug('VRMA search completed', {
                originalBillNumber: originalBillNumber,
                totalMatches: VRMAResults.length,
                recordId: recordId
            });

            return VRMAResults;

        } catch (error) {
            log.error('Error searching for matching VRMA', {
                error: error.toString(),
                originalBillNumber: originalBillNumber,
                recordId: recordId
            });
            return [];
        }
    }

    function attachFileToRecord(recordId, fileId, originalRecordId, recordType) {
        try {
            // Default to Journal Entry if no record type specified
            var targetRecordType = recordType || record.Type.JOURNAL_ENTRY;
            var recordTypeName = targetRecordType === record.Type.VENDOR_CREDIT ? 'Vendor Credit' : 'Journal Entry';

            log.debug('Attaching file to ' + recordTypeName + ' using record.attach', {
                recordId: recordId,
                fileId: fileId,
                originalRecordId: originalRecordId,
                recordType: targetRecordType
            });

            // Use record.attach to attach the file to the record
            record.attach({
                record: {
                    type: 'file',
                    id: fileId
                },
                to: {
                    type: targetRecordType,
                    id: recordId
                }
            });

            log.debug('File attached successfully to ' + recordTypeName, {
                recordId: recordId,
                fileId: fileId,
                originalRecordId: originalRecordId
            });

            return {
                success: true,
                fileId: fileId,
                method: 'record.attach',
                recordType: recordTypeName
            };

        } catch (error) {
            log.error('Error attaching file to ' + (recordTypeName || 'record'), {
                error: error.toString(),
                recordId: recordId,
                fileId: fileId,
                originalRecordId: originalRecordId
            });

            // Even if attachment fails, we don't want to fail the entire creation process
            return {
                success: true,
                fileId: fileId,
                attachmentFailed: true,
                error: error.toString(),
                method: 'attachment_failed_but_record_created',
                recordType: recordTypeName || 'Unknown'
            };
        }
    }

    return {
        execute: execute
    };
});
