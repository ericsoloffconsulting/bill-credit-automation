/**
 * @NApiVersion 2.1
 * @NScriptType ScheduledScript
 */
define(['N/search', 'N/log', 'N/file', 'N/record', 'N/https', 'N/email', 'N/runtime', 'N/task'], function (search, log, file, record, https, email, runtime, task) {

    // Create configuration object
    var CONFIG = {
        FOLDERS: {
            SOURCE: 2462381,
            PROCESSED: 2462382,
            FAILED: 2466592,
            JSON: 2466590
        },
        ACCOUNTS: {
            ACCOUNTS_PAYABLE: 111,
            ACCOUNTS_RECEIVABLE: 119,
            FREIGHT_IN: 367
        },
        ENTITIES: {
            MARCONE: 2106,
            SERVICE_DEPARTMENT: 13
        },
        LIMITS: {
            MAX_PDF_SIZE: 10000000, // 10MB
            MAX_PROCESSING_TIME: 3600 // 1 hour
        }
    };

    function execute(context) {
        try {
            log.debug('Script Start', 'Starting PDF File copy and public access processing');

            // Get script parameters
            var script = runtime.getCurrentScript();
            var API_KEY = script.getParameter({
                name: 'custscript_bas_marcone_pdfco_api_key'
            });
            var SPLIT_TEXT = script.getParameter({
                name: 'custscript_bas_marcone_pdfco_split_text'
            });

            // Validate required parameters
            if (!API_KEY) {
                log.error('Missing required parameter', 'custscript_bas_marcone_pdfco_api_key');
                throw new Error('Missing required API Key parameter');
            }

            if (!SPLIT_TEXT) {
                log.error('Missing required parameter', 'custscript_bas_marcone_pdfco_split_text');
                throw new Error('Missing required Split Text parameter');
            }

            log.debug('Script parameters loaded', {
                hasApiKey: !!API_KEY,
                splitText: SPLIT_TEXT
            });

            // Load the saved search
            var savedSearchId = 'customsearch_bas_zonecapture_marcone_bc';

            log.debug('Loading saved search', savedSearchId);

            var savedSearchResults = search.load({
                id: savedSearchId
            });

            // UPDATED: Enhanced tracking variables
            var searchResultCount = 0;
            var singlePdfCount = 0;
            var splitPdfCount = 0;
            var totalPdfPartsProcessed = 0;
            var pdfFilesFound = 0;
            var pdfFilesCopied = 0;
            var pdfFilesSplit = 0;
            var splitPartsSaved = 0;
            var jsonConversions = 0;
            var journalEntriesCreated = 0;
            var vendorCreditsCreated = 0;
            var targetFolderId = CONFIG.FOLDERS.SOURCE;
            var processedDetails = []; // Track all processing details
            var failedEntries = []; // Track failed journal entries
            var skippedEntries = []; // Track skipped journal entries - EACH PART SEPARATELY
            var recordsInactivated = 0;
            var recordsLeftActive = 0;

            var MAX_RECORDS_PER_RUN = 25;
            var searchResults = savedSearchResults.run().getRange({
                start: 0,
                end: MAX_RECORDS_PER_RUN
            });

            log.debug('Processing limited result set', {
                maxRecordsRequested: MAX_RECORDS_PER_RUN,
                actualRecordsRetrieved: searchResults.length
            });

            for (var resultIndex = 0; resultIndex < searchResults.length; resultIndex++) {
                var result = searchResults[resultIndex];
                searchResultCount++;

                // Get the PDF File value from the column
                var pdfFileColumnValue = result.getValue({
                    name: 'custrecord_eff_nsp2p_trans_pdffile'
                });

                // Also try to get it by column label if the above doesn't work
                var pdfFileText = result.getText({
                    name: 'custrecord_eff_nsp2p_trans_pdffile'
                });

                // Get the record ID for reference (this is the Internal ID from the saved search)
                var recordId = result.id;

                // Get the memo field to check for SPLIT requirement
                var recordMemo = result.getValue({
                    name: 'custrecord_eff_nsp2p_trans_memo'
                }) || '';

                // Determine if PDF needs to be split
                var needsSplitting = recordMemo.toUpperCase().indexOf('SPLIT') !== -1;

                log.debug('Processing Record', {
                    recordId: recordId,
                    searchResultNumber: searchResultCount,
                    pdfFileId: pdfFileColumnValue,
                    pdfFileName: pdfFileText,
                    recordMemo: recordMemo,
                    needsSplitting: needsSplitting
                });

                // Process PDF file if found
                if (pdfFileColumnValue) {
                    pdfFilesFound++;

                    // Count split vs single PDFs
                    if (needsSplitting) {
                        splitPdfCount++;
                    } else {
                        singlePdfCount++;
                    }

                    log.debug('PDF File Found', {
                        recordId: recordId,
                        pdfFileId: pdfFileColumnValue,
                        pdfFileText: pdfFileText,
                        resultNumber: searchResultCount,
                        needsSplitting: needsSplitting
                    });

                    // Copy the PDF file to the target folder and make it public
                    var copyResult = copyPDFToFolderAndMakePublic(pdfFileColumnValue, targetFolderId, recordId);

                    if (copyResult.success) {
                        pdfFilesCopied++;
                        log.debug('PDF File Successfully Copied and Made Public', {
                            originalFileId: pdfFileColumnValue,
                            newFileId: copyResult.newFileId,
                            newFileName: copyResult.newFileName,
                            publicUrl: copyResult.publicUrl,
                            targetFolderId: targetFolderId,
                            recordId: recordId,
                            needsSplitting: needsSplitting
                        });

                        var splitResult;

                        if (needsSplitting) {
                            // Original logic: Split the PDF using PDF.co
                            splitResult = splitPDFWithPDFCo(copyResult.publicUrl, copyResult.newFileName, recordId, API_KEY, SPLIT_TEXT);
                            if (splitResult.success) {
                                pdfFilesSplit++;
                                splitPartsSaved += splitResult.savedParts ? splitResult.savedParts.length : 0;
                            }
                        } else {
                            // Skip splitting, move directly to processed folder and convert to JSON
                            splitResult = processSinglePDF(copyResult.newFileId, copyResult.newFileName, copyResult.publicUrl, recordId, API_KEY);
                            if (splitResult.success) {
                                splitPartsSaved += 1; // Single file moved to processed folder
                            }
                        }

                        // Add needsSplitting flag to splitResult for later use
                        if (splitResult.success) {
                            splitResult.needsSplitting = needsSplitting;
                        }

                        if (splitResult.success) {
                            // Add this check
                            if (!splitResult.savedParts || splitResult.savedParts.length === 0) {
                                log.error('PDF processing reported success but no parts were created', {
                                    fileName: copyResult.newFileName,
                                    needsSplitting: needsSplitting,
                                    splitText: needsSplitting ? SPLIT_TEXT : 'N/A',
                                    recordId: recordId
                                });

                                // Save to failed folder and create failure entry
                                var failedPdfSaveResult = savePDFToFailedFolder(
                                    copyResult.newFileId,
                                    copyResult.newFileName,
                                    recordId,
                                    { error: needsSplitting ? 'No split parts created - split text not found' : 'Single PDF processing failed' }
                                );

                                failedEntries.push({
                                    success: false,
                                    recordId: recordId,
                                    sourceFileName: copyResult.newFileName,
                                    error: needsSplitting ?
                                        'PDF split returned no parts - split text "' + SPLIT_TEXT + '" not found' :
                                        'Single PDF processing failed',
                                    failedPdfSaved: failedPdfSaveResult.success,
                                    failedPdfId: failedPdfSaveResult.success ? failedPdfSaveResult.fileId : null
                                });

                                continue; // Continue to next record
                            }

                            // UPDATED: Count total PDF parts processed
                            totalPdfPartsProcessed += splitResult.savedParts ? splitResult.savedParts.length : 0;

                            // Track if we successfully processed at least one part with journal entry
                            var hasSuccessfulProcessing = false;

                            // Process each split part (or single part for non-split PDFs)
                            if (splitResult.savedParts) {
                                splitResult.savedParts.forEach(function (part) {
                                    if (part.jsonResult && part.jsonResult.success && part.jsonResult.extractedData) {
                                        jsonConversions++;

                                        // Create journal entries from line items
                                        var jeResult = createJournalEntriesFromLineItems(part, recordId);

                                        if (jeResult.success) {
                                            // Process successful journal entries
                                            if (jeResult.journalEntries && jeResult.journalEntries.length > 0) {
                                                jeResult.journalEntries.forEach(function (je) {
                                                    journalEntriesCreated++;
                                                    hasSuccessfulProcessing = true;

                                                    // Determine the journal entry type and consolidation info
                                                    var journalEntryType = 'Unknown';
                                                    var consolidatedLineCount = 0;
                                                    var nardaGroupsArray = je.nardaGroups || [];

                                                    if (nardaGroupsArray.length > 1) {
                                                        journalEntryType = 'Multi-NARDA Groups';
                                                    } else if (nardaGroupsArray.length === 1) {
                                                        if (part.jsonResult.extractedData.groupedLineItems &&
                                                            part.jsonResult.extractedData.groupedLineItems[nardaGroupsArray[0]] &&
                                                            part.jsonResult.extractedData.groupedLineItems[nardaGroupsArray[0]].lineItems) {
                                                            consolidatedLineCount = part.jsonResult.extractedData.groupedLineItems[nardaGroupsArray[0]].lineItems.length;
                                                            journalEntryType = consolidatedLineCount > 1 ? 'Consolidated NARDA' : 'Single NARDA';
                                                        }
                                                    }

                                                    processedDetails.push({
                                                        success: true,
                                                        isVendorCredit: false,
                                                        recordId: recordId,
                                                        sourceFileName: copyResult.newFileName,
                                                        splitPartNumber: part.partNumber,
                                                        splitFileName: part.fileName,
                                                        pdfUrl: 'https://system.netsuite.com' + file.load({ id: part.fileId }).url,
                                                        journalEntryId: je.journalEntryId,
                                                        tranid: je.tranid,
                                                        nardaNumber: nardaGroupsArray.length === 1 ? nardaGroupsArray[0] : 'Multiple: ' + nardaGroupsArray.join(', '),
                                                        nardaGroups: nardaGroupsArray,
                                                        totalAmount: je.grandTotal || je.totalAmount,
                                                        journalEntryType: journalEntryType,
                                                        consolidatedLines: nardaGroupsArray.length === 1 ? consolidatedLineCount : null,
                                                        extractedData: part.jsonResult.extractedData,
                                                        attachmentResult: je.attachmentResult
                                                    });
                                                });
                                            }

                                            // Process successful vendor credits
                                            if (jeResult.vendorCredits && jeResult.vendorCredits.length > 0) {
                                                jeResult.vendorCredits.forEach(function (vc) {
                                                    vendorCreditsCreated++;
                                                    hasSuccessfulProcessing = true;

                                                    var vcNardaNumber = vc.nardaNumber || 'CONCDA/NF';
                                                    var vcTotalAmount = null;
                                                    if (vc.totalAmount !== undefined) {
                                                        vcTotalAmount = Math.abs(vc.totalAmount);
                                                    } else if (vc.matchingVRA && vc.matchingVRA.amount) {
                                                        vcTotalAmount = Math.abs(parseFloat(vc.matchingVRA.amount));
                                                    }

                                                    processedDetails.push({
                                                        success: true,
                                                        isVendorCredit: true,
                                                        recordId: recordId,
                                                        sourceFileName: copyResult.newFileName,
                                                        splitPartNumber: part.partNumber,
                                                        splitFileName: part.fileName,
                                                        pdfUrl: 'https://system.netsuite.com' + file.load({ id: part.fileId }).url,
                                                        vendorCreditId: vc.vendorCreditId,
                                                        vendorCreditTranid: vc.vendorCreditTranid,
                                                        nardaNumber: vcNardaNumber,
                                                        totalAmount: vcTotalAmount,
                                                        matchingVRA: vc.matchingVRA,
                                                        extractedData: part.jsonResult.extractedData,
                                                        attachmentResult: vc.attachmentResult,
                                                        deliveryAmountProcessed: vc.deliveryAmountProcessed || false
                                                    });
                                                });
                                            }

                                            // UPDATED: Process skipped entries - count each individual part
                                            if (jeResult.skippedTransactions && jeResult.skippedTransactions.length > 0) {
                                                jeResult.skippedTransactions.forEach(function (skipped) {
                                                    // Extract NARDA number and total amount from skipped entry
                                                    var skippedNardaNumber = skipped.nardaNumber || 'Unknown';
                                                    var skippedTotalAmount = null;

                                                    if (skipped.totalAmount !== undefined) {
                                                        skippedTotalAmount = Math.abs(skipped.totalAmount);
                                                    } else if (part.jsonResult.extractedData.groupedLineItems &&
                                                        part.jsonResult.extractedData.groupedLineItems[skippedNardaNumber]) {
                                                        skippedTotalAmount = part.jsonResult.extractedData.groupedLineItems[skippedNardaNumber].totalAmount;
                                                    }

                                                    skippedEntries.push({
                                                        recordId: recordId,
                                                        sourceFileName: copyResult.newFileName,
                                                        splitPartNumber: part.partNumber,
                                                        splitFileName: part.fileName,
                                                        pdfUrl: 'https://system.netsuite.com' + file.load({ id: part.fileId }).url,
                                                        nardaNumber: skippedNardaNumber,
                                                        totalAmount: skippedTotalAmount,
                                                        skipReason: skipped.skipReason,
                                                        skipType: skipped.skipType,
                                                        extractedData: part.jsonResult.extractedData,
                                                        matchingVRA: skipped.matchingVRA,
                                                        vendorCreditError: skipped.vendorCreditError,
                                                        existingJournalEntry: skipped.existingJournalEntry,
                                                        existingVendorCredit: skipped.existingVendorCredit,
                                                        isDuplicate: skipped.isDuplicate || false
                                                    });
                                                });
                                            }

                                        } else {
                                            // UPDATED: Handle failed transaction creation - count each part separately
                                            var failedPdfSaveResult = savePDFToFailedFolder(
                                                part.fileId,
                                                part.fileName,
                                                recordId,
                                                part.jsonResult.extractedData
                                            );

                                            failedEntries.push({
                                                success: false,
                                                recordId: recordId,
                                                sourceFileName: copyResult.newFileName,
                                                splitPartNumber: part.partNumber,
                                                splitFileName: part.fileName,
                                                pdfUrl: 'https://system.netsuite.com' + file.load({ id: part.fileId }).url,
                                                extractedData: part.jsonResult.extractedData,
                                                error: jeResult.error,
                                                isDuplicate: jeResult.isDuplicate || false,
                                                existingJournalEntry: jeResult.existingJournalEntry || null,
                                                existingVendorCredit: jeResult.existingVendorCredit || null,
                                                failedPdfSaved: failedPdfSaveResult.success,
                                                failedPdfId: failedPdfSaveResult.success ? failedPdfSaveResult.fileId : null,
                                                failedPdfError: failedPdfSaveResult.success ? null : failedPdfSaveResult.error
                                            });
                                        }
                                    } else {
                                        // UPDATED: Handle JSON conversion failures - count each part separately
                                        var jsonFailedPdfSaveResult = savePDFToFailedFolder(
                                            part.fileId,
                                            part.fileName,
                                            recordId,
                                            { error: 'JSON conversion failed' }
                                        );

                                        failedEntries.push({
                                            success: false,
                                            recordId: recordId,
                                            sourceFileName: copyResult.newFileName,
                                            splitPartNumber: part.partNumber,
                                            splitFileName: part.fileName,
                                            pdfUrl: 'https://system.netsuite.com' + file.load({ id: part.fileId }).url,
                                            extractedData: null,
                                            error: 'JSON conversion failed: ' + (part.jsonResult ? part.jsonResult.error : 'Unknown error'),
                                            failedPdfSaved: jsonFailedPdfSaveResult.success,
                                            failedPdfId: jsonFailedPdfSaveResult.success ? jsonFailedPdfSaveResult.fileId : null,
                                            failedPdfError: jsonFailedPdfSaveResult.success ? null : jsonFailedPdfSaveResult.error
                                        });
                                    }
                                });

                                // Update custom record memo with processing results
                                var updateResult = updateCustomRecordMemo(recordId, {
                                    sourceFileName: copyResult.newFileName,
                                    needsSplitting: needsSplitting,
                                    splitParts: splitResult.savedParts.map(function (part) {
                                        var partResult = {
                                            fileName: part.fileName,
                                            fileId: part.fileId,
                                            partNumber: part.partNumber,
                                            success: false,
                                            extractedData: null,
                                            transactions: []
                                        };

                                        if (part.jsonResult && part.jsonResult.success) {
                                            partResult.success = true;
                                            partResult.extractedData = part.jsonResult.extractedData;

                                            // Find transactions created for this part
                                            var partTransactions = [];

                                            // Check for journal entries
                                            processedDetails.forEach(function (detail) {
                                                if (detail.splitPartNumber === part.partNumber &&
                                                    detail.recordId === recordId &&
                                                    !detail.isVendorCredit) {
                                                    partTransactions.push({
                                                        type: 'journalEntry',
                                                        id: detail.journalEntryId,
                                                        tranid: detail.tranid,
                                                        nardaNumber: detail.nardaNumber,
                                                        totalAmount: detail.totalAmount
                                                    });
                                                }
                                            });

                                            // Check for vendor credits
                                            processedDetails.forEach(function (detail) {
                                                if (detail.splitPartNumber === part.partNumber &&
                                                    detail.recordId === recordId &&
                                                    detail.isVendorCredit) {
                                                    partTransactions.push({
                                                        type: 'vendorCredit',
                                                        id: detail.vendorCreditId,
                                                        tranid: detail.vendorCreditTranid,
                                                        nardaNumber: detail.nardaNumber,
                                                        totalAmount: detail.totalAmount,
                                                        matchingVRA: detail.matchingVRA
                                                    });
                                                }
                                            });

                                            // Check for skipped entries
                                            skippedEntries.forEach(function (skipped) {
                                                if (skipped.splitPartNumber === part.partNumber &&
                                                    skipped.recordId === recordId) {
                                                    partTransactions.push({
                                                        type: 'skipped',
                                                        nardaNumber: skipped.nardaNumber,
                                                        totalAmount: skipped.totalAmount,
                                                        skipReason: skipped.skipReason,
                                                        skipType: skipped.skipType
                                                    });
                                                }
                                            });

                                            // Check for failed entries
                                            failedEntries.forEach(function (failed) {
                                                if (failed.splitPartNumber === part.partNumber &&
                                                    failed.recordId === recordId) {
                                                    partTransactions.push({
                                                        type: 'failed',
                                                        error: failed.error,
                                                        isDuplicate: failed.isDuplicate
                                                    });
                                                }
                                            });

                                            partResult.transactions = partTransactions;

                                        } else {
                                            partResult.error = part.jsonResult ? part.jsonResult.error : 'JSON conversion failed';
                                        }

                                        return partResult;
                                    })
                                });

                                if (updateResult.success) {
                                    // UPDATED: Track record status separately
                                    if (updateResult.recordDeactivated) {
                                        recordsInactivated++;
                                    } else {
                                        recordsLeftActive++;
                                    }

                                    log.debug('Custom record processed', {
                                        recordId: recordId,
                                        recordType: 'customrecord_eff_nsp2p_xml2nstrans',
                                        deactivated: updateResult.recordDeactivated,
                                        needsSplitting: needsSplitting,
                                        hasSuccessfulProcessing: hasSuccessfulProcessing
                                    });
                                }
                            }
                        } else {
                            log.error('Failed to process PDF', {
                                originalFileId: pdfFileColumnValue,
                                publicUrl: copyResult.publicUrl,
                                fileName: copyResult.newFileName,
                                needsSplitting: needsSplitting,
                                error: splitResult.error,
                                recordId: recordId
                            });
                        }
                    } else {
                        log.error('Failed to copy PDF file', {
                            originalFileId: pdfFileColumnValue,
                            error: copyResult.error,
                            recordId: recordId
                        });
                    }
                } else {
                    log.debug('No PDF File', {
                        recordId: recordId,
                        message: 'No PDF file found for this record'
                    });
                }
            } // CLOSING BRACE FOR THE for LOOP

            log.debug('Script Complete', {
                totalZoneCaptureRecordsProcessed: searchResultCount,
                singlePdfFiles: singlePdfCount,
                splitPdfFiles: splitPdfCount,
                totalPdfPartsProcessed: totalPdfPartsProcessed,
                pdfFilesFound: pdfFilesFound,
                pdfFilesCopied: pdfFilesCopied,
                pdfFilesSplit: pdfFilesSplit,
                splitPartsSaved: splitPartsSaved,
                jsonConversions: jsonConversions,
                journalEntriesCreated: journalEntriesCreated,
                vendorCreditsCreated: vendorCreditsCreated,
                failedEntries: failedEntries.length,
                skippedEntries: skippedEntries.length,
                recordsInactivated: recordsInactivated,
                recordsLeftActive: recordsLeftActive,
                targetFolderId: targetFolderId,
                savedSearchId: savedSearchId
            });

            // UPDATED: Send email with enhanced results
            sendResultsEmail(searchResultCount, singlePdfCount, splitPdfCount, totalPdfPartsProcessed,
                jsonConversions, journalEntriesCreated, vendorCreditsCreated,
                processedDetails, failedEntries, recordsInactivated, recordsLeftActive, skippedEntries);

            // Trigger the AR application script if we created any journal entries or vendor credits
            if (journalEntriesCreated > 0 || vendorCreditsCreated > 0) {
                try {
                    log.debug('Triggering AR Application Script', {
                        journalEntriesCreated: journalEntriesCreated,
                        vendorCreditsCreated: vendorCreditsCreated,
                        targetScript: 'customscript_bas_je_ar_appl_script',
                        targetDeployment: 'customdeploy_bas_je_ar_appl_script'
                    });

                    var arApplicationTask = task.create({
                        taskType: task.TaskType.SCHEDULED_SCRIPT
                    });

                    arApplicationTask.scriptId = 'customscript_bas_je_ar_appl_script';
                    arApplicationTask.deploymentId = 'customdeploy_bas_je_ar_appl_script';

                    var taskId = arApplicationTask.submit();

                    log.debug('AR Application Script triggered successfully', {
                        taskId: taskId,
                        scriptId: 'customscript_bas_je_ar_appl_script',
                        deploymentId: 'customdeploy_bas_je_ar_appl_script',
                        journalEntriesCreated: journalEntriesCreated,
                        vendorCreditsCreated: vendorCreditsCreated
                    });

                } catch (taskError) {
                    log.error('Error triggering AR Application Script', {
                        error: taskError.toString(),
                        targetScript: 'customscript_bas_je_ar_appl_script',
                        targetDeployment: 'customdeploy_bas_je_ar_appl_script',
                        journalEntriesCreated: journalEntriesCreated,
                        vendorCreditsCreated: vendorCreditsCreated
                    });
                }
            } else {
                log.debug('AR Application Script not triggered', {
                    reason: 'No journal entries or vendor credits were created',
                    journalEntriesCreated: journalEntriesCreated,
                    vendorCreditsCreated: vendorCreditsCreated
                });
            }

        } catch (error) {
            log.error('Script Error', {
                error: error.toString(),
                name: error.name,
                message: error.message,
                stack: error.stack
            });
        }
    } // CLOSING BRACE FOR execute FUNCTION

    // NEW: Function to process single PDF (non-split case)
    function processSinglePDF(sourceFileId, sourceFileName, sourceFileUrl, recordId, apiKey) {
        try {
            log.debug('Processing single PDF (no splitting required)', {
                sourceFileId: sourceFileId,
                sourceFileName: sourceFileName,
                sourceFileUrl: sourceFileUrl,
                recordId: recordId
            });

            // Move the file to the processed folder
            var originalFile = file.load({
                id: sourceFileId
            });

            // Create new file in processed folder
            var processedFileName = sourceFileName; // Keep original name since it's already a single part
            var processedFile = file.create({
                name: processedFileName,
                fileType: originalFile.fileType,
                contents: originalFile.getContents(),
                folder: CONFIG.FOLDERS.PROCESSED,
                isOnline: true // Keep it public for PDF.co processing
            });

            var processedFileId = processedFile.save();

            // Load the processed file to get the public URL
            var publicFile = file.load({
                id: processedFileId
            });

            var publicUrl = publicFile.url;
            if (publicUrl && publicUrl.indexOf('http') !== 0) {
                publicUrl = 'https://system.netsuite.com' + publicUrl;
            }

            log.debug('Single PDF moved to processed folder', {
                originalFileId: sourceFileId,
                processedFileId: processedFileId,
                processedFileName: processedFileName,
                publicUrl: publicUrl,
                recordId: recordId
            });

            // Convert PDF to JSON
            var jsonResult = convertPDFToJSON(publicUrl, processedFileName, 1, recordId, apiKey);

            return {
                success: true,
                splitCount: 1,
                savedParts: [{
                    success: true,
                    fileId: processedFileId,
                    fileName: processedFileName,
                    partNumber: 1,
                    publicUrl: publicUrl,
                    jsonResult: jsonResult
                }]
            };

        } catch (error) {
            log.error('Error processing single PDF', {
                error: error.toString(),
                sourceFileId: sourceFileId,
                sourceFileName: sourceFileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function copyPDFToFolderAndMakePublic(originalFileId, targetFolderId, recordId) {
        try {
            log.debug('Copying PDF to folder and making public', {
                originalFileId: originalFileId,
                targetFolderId: targetFolderId,
                recordId: recordId
            });

            // Load the original PDF file
            var originalFile = file.load({
                id: originalFileId
            });

            log.debug('Original file loaded', {
                originalFileId: originalFileId,
                fileName: originalFile.name,
                fileType: originalFile.fileType,
                fileSize: originalFile.size,
                recordId: recordId
            });

            // Create a new file name with timestamp to avoid duplicates
            var timestamp = new Date().getTime();
            var originalName = originalFile.name;
            var fileExtension = originalName.substring(originalName.lastIndexOf('.'));
            var baseName = originalName.substring(0, originalName.lastIndexOf('.'));
            var newFileName = baseName + '_copy_' + timestamp + fileExtension;

            // Create new file in the target folder with isOnline set to true
            var newFile = file.create({
                name: newFileName,
                fileType: originalFile.fileType,
                contents: originalFile.getContents(),
                folder: targetFolderId,
                isOnline: true // This makes it "Available without Login"
            });

            // Save the new file
            var newFileId = newFile.save();

            log.debug('New file created with public access enabled', {
                newFileId: newFileId,
                newFileName: newFileName,
                targetFolderId: targetFolderId,
                originalFileId: originalFileId,
                isOnline: true,
                recordId: recordId
            });

            // Load the file to get the public URL
            var publicFile = file.load({
                id: newFileId
            });

            var publicUrl = publicFile.url;

            // Make URL absolute if it's relative
            if (publicUrl && publicUrl.indexOf('http') !== 0) {
                // Try to construct absolute URL
                publicUrl = 'https://system.netsuite.com' + publicUrl;
            }

            log.debug('PDF File Public URL Generated', {
                newFileId: newFileId,
                newFileName: newFileName,
                publicUrl: publicUrl,
                isPublicAccessEnabled: true,
                targetFolderId: targetFolderId,
                originalFileId: originalFileId,
                recordId: recordId
            });

            return {
                success: true,
                newFileId: newFileId,
                newFileName: newFileName,
                publicUrl: publicUrl
            };

        } catch (error) {
            log.error('Error copying PDF file and making it public', {
                error: error.toString(),
                originalFileId: originalFileId,
                targetFolderId: targetFolderId,
                recordId: recordId
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function splitPDFWithPDFCo(sourceFileUrl, fileName, recordId, apiKey, splitText) {
        try {
            log.debug('Starting PDF split with PDF.co', {
                sourceFileUrl: sourceFileUrl,
                fileName: fileName,
                splitText: splitText,
                recordId: recordId
            });

            // JSON payload for api request
            var jsonPayload = JSON.stringify({
                searchString: splitText,
                url: sourceFileUrl
            });

            // Send request to PDF.co
            var response = https.post({
                url: 'https://api.pdf.co/v1/pdf/split2',
                body: jsonPayload,
                headers: {
                    'x-api-key': apiKey,
                    'Content-Type': 'application/json'
                }
            });

            log.debug('PDF.co API Response', {
                statusCode: response.code,
                responseBody: response.body,
                fileName: fileName,
                recordId: recordId
            });

            if (response.code === 200) {
                // Parse JSON response
                var data = JSON.parse(response.body);

                if (data.error == false) {
                    // Check if we actually got any URLs back
                    if (!data.urls || data.urls.length === 0) {
                        log.error('PDF.co split returned no URLs - PDF may not contain split text', {
                            splitText: splitText,
                            sourceFileUrl: sourceFileUrl,
                            fileName: fileName,
                            recordId: recordId,
                            pdfcoResponse: data
                        });
                        return {
                            success: false,
                            error: 'PDF split returned no parts - split text "' + splitText + '" not found in PDF'
                        };
                    }

                    log.debug('PDF split successful', {
                        numberOfParts: data.urls.length,
                        urls: data.urls,
                        fileName: fileName,
                        recordId: recordId
                    });

                    // Save each split PDF to folder 2462382
                    var splitFolderId = CONFIG.FOLDERS.PROCESSED;
                    var savedParts = [];
                    var part = 1;

                    data.urls.forEach(function (url) {
                        log.debug('Processing Split PDF Part ' + part, {
                            partNumber: part,
                            url: url,
                            originalFileName: fileName,
                            recordId: recordId
                        });

                        // Download and save each split PDF
                        var saveResult = downloadAndSaveSplitPDF(url, fileName, part, splitFolderId, recordId, apiKey);
                        if (saveResult.success) {
                            savedParts.push(saveResult);
                            log.debug('Split PDF Part ' + part + ' saved successfully', {
                                partNumber: part,
                                savedFileId: saveResult.fileId,
                                savedFileName: saveResult.fileName,
                                splitFolderId: splitFolderId,
                                recordId: recordId
                            });
                        } else {
                            log.error('Failed to save Split PDF Part ' + part, {
                                partNumber: part,
                                url: url,
                                error: saveResult.error,
                                recordId: recordId
                            });
                        }
                        part++;
                    });

                    return {
                        success: true,
                        splitCount: data.urls.length,
                        urls: data.urls,
                        savedParts: savedParts
                    };
                } else {
                    // Service reported error
                    log.error('PDF.co service error', {
                        error: data.message,
                        fileName: fileName,
                        recordId: recordId
                    });
                    return {
                        success: false,
                        error: data.message
                    };
                }
            } else {
                log.error('PDF.co API request failed', {
                    statusCode: response.code,
                    responseBody: response.body,
                    fileName: fileName,
                    recordId: recordId
                });
                return {
                    success: false,
                    error: 'HTTP ' + response.code
                };
            }

        } catch (error) {
            log.error('Error splitting PDF with PDF.co', {
                error: error.toString(),
                sourceFileUrl: sourceFileUrl,
                fileName: fileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function downloadAndSaveSplitPDF(pdfUrl, originalFileName, partNumber, folderId, recordId, apiKey) {
        try {
            log.debug('Downloading split PDF part', {
                url: pdfUrl,
                partNumber: partNumber,
                originalFileName: originalFileName,
                folderId: folderId,
                recordId: recordId
            });

            // Download the PDF content
            var response = https.get({
                url: pdfUrl
            });

            if (response.code === 200) {
                // Create filename for the split part
                var originalName = originalFileName;
                var fileExtension = originalName.substring(originalName.lastIndexOf('.'));
                var baseName = originalName.substring(0, originalName.lastIndexOf('.'));
                var splitFileName = baseName + '_part' + partNumber + fileExtension;

                // Create a new file in NetSuite with the split PDF
                var newFile = file.create({
                    name: splitFileName,
                    fileType: file.Type.PDF,
                    contents: response.body,
                    folder: folderId,
                    isOnline: true // Make it available without login
                });

                var savedFileId = newFile.save();

                // Load the file to get the public URL for PDF.co processing
                var publicFile = file.load({
                    id: savedFileId
                });

                var publicUrl = publicFile.url;

                // Make URL absolute if it's relative
                if (publicUrl && publicUrl.indexOf('http') !== 0) {
                    publicUrl = 'https://system.netsuite.com' + publicUrl;
                }

                log.debug('Split PDF part saved to NetSuite', {
                    savedFileId: savedFileId,
                    fileName: splitFileName,
                    partNumber: partNumber,
                    folderId: folderId,
                    originalFileName: originalFileName,
                    publicUrl: publicUrl,
                    recordId: recordId
                });

                // Convert PDF to JSON using PDF.co with improved extraction
                var jsonResult = convertPDFToJSON(publicUrl, splitFileName, partNumber, recordId, apiKey);

                return {
                    success: true,
                    fileId: savedFileId,
                    fileName: splitFileName,
                    partNumber: partNumber,
                    publicUrl: publicUrl,
                    jsonResult: jsonResult
                };

            } else {
                log.error('Failed to download split PDF', {
                    statusCode: response.code,
                    url: pdfUrl,
                    partNumber: partNumber,
                    recordId: recordId
                });
                return {
                    success: false,
                    error: 'HTTP ' + response.code
                };
            }

        } catch (error) {
            log.error('Error downloading and saving split PDF', {
                error: error.toString(),
                url: pdfUrl,
                partNumber: partNumber,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function convertPDFToJSON(sourceFileUrl, fileName, partNumber, recordId, apiKey) {
        try {
            log.debug('Converting PDF to JSON with PDF.co', {
                sourceFileUrl: sourceFileUrl,
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });

            // Create destination file name for JSON
            var baseName = fileName.substring(0, fileName.lastIndexOf('.'));
            var destinationFile = baseName + '.json';

            // JSON payload for api request
            var jsonPayload = JSON.stringify({
                name: destinationFile,
                password: "",
                pages: "",
                url: sourceFileUrl
            });

            // Send request to PDF.co
            var response = https.post({
                url: 'https://api.pdf.co/v1/pdf/convert/to/json',
                body: jsonPayload,
                headers: {
                    'x-api-key': apiKey,
                    'Content-Type': 'application/json'
                }
            });

            log.debug('PDF.co JSON conversion response', {
                statusCode: response.code,
                responseBody: response.body,
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });

            if (response.code === 200) {
                // Parse JSON response
                var data = JSON.parse(response.body);

                if (data.error == false) {
                    log.debug('PDF to JSON conversion successful', {
                        jsonUrl: data.url,
                        fileName: fileName,
                        partNumber: partNumber,
                        recordId: recordId
                    });

                    // Save the JSON file to NetSuite file cabinet
                    var jsonSaveResult = saveJSONFileToFileCabinet(data.url, baseName + '.json', fileName, partNumber, recordId);

                    // Download and process the JSON content
                    var jsonContent = downloadJSONContent(data.url, fileName, partNumber, recordId);

                    // Log extracted data if successful
                    if (jsonContent.success && jsonContent.extractedData) {
                        log.debug('FINAL EXTRACTED DATA', {
                            fileName: fileName,
                            partNumber: partNumber,
                            recordId: recordId,
                            nardaNumber: jsonContent.extractedData.nardaNumber,
                            totalAmount: jsonContent.extractedData.totalAmount,
                            invoiceDate: jsonContent.extractedData.invoiceDate,
                            invoiceNumber: jsonContent.extractedData.invoiceNumber,
                            extractionSuccessful: (
                                jsonContent.extractedData.nardaNumber !== null ||
                                jsonContent.extractedData.totalAmount !== null ||
                                jsonContent.extractedData.invoiceDate !== null ||
                                jsonContent.extractedData.invoiceNumber !== null
                            ),
                            allFieldsFound: (
                                jsonContent.extractedData.nardaNumber !== null &&
                                jsonContent.extractedData.totalAmount !== null &&
                                jsonContent.extractedData.invoiceDate !== null &&
                                jsonContent.extractedData.invoiceNumber !== null
                            )
                        });
                    }

                    return {
                        success: true,
                        jsonUrl: data.url,
                        jsonContent: jsonContent,
                        extractedData: jsonContent.extractedData,
                        savedJsonFile: jsonSaveResult
                    };
                } else {
                    // Service reported error
                    log.error('PDF.co JSON conversion error', {
                        error: data.message,
                        fileName: fileName,
                        partNumber: partNumber,
                        recordId: recordId
                    });
                    return {
                        success: false,
                        error: data.message
                    };
                }
            } else {
                log.error('PDF.co JSON conversion request failed', {
                    statusCode: response.code,
                    responseBody: response.body,
                    fileName: fileName,
                    partNumber: partNumber,
                    recordId: recordId
                });
                return {
                    success: false,
                    error: 'HTTP ' + response.code
                };
            }

        } catch (error) {
            log.error('Error converting PDF to JSON', {
                error: error.toString(),
                sourceFileUrl: sourceFileUrl,
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function saveJSONFileToFileCabinet(jsonUrl, jsonFileName, originalFileName, partNumber, recordId) {
        try {
            log.debug('Saving JSON file to NetSuite file cabinet', {
                jsonUrl: jsonUrl,
                jsonFileName: jsonFileName,
                originalFileName: originalFileName,
                partNumber: partNumber,
                recordId: recordId,
                targetFolderId: CONFIG.FOLDERS.JSON
            });

            // Download the JSON content from PDF.co
            var response = https.get({
                url: jsonUrl
            });

            if (response.code === 200) {
                // Create a new file in NetSuite with the JSON content
                var newFile = file.create({
                    name: jsonFileName,
                    fileType: file.Type.JSON,
                    contents: response.body,
                    folder: CONFIG.FOLDERS.JSON, // Target folder ID for JSON files
                    isOnline: false // JSON files don't need to be public
                });

                var savedFileId = newFile.save();

                log.debug('JSON file saved to NetSuite file cabinet successfully', {
                    savedFileId: savedFileId,
                    jsonFileName: jsonFileName,
                    targetFolderId: CONFIG.FOLDERS.JSON,
                    originalFileName: originalFileName,
                    partNumber: partNumber,
                    recordId: recordId
                });

                return {
                    success: true,
                    fileId: savedFileId,
                    fileName: jsonFileName,
                    folderId: CONFIG.FOLDERS.JSON
                };

            } else {
                log.error('Failed to download JSON content for saving', {
                    statusCode: response.code,
                    jsonUrl: jsonUrl,
                    jsonFileName: jsonFileName,
                    partNumber: partNumber,
                    recordId: recordId
                });
                return {
                    success: false,
                    error: 'HTTP ' + response.code + ' - Failed to download JSON content'
                };
            }

        } catch (error) {
            log.error('Error saving JSON file to NetSuite file cabinet', {
                error: error.toString(),
                jsonUrl: jsonUrl,
                jsonFileName: jsonFileName,
                partNumber: partNumber,
                recordId: recordId,
                targetFolderId: CONFIG.FOLDERS.JSON
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function downloadJSONContent(jsonUrl, fileName, partNumber, recordId) {
        try {
            log.debug('Downloading JSON content', {
                jsonUrl: jsonUrl,
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });

            // Download the JSON content
            var response = https.get({
                url: jsonUrl
            });

            if (response.code === 200) {
                var jsonContent = JSON.parse(response.body);

                log.debug('JSON content downloaded and parsed', {
                    fileName: fileName,
                    partNumber: partNumber,
                    jsonDataKeys: Object.keys(jsonContent),
                    recordId: recordId
                });

                // Extract data using new structured approach
                var extractedData = extractDocumentAndLineItemData(jsonContent, fileName, partNumber, recordId);

                log.debug('FINAL EXTRACTED DATA', {
                    fileName: fileName,
                    partNumber: partNumber,
                    recordId: recordId,
                    invoiceNumber: extractedData.invoiceNumber,
                    invoiceDate: extractedData.invoiceDate,
                    deliveryAmount: extractedData.deliveryAmount,
                    totalLineItems: extractedData.lineItems ? extractedData.lineItems.length : 0,
                    groupedNARDAs: extractedData.groupedLineItems ? Object.keys(extractedData.groupedLineItems) : [],
                    extractionSuccessful: extractedData.extractionSuccessful,
                    allFieldsFound: extractedData.allFieldsFound
                });

                return {
                    success: true,
                    content: jsonContent,
                    extractedData: extractedData
                };

            } else {
                log.error('Failed to download JSON content', {
                    statusCode: response.code,
                    jsonUrl: jsonUrl,
                    partNumber: partNumber,
                    recordId: recordId
                });
                return {
                    success: false,
                    error: 'HTTP ' + response.code
                };
            }

        } catch (error) {
            log.error('Error downloading JSON content', {
                error: error.toString(),
                jsonUrl: jsonUrl,
                partNumber: partNumber,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function extractDocumentAndLineItemData(jsonContent, fileName, partNumber, recordId) {
        try {
            log.debug('Extracting document and line item data using structured approach', {
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });

            // Collect all text elements first
            var allTextElements = [];

            function collectAllTextElements(obj) {
                if (obj && typeof obj === 'object') {
                    if (obj.text && obj.text['@x'] && obj.text['@y'] && obj.text['#text']) {
                        allTextElements.push({
                            text: obj.text['#text'],
                            x: parseFloat(obj.text['@x']),
                            y: parseFloat(obj.text['@y'])
                        });
                    }

                    for (var key in obj) {
                        if (obj.hasOwnProperty(key)) {
                            collectAllTextElements(obj[key]);
                        }
                    }
                } else if (Array.isArray(obj)) {
                    for (var i = 0; i < obj.length; i++) {
                        collectAllTextElements(obj[i]);
                    }
                }
            }

            collectAllTextElements(jsonContent);

            log.debug('Collected text elements', {
                totalElements: allTextElements.length,
                fileName: fileName
            });

            // Extract whole document data
            var documentData = extractWholeDocumentData(allTextElements, fileName, recordId);

            // Extract line item data
            var lineItemData = extractLineItemData(allTextElements, fileName, recordId);

            // Group line items by NARDA
            var groupedLineItems = groupLineItemsByNARDA(lineItemData.lineItems);

            // Determine extraction success
            var extractionSuccessful = (
                documentData.invoiceNumber !== null ||
                documentData.invoiceDate !== null ||
                lineItemData.lineItems.length > 0
            );

            var allFieldsFound = (
                documentData.invoiceNumber !== null &&
                documentData.invoiceDate !== null &&
                lineItemData.lineItems.length > 0
            );

            return {
                success: true,
                invoiceNumber: documentData.invoiceNumber,
                invoiceDate: documentData.invoiceDate,
                deliveryAmount: documentData.deliveryAmount,
                lineItems: lineItemData.lineItems,
                groupedLineItems: groupedLineItems,
                fileName: fileName,
                partNumber: partNumber,
                extractionSuccessful: extractionSuccessful,
                allFieldsFound: allFieldsFound
            };

        } catch (error) {
            log.error('Error extracting document and line item data', {
                error: error.toString(),
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function extractWholeDocumentData(allTextElements, fileName, recordId) {
        try {
            log.debug('Extracting whole document data', {
                fileName: fileName,
                recordId: recordId,
                totalElements: allTextElements.length
            });

            var invoiceNumber = null;
            var invoiceDate = null;
            var deliveryAmount = null;

            // Find Invoice Number - UPDATED to handle both "Invoice Number" and "Invoice:" patterns
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                var elementText = element.text.toLowerCase().trim();

                // Check for both "invoice number" and "invoice:" patterns
                if (elementText.indexOf('invoice number') !== -1 || elementText === 'invoice:') {
                    log.debug('Found Invoice Number label', {
                        text: element.text,
                        coordinates: { x: element.x, y: element.y },
                        fileName: fileName
                    });

                    // Look for value in same row (same Y coordinate within tolerance)
                    for (var j = 0; j < allTextElements.length; j++) {
                        var valueElement = allTextElements[j];
                        if (Math.abs(valueElement.y - element.y) < 2 && valueElement.x > element.x) {
                            // Check if it starts with 6 and looks like an invoice number pattern
                            if (/^[67]\d{7}$/ .test(valueElement.text)) {
                                invoiceNumber = valueElement.text;
                                log.debug('Invoice Number found', {
                                    invoiceNumber: invoiceNumber,
                                    coordinates: { x: valueElement.x, y: valueElement.y },
                                    labelFound: element.text,
                                    fileName: fileName
                                });
                                break;
                            }
                        }
                    }
                    if (invoiceNumber) break;
                }
            }

            // Find Invoice Date
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                if (element.text.toLowerCase().indexOf('invoice date') !== -1) {
                    log.debug('Found Invoice Date label', {
                        text: element.text,
                        coordinates: { x: element.x, y: element.y },
                        fileName: fileName
                    });

                    // Look for value in same row (same Y coordinate within tolerance)
                    for (var j = 0; j < allTextElements.length; j++) {
                        var valueElement = allTextElements[j];
                        if (Math.abs(valueElement.y - element.y) < 2 && valueElement.x > element.x) {
                            // Check if it looks like a date pattern
                            if (/\d{1,2}\/\d{1,2}\/\d{4}/.test(valueElement.text)) {
                                invoiceDate = valueElement.text;
                                log.debug('Invoice Date found', {
                                    invoiceDate: invoiceDate,
                                    coordinates: { x: valueElement.x, y: valueElement.y },
                                    fileName: fileName
                                });
                                break;
                            }
                        }
                    }
                    if (invoiceDate) break;
                }
            }

            // Find Delivery Amount
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                if (element.text.toLowerCase().indexOf('delivery') !== -1 && element.text.indexOf(':') !== -1) {
                    log.debug('Found Delivery label', {
                        text: element.text,
                        coordinates: { x: element.x, y: element.y },
                        fileName: fileName
                    });

                    // Look for value in same row (same Y coordinate within tolerance)
                    for (var j = 0; j < allTextElements.length; j++) {
                        var valueElement = allTextElements[j];
                        if (Math.abs(valueElement.y - element.y) < 2 && valueElement.x > element.x) {
                            // Check if it looks like a currency amount
                            if (/\$\d+\.\d{2}/.test(valueElement.text)) {
                                deliveryAmount = valueElement.text;
                                log.debug('Delivery Amount found', {
                                    deliveryAmount: deliveryAmount,
                                    coordinates: { x: valueElement.x, y: valueElement.y },
                                    fileName: fileName
                                });
                                break;
                            }
                        }
                    }
                    if (deliveryAmount) break;
                }
            }

            log.debug('Whole document data extraction complete', {
                invoiceNumber: invoiceNumber,
                invoiceDate: invoiceDate,
                deliveryAmount: deliveryAmount,
                fileName: fileName
            });

            return {
                invoiceNumber: invoiceNumber,
                invoiceDate: invoiceDate,
                deliveryAmount: deliveryAmount
            };

        } catch (error) {
            log.error('Error extracting whole document data', {
                error: error.toString(),
                fileName: fileName,
                recordId: recordId
            });
            return {
                invoiceNumber: null,
                invoiceDate: null,
                deliveryAmount: null
            };
        }
    }

    function extractLineItemData(allTextElements, fileName, recordId) {
        try {
            log.debug('Extracting line item data', {
                fileName: fileName,
                recordId: recordId,
                totalElements: allTextElements.length
            });

            var nardaColumnX = null;
            var totalColumnX = null;
            var descriptionColumnX = null;
            var lineItems = [];

            // Find NARDA column header - UPDATED to handle "NARDA #"
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                var headerText = element.text.toUpperCase().trim();

                // Check for both "NARDA" and "NARDA #" patterns
                if (headerText === 'NARDA' || headerText === 'NARDA #' || headerText.indexOf('NARDA') === 0) {
                    nardaColumnX = element.x;
                    log.debug('NARDA column found', {
                        text: element.text,
                        x: element.x,
                        y: element.y,
                        fileName: fileName
                    });
                    break;
                }
            }

            // Find Total column header (first instance)
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                if (element.text.toUpperCase() === 'TOTAL') {
                    totalColumnX = element.x;
                    log.debug('Total column found', {
                        x: totalColumnX,
                        y: element.y,
                        fileName: fileName
                    });
                    break;
                }
            }

            // Find Description column header
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                if (element.text.toUpperCase() === 'DESCRIPTION') {
                    descriptionColumnX = element.x;
                    log.debug('Description column found', {
                        x: descriptionColumnX,
                        y: element.y,
                        fileName: fileName
                    });
                    break;
                }
            }

            if (!nardaColumnX || !totalColumnX) {
                log.error('Required column headers not found', {
                    nardaColumnX: nardaColumnX,
                    totalColumnX: totalColumnX,
                    fileName: fileName
                });
                return { lineItems: [] };
            }

            // Find NARDA values in the NARDA column
            var nardaValues = [];
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                if (Math.abs(element.x - nardaColumnX) < 5) {
                    var textUpper = element.text.toUpperCase().trim();

                    // Known NARDA patterns for vendor credits and journal entries
                    var knownPatterns = [
                        /^CONCDA$/i,          // CONCDA (vendor credit)
                        /^CONCDAM$/i,         // CONCDAM (vendor credit)
                        /^NF$/i,              // NF (vendor credit)
                        /^CORE$/i,            // CORE (vendor credit)
                        /^CONCES$/i,          // CONCESSION part 1 (vendor credit)
                        /^CONCESSION$/i,      // CONCESSION full (vendor credit)
                        /^J\d{4,6}$/i,        // J followed by 4-6 digits (journal entries)
                        /^INV\d+$/i,          // INV followed by numbers (journal entries)
                        /^SHORT$/i,           // SHORT (short ship)
                        /^BOX$/i              // BOX (short ship)
                    ];

                    var matchesPattern = false;
                    for (var p = 0; p < knownPatterns.length; p++) {
                        if (knownPatterns[p].test(textUpper)) {
                            matchesPattern = true;
                            break;
                        }
                    }

                    // Also check for partial patterns that might complete on next line
                    var partialPatterns = [
                        /^INV\d+$/i,          // INV with any digits (might continue on next line)
                        /^J\d+$/i,            // J with any digits (might continue on next line)
                        /^CONCES$/i           // CONCESSION part 1
                    ];

                    var isPartialPattern = false;
                    for (var p = 0; p < partialPatterns.length; p++) {
                        if (partialPatterns[p].test(textUpper)) {
                            isPartialPattern = true;
                            break;
                        }
                    }

                    if (matchesPattern || isPartialPattern) {
                        // Use two-line capture to get complete NARDA value
                        var completeNardaValue = findCompleteNardaValue(element, allTextElements);
                        var completeNardaUpper = completeNardaValue.toUpperCase().trim();

                        // Validate the complete value against known patterns
                        var isValidCompletePattern = false;

                        // Check all known patterns again with complete value
                        for (var p = 0; p < knownPatterns.length; p++) {
                            if (knownPatterns[p].test(completeNardaUpper)) {
                                isValidCompletePattern = true;
                                break;
                            }
                        }

                        // Special handling for INV patterns (allow any number of digits)
                        if (/^INV\d+$/i.test(completeNardaUpper)) {
                            isValidCompletePattern = true;
                        }

                        // Special handling for J patterns (allow any number of digits)
                        if (/^J\d+$/i.test(completeNardaUpper)) {
                            isValidCompletePattern = true;
                        }

                        // Special handling for CONCESSION (might be split as CONCES + SION)
                        if (completeNardaUpper === 'CONCESSION' || completeNardaUpper === 'CONCESSSION') {
                            isValidCompletePattern = true;
                            completeNardaValue = 'CONCESSION'; // Normalize
                        }

                        if (isValidCompletePattern) {
                            nardaValues.push({
                                text: completeNardaValue,        // CRITICAL: Complete combined NARDA value
                                originalText: element.text,       // Keep original for reference
                                x: element.x,
                                y: element.y,
                                nardaCoordinates: {
                                    x: element.x,
                                    y: element.y
                                }
                            });

                            log.debug('NARDA value found (with two-line capture)', {
                                narda: completeNardaValue,
                                originalText: element.text,
                                coordinates: { x: element.x, y: element.y },
                                fileName: fileName
                            });
                        }
                    }
                }
            }

            // NEW: If no NARDA values found in NARDA column, search description column
            if (nardaValues.length === 0 && descriptionColumnX) {
                log.debug('No NARDA values found in NARDA column, searching description column for embedded values', {
                    fileName: fileName,
                    nardaColumnX: nardaColumnX,
                    descriptionColumnX: descriptionColumnX
                });

                for (var i = 0; i < allTextElements.length; i++) {
                    var element = allTextElements[i];

                    // Look in description column area (within 50 units, below headers)
                    var isInDescriptionColumn = Math.abs(element.x - descriptionColumnX) < 50;
                    var isInDataArea = element.y > 400 && element.y < 500;

                    if (isInDescriptionColumn && isInDataArea) {
                        // Look for embedded NARDA patterns  
                        var embeddedMatches = element.text.match(/\b(CONCDA|CONCDAM|CORE|SHORT|BOX|NF|J\d{4,6}|INV\d{4,6})\b/gi);
                        if (embeddedMatches && embeddedMatches.length > 0) {
                            var nardaValue = embeddedMatches[embeddedMatches.length - 1].toUpperCase();

                            // Clean J numbers (remove trailing letters if any)
                            if (/^J\d{4,6}[A-Z]*$/i.test(nardaValue)) {
                                var jMatch = nardaValue.match(/^(J\d{4,6})/i);
                                if (jMatch) {
                                    nardaValue = jMatch[1].toUpperCase();
                                }
                            }

                            log.debug('Found embedded NARDA in description', {
                                nardaValue: nardaValue,
                                fullText: element.text,
                                allMatches: embeddedMatches,
                                coordinates: { x: element.x, y: element.y },
                                fileName: fileName
                            });

                            nardaValues.push({
                                text: nardaValue,  // Use text instead of narda for consistency
                                x: element.x,
                                y: element.y,
                                nardaCoordinates: {
                                    x: element.x,
                                    y: element.y
                                }
                            });
                            break; // Only take the first embedded NARDA found
                        }
                    }
                }
            }

            // Process each NARDA value and create line items
            for (var i = 0; i < nardaValues.length; i++) {
                var nardaItem = nardaValues[i];
                var nardaY = nardaItem.y;

                // CRITICAL FIX: Find total amount with FLEXIBLE Y-coordinate matching (2 pixels instead of 1)
                var totalAmount = null;
                for (var j = 0; j < allTextElements.length; j++) {
                    var element = allTextElements[j];
                    // Increased Y tolerance from 1 to 2 pixels
                    if (Math.abs(element.x - totalColumnX) < 5 && Math.abs(element.y - nardaY) < 2) {
                        totalAmount = element.text;

                        log.debug('Total amount found for NARDA', {
                            narda: nardaItem.text,
                            totalAmount: totalAmount,
                            coordinates: { x: element.x, y: element.y },
                            nardaY: nardaY,
                            yDifference: Math.abs(element.y - nardaY),
                            fileName: fileName
                        });
                        break;
                    }
                }

                if (!totalAmount) {
                    log.debug('No total amount found for NARDA, skipping', {
                        narda: nardaItem.text,
                        nardaY: nardaY,
                        fileName: fileName
                    });
                    continue;
                }

                // Find original bill number using two-line approach
                var originalBillNumber = findOriginalBillNumber(nardaItem, allTextElements);

                if (originalBillNumber) {
                    log.debug('Original bill number found for NARDA using two-line approach', {
                        narda: nardaItem.text,
                        originalBillNumber: originalBillNumber,
                        nardaY: nardaY,
                        fileName: fileName
                    });
                } else {
                    log.debug('No original bill number found for NARDA', {
                        narda: nardaItem.text,
                        nardaY: nardaY,
                        fileName: fileName
                    });
                }

                // CRITICAL: Add line item with COMPLETE NARDA VALUE
                lineItems.push({
                    nardaNumber: nardaItem.text,  // This now contains the complete combined value
                    totalAmount: totalAmount,
                    originalBillNumber: originalBillNumber,
                    nardaCoordinates: nardaItem.nardaCoordinates,
                    rowY: nardaY
                });

                log.debug('Line item added', {
                    nardaNumber: nardaItem.text,
                    totalAmount: totalAmount,
                    originalBillNumber: originalBillNumber,
                    fileName: fileName
                });
            }

            log.debug('Line item data extraction complete', {
                totalLineItems: lineItems.length,
                fileName: fileName
            });

            // Deduplicate line items before returning
            lineItems = deduplicateLineItems(lineItems);

            return {
                lineItems: lineItems,
                nardaColumnX: nardaColumnX,
                totalColumnX: totalColumnX,
                descriptionColumnX: descriptionColumnX
            };

        } catch (error) {
            log.error('Error extracting line item data', {
                error: error.toString(),
                fileName: fileName,
                recordId: recordId
            });
            return {
                lineItems: []
            };
        }
    }

    /**
 * Finds original bill number for a NARDA item using two-line combination approach
 * @param {Object} nardaItem - NARDA item with coordinates
 * @param {Array} allTextElements - All text elements from PDF
 * @returns {string|null} Original bill number or null if not found
 */
    function findOriginalBillNumber(nardaItem, allTextElements) {
        try {
            var nardaY = nardaItem.y;
            var descriptionColumnX = 122.54; // Description column X position
            var yTolerance = 2; // Same row tolerance
            var nextLineYMax = 15; // Maximum distance for "next line"
            var xTolerance = 50; // Description column width tolerance

            // Step 1: Find description text on SAME row as NARDA
            var currentLineText = '';
            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                var isInDescriptionColumn = Math.abs(element.x - descriptionColumnX) < xTolerance;
                var isOnSameRow = Math.abs(element.y - nardaY) < yTolerance;

                if (isInDescriptionColumn && isOnSameRow) {
                    currentLineText += element.text;
                    log.debug('Found description text on same row as NARDA', {
                        nardaY: nardaY,
                        text: element.text,
                        coordinates: { x: element.x, y: element.y }
                    });
                }
            }

            // Step 2: Find next line text (within 15 pixels below)
            var nextLineText = '';
            var nextLineY = null;

            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                var isInDescriptionColumn = Math.abs(element.x - descriptionColumnX) < xTolerance;
                var yDistance = element.y - nardaY;
                var isNextLine = yDistance > 0 && yDistance <= nextLineYMax;

                if (isInDescriptionColumn && isNextLine) {
                    // Take the first text element found on the next line
                    if (nextLineY === null || Math.abs(element.y - nextLineY) < 2) {
                        nextLineText += element.text;
                        nextLineY = element.y;
                        log.debug('Found description text on next line', {
                            nardaY: nardaY,
                            nextLineY: element.y,
                            yDistance: yDistance,
                            text: element.text,
                            coordinates: { x: element.x, y: element.y }
                        });
                    }
                }
            }

            // Step 3: Combine both lines
            var combinedText = currentLineText + nextLineText;

            log.debug('Combined text from two lines', {
                currentLineText: currentLineText,
                nextLineText: nextLineText,
                combinedText: combinedText,
                nardaY: nardaY
            });

            // Step 4: Extract bill number from combined text using pattern matching
            // Look for patterns in priority order: HN, W, N (all followed by 7-10 digits)
            var billNumber = null;

            // Find ALL matches for each pattern
            var hnMatches = combinedText.match(/HN(\d{7,10})/gi);
            var wMatches = combinedText.match(/W(\d{7,10})/gi);
            var nMatches = combinedText.match(/N(\d{7,10})/gi);

            // Take the LAST match from the highest priority pattern found
            if (hnMatches && hnMatches.length > 0) {
                var lastMatch = hnMatches[hnMatches.length - 1];
                var digits = lastMatch.match(/\d{7,10}/);
                if (digits) {
                    billNumber = digits[0];
                    log.debug('Original bill number found - HN pattern (last match)', {
                        pattern: 'HN',
                        fullMatch: lastMatch,
                        billNumber: billNumber,
                        totalMatches: hnMatches.length,
                        allMatches: hnMatches,
                        combinedText: combinedText
                    });
                }
            } else if (wMatches && wMatches.length > 0) {
                var lastMatch = wMatches[wMatches.length - 1];
                var digits = lastMatch.match(/\d{7,10}/);
                if (digits) {
                    billNumber = digits[0];
                    log.debug('Original bill number found - W pattern (last match)', {
                        pattern: 'W',
                        fullMatch: lastMatch,
                        billNumber: billNumber,
                        totalMatches: wMatches.length,
                        allMatches: wMatches,
                        combinedText: combinedText
                    });
                }
            } else if (nMatches && nMatches.length > 0) {
                var lastMatch = nMatches[nMatches.length - 1];
                var digits = lastMatch.match(/\d{7,10}/);
                if (digits) {
                    billNumber = digits[0];
                    log.debug('Original bill number found - N pattern (last match)', {
                        pattern: 'N',
                        fullMatch: lastMatch,
                        billNumber: billNumber,
                        totalMatches: nMatches.length,
                        allMatches: nMatches,
                        combinedText: combinedText
                    });
                }
            }

            // Step 5: Validate digit count (7-10 digits)
            if (billNumber) {
                var digitCount = billNumber.length;
                if (digitCount >= 7 && digitCount <= 10) {
                    log.debug('Bill number validation passed', {
                        billNumber: billNumber,
                        digitCount: digitCount,
                        nardaY: nardaY
                    });
                    return billNumber;
                } else {
                    log.debug('Bill number validation failed - invalid digit count', {
                        billNumber: billNumber,
                        digitCount: digitCount,
                        expectedRange: '7-10 digits',
                        nardaY: nardaY
                    });
                    return null;
                }
            }

            log.debug('No original bill number found', {
                currentLineText: currentLineText,
                nextLineText: nextLineText,
                combinedText: combinedText,
                nardaY: nardaY
            });

            return null;

        } catch (error) {
            log.error('Error finding original bill number', {
                error: error.toString(),
                nardaY: nardaItem ? nardaItem.y : 'unknown'
            });
            return null;
        }
    }

    /**
 * Finds NARDA value using two-line combination approach
 * Handles cases where NARDA values span multiple lines (e.g., INV1666079 or CONCESSION)
 * @param {Object} nardaItem - NARDA item with coordinates from first line
 * @param {Array} allTextElements - All text elements from PDF
 * @returns {string} Complete NARDA value (possibly combined from two lines)
 */
    function findCompleteNardaValue(nardaItem, allTextElements) {
        try {
            var nardaY = nardaItem.y;
            var nardaX = nardaItem.x;
            var currentLineText = nardaItem.text.trim();
            var xTolerance = 5; // X-axis tolerance for same column
            var nextLineYMax = 15; // Maximum distance for "next line"

            log.debug('Finding complete NARDA value', {
                currentLineText: currentLineText,
                nardaY: nardaY,
                nardaX: nardaX
            });

            // Step 1: Find text on next line (within 15 pixels below, same X position)
            var nextLineText = '';
            var nextLineY = null;

            for (var i = 0; i < allTextElements.length; i++) {
                var element = allTextElements[i];
                var isInSameColumn = Math.abs(element.x - nardaX) < xTolerance;
                var yDistance = element.y - nardaY;
                var isNextLine = yDistance > 0 && yDistance <= nextLineYMax;

                if (isInSameColumn && isNextLine) {
                    if (!nextLineText || element.y < nextLineY) {
                        nextLineText = element.text.trim();
                        nextLineY = element.y;

                        log.debug('Found next line text for NARDA', {
                            nardaY: nardaY,
                            nextLineY: nextLineY,
                            yDistance: yDistance,
                            text: nextLineText,
                            nardaX: nardaX,
                            elementX: element.x
                        });
                    }
                }
            }

            // Step 2: Combine both lines
            var combinedText = currentLineText + nextLineText;

            log.debug('Combined NARDA text from two lines', {
                currentLineText: currentLineText,
                nextLineText: nextLineText,
                combinedText: combinedText,
                nardaY: nardaY
            });

            // Step 3: Return the combined value (trimmed)
            return combinedText;

        } catch (error) {
            log.error('Error finding complete NARDA value', {
                error: error.toString(),
                nardaY: nardaItem ? nardaItem.y : 'unknown',
                nardaText: nardaItem ? nardaItem.text : 'unknown'
            });
            return nardaItem.text; // Fallback to original text
        }
    }

    function deduplicateLineItems(lineItems) {
        var seen = {};
        var deduplicated = [];

        for (var i = 0; i < lineItems.length; i++) {
            var item = lineItems[i];
            var rowKey = item.rowY.toFixed(2); // Group by Y coordinate

            if (!seen[rowKey]) {
                seen[rowKey] = item;
            } else {
                // Keep the item with the LONGEST original bill number
                var existingBillLength = seen[rowKey].originalBillNumber ?
                    seen[rowKey].originalBillNumber.length : 0;
                var newBillLength = item.originalBillNumber ?
                    item.originalBillNumber.length : 0;

                if (newBillLength > existingBillLength) {
                    seen[rowKey] = item; // Replace with longer bill number
                }
            }
        }

        // Convert seen object back to array
        for (var key in seen) {
            deduplicated.push(seen[key]);
        }

        log.debug('Line items deduplicated', {
            originalCount: lineItems.length,
            deduplicatedCount: deduplicated.length,
            removedDuplicates: lineItems.length - deduplicated.length
        });

        return deduplicated;
    }

    function groupLineItemsByNARDA(lineItems) {
        var grouped = {};

        for (var i = 0; i < lineItems.length; i++) {
            var item = lineItems[i];
            var narda = item.nardaNumber;

            if (!grouped[narda]) {
                grouped[narda] = {
                    nardaNumber: narda,
                    lineItems: [],
                    totalAmount: 0,
                    originalBillNumbers: []
                };
            }

            grouped[narda].lineItems.push(item);

            // Parse and sum the price (remove parentheses and $ signs, convert to positive number)
            var price = parseFloat(item.totalAmount.replace(/[()$,-]/g, ''));
            if (!isNaN(price)) {
                grouped[narda].totalAmount += price;
            }

            // Collect original bill numbers
            if (item.originalBillNumber && grouped[narda].originalBillNumbers.indexOf(item.originalBillNumber) === -1) {
                grouped[narda].originalBillNumbers.push(item.originalBillNumber);
            }

            log.debug('Grouped line item', {
                narda: narda,
                price: item.totalAmount,
                parsedPrice: price,
                runningTotal: grouped[narda].totalAmount,
                originalBillNumber: item.originalBillNumber
            });
        }

        log.debug('Final NARDA grouping results', {
            totalGroups: Object.keys(grouped).length,
            groups: Object.keys(grouped).map(function (narda) {
                return {
                    narda: narda,
                    itemCount: grouped[narda].lineItems.length,
                    totalAmount: grouped[narda].totalAmount,
                    originalBillNumbers: grouped[narda].originalBillNumbers
                };
            })
        });

        return grouped;
    }

    /* OLD CODE, PRESERVE FOR HISTORIC REFERENCE function extractNARDAAndTotal
    function extractNARDAAndTotal(jsonContent, fileName, partNumber, recordId) {
        try {
            log.debug('Extracting NARDA, Total, Invoice Date, Invoice Number, and Original Bill Number from JSON', {
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });

            // Declare ALL variables at the top
            var originalBillNumber = null;
            var nardaNumber = null;
            var totalAmount = null;
            var invoiceDate = null;
            var invoiceNumber = null;
            var textElementsFound = 0;
            var deliveryAmount

            // Function to recursively search through JSON structure
            function searchJSON(obj, parent) {
                if (obj && typeof obj === 'object') {
                    // Check if this is a text object with coordinates
                    if (obj.text && obj.text['@x'] && obj.text['@y'] && obj.text['#text']) {
                        var x = parseFloat(obj.text['@x']);
                        var y = parseFloat(obj.text['@y']);
                        var text = obj.text['#text'];

                        textElementsFound++;

                        // Log every 25th element to avoid spam but get good coverage
                        if (textElementsFound % 25 === 0) {
                            log.debug('Processing text elements', {
                                count: textElementsFound,
                                currentText: text,
                                coordinates: { x: x, y: y },
                                fileName: fileName
                            });
                        }

                        // NARDA Number extraction - EXACT coordinates based on analysis
                        if (!nardaNumber) {
                            // Primary NARDA location (found in most samples)
                            if (Math.abs(x - 229.53) < 1 && Math.abs(y - 426.41) < 1) {
                                nardaNumber = text.trim();
                                log.debug('NARDA Number Found by exact coordinates (primary)', {
                                    nardaNumber: nardaNumber,
                                    coordinates: { x: x, y: y },
                                    fileName: fileName,
                                    partNumber: partNumber,
                                    recordId: recordId
                                });
                            }
                            // JSON SAMPLE 6 pattern - embedded NARDA number
                            else if (Math.abs(x - 122.54) < 1 && Math.abs(y - 426.41) < 1) {
                                // Extract NARDA number from text like "AUTOFILL W66863147 J16836"
                                var nardaMatches = text.match(/\b(J\d{4,6}|CONCDA|[A-Z]\d{4,6})\b/gi);
                                if (nardaMatches && nardaMatches.length > 0) {
                                    // Take the last match (J16836 in this case)
                                    nardaNumber = nardaMatches[nardaMatches.length - 1].trim();
                                    log.debug('NARDA Number Found by embedded pattern (JSON SAMPLE 6)', {
                                        nardaNumber: nardaNumber,
                                        originalText: text,
                                        allMatches: nardaMatches,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName,
                                        partNumber: partNumber,
                                        recordId: recordId
                                    });
                                }
                            }
                            // Fallback NARDA search in broader area around expected location
                            else if (Math.abs(x - 229.53) < 10 && Math.abs(y - 426.41) < 10) {
                                // Check if text matches NARDA patterns
                                if (text === 'CONCDA' || /^J\d{4,6}$/i.test(text) || /^[A-Z]\d{4,6}$/i.test(text)) {
                                    nardaNumber = text.trim();
                                    log.debug('NARDA Number Found by fallback coordinates', {
                                        nardaNumber: nardaNumber,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName,
                                        partNumber: partNumber,
                                        recordId: recordId
                                    });
                                }
                            }
                            // Additional broad search for embedded NARDA patterns
                            else if (x > 100 && x < 200 && y > 420 && y < 430) {
                                var embeddedNardaMatches = text.match(/\b(J\d{4,6}|CONCDA|[A-Z]\d{4,6})\b/gi);
                                if (embeddedNardaMatches && embeddedNardaMatches.length > 0) {
                                    // Take the last match which is likely the NARDA number
                                    nardaNumber = embeddedNardaMatches[embeddedNardaMatches.length - 1].trim();
                                    log.debug('NARDA Number Found by broad embedded search', {
                                        nardaNumber: nardaNumber,
                                        originalText: text,
                                        allMatches: embeddedNardaMatches,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName
                                    });
                                }
                            }
                        }

                        // Original Bill Number extraction
                        if (!originalBillNumber) {
                            // Primary location (most samples)
                            if (Math.abs(x - 122.54) < 1 && Math.abs(y - 437.91) < 1) {
                                var billMatch1 = text.match(/([A-Z]*[NW]?)(\d{8,10})/i);
                                if (billMatch1) {
                                    originalBillNumber = billMatch1[2]; // Extract just the number part
                                    log.debug('Original Bill Number Found at primary coordinates', {
                                        originalBillNumber: originalBillNumber,
                                        fullText: text,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName
                                    });
                                }
                            }
                            // Alternative location (JSON SAMPLE 4 pattern)
                            else if (Math.abs(x - 166.37) < 5 && Math.abs(y - 426.41) < 5) {
                                var billMatch2 = text.match(/([A-Z]*[NW]?)(\d{8,10})/i);
                                if (billMatch2) {
                                    originalBillNumber = billMatch2[2]; // Extract just the number part
                                    log.debug('Original Bill Number Found at alternative coordinates', {
                                        originalBillNumber: originalBillNumber,
                                        fullText: text,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName
                                    });
                                }
                            }
                            // Broader fallback search in expected area
                            else if (x > 120 && x < 180 && y > 425 && y < 440) {
                                var billMatch3 = text.match(/([A-Z]*[NW]?)(\d{8,10})/i);
                                if (billMatch3) {
                                    originalBillNumber = billMatch3[2]; // Extract just the number part
                                    log.debug('Original Bill Number Found by broad search', {
                                        originalBillNumber: originalBillNumber,
                                        fullText: text,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName
                                    });
                                }
                            }
                        }

                        // Total Amount extraction (keep existing logic but enhance)
                        if (!totalAmount) {
                            // Look for "Total:" label and associated amount
                            if (text && text.trim().toLowerCase() === 'total:' && y > 500) {
                                if (parent && parent.column && Array.isArray(parent.column)) {
                                    for (var i = 0; i < parent.column.length; i++) {
                                        var col = parent.column[i];
                                        if (col.text && col.text['#text'] && col.text['#text'] !== text) {
                                            var potentialTotal = col.text['#text'];
                                            var colX = parseFloat(col.text['@x']);
                                            if (colX > x && (potentialTotal.indexOf('$') !== -1 ||
                                                (potentialTotal.indexOf('(') !== -1 && potentialTotal.indexOf(')') !== -1))) {
                                                totalAmount = potentialTotal;
                                                log.debug('Total Amount Found by Total: label', {
                                                    totalAmount: totalAmount,
                                                    coordinates: { x: colX, y: parseFloat(col.text['@y']) },
                                                    fileName: fileName
                                                });
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            // Look for currency amounts in parentheses in bottom area
                            if (!totalAmount && text.indexOf('($') === 0 && text.indexOf(')') === text.length - 1) {
                                if (x > 500 && y > 500) {
                                    totalAmount = text;
                                    log.debug('Total Amount Found by currency pattern', {
                                        totalAmount: totalAmount,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName
                                    });
                                }
                            }
                        }

                        // Invoice Date extraction (exact coordinates from samples)
                        if (!invoiceDate && Math.abs(x - 510.90) < 1 && Math.abs(y - 87.92) < 1) {
                            invoiceDate = text;
                            log.debug('Invoice Date Found', {
                                invoiceDate: invoiceDate,
                                coordinates: { x: x, y: y },
                                fileName: fileName
                            });
                        }

                        // Invoice Number extraction (exact coordinates from samples)
                        if (!invoiceNumber && Math.abs(x - 510.90) < 1 && Math.abs(y - 102.32) < 1) {
                            invoiceNumber = text;
                            log.debug('Invoice Number Found', {
                                invoiceNumber: invoiceNumber,
                                coordinates: { x: x, y: y },
                                fileName: fileName
                            });
                        }

                        // Enhanced Delivery Amount extraction with multiple coordinate patterns
                        if (!deliveryAmount) {
                            // Look for "Delivery:" label first to confirm we're in the right area
                            if (text && text.trim().toLowerCase() === 'delivery:' &&
                                Math.abs(x - 441.42) < 1 && Math.abs(y - 594.79) < 10) { // More flexible Y coordinate

                                // Found the label, now look for the amount in the same row
                                if (parent && parent.column && Array.isArray(parent.column)) {
                                    for (var k = 0; k < parent.column.length; k++) {
                                        var col = parent.column[k];
                                        if (col.text && col.text['#text'] && col.text['#text'] !== text) {
                                            var colX = parseFloat(col.text['@x']);
                                            var colY = parseFloat(col.text['@y']);

                                            // Check multiple possible X coordinates for delivery amount
                                            var validXCoordinates = [545.17, 527.62]; // Add the new coordinate
                                            var foundValidX = false;

                                            for (var coordIdx = 0; coordIdx < validXCoordinates.length; coordIdx++) {
                                                if (Math.abs(colX - validXCoordinates[coordIdx]) < 5 && Math.abs(colY - y) < 1) {
                                                    foundValidX = true;
                                                    break;
                                                }
                                            }

                                            if (foundValidX) {
                                                deliveryAmount = col.text['#text'];
                                                log.debug('Delivery Amount Found by label search', {
                                                    deliveryAmount: deliveryAmount,
                                                    coordinates: { x: colX, y: colY },
                                                    fileName: fileName
                                                });
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            // Alternative: Direct coordinate search for delivery amount with multiple patterns
                            if (!deliveryAmount) {
                                var deliveryCoordinates = [
                                    { x: 545.17, y: 587.97 }, // Original pattern
                                    { x: 527.62, y: 594.79 }  // JSON SAMPLE 5 pattern
                                ];

                                for (var i = 0; i < deliveryCoordinates.length; i++) {
                                    var coord = deliveryCoordinates[i];
                                    if (Math.abs(x - coord.x) < 5 && Math.abs(y - coord.y) < 5) {
                                        // Validate this looks like a currency amount
                                        if (text && (text.indexOf('$') !== -1 ||
                                            (text.indexOf('(') === 0 && text.indexOf('$') !== -1 && text.indexOf(')') === text.length - 1) ||
                                            /^\d+\.\d{2}$/.test(text))) {
                                            deliveryAmount = text;
                                            log.debug('Delivery Amount Found by direct coordinates', {
                                                deliveryAmount: deliveryAmount,
                                                coordinates: { x: x, y: y },
                                                fileName: fileName,
                                                pattern: 'coordinate_' + i
                                            });
                                            break;
                                        }
                                    }
                                }
                            }

                            // Broader fallback: Look for currency amounts near "Delivery:" text
                            if (!deliveryAmount && text && (text.indexOf('($') === 0 || text.indexOf('$') !== -1)) {
                                // Check if this is in the delivery area (Y coordinate around 587-595)
                                if (y > 587 && y < 600 && x > 520 && x < 550) {
                                    deliveryAmount = text;
                                    log.debug('Delivery Amount Found by broad area search', {
                                        deliveryAmount: deliveryAmount,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName
                                    });
                                }
                            }
                        }

                    }

                    // Continue recursive search
                    for (var key in obj) {
                        if (obj.hasOwnProperty(key)) {
                            searchJSON(obj[key], parent);
                        }
                    }
                } else if (Array.isArray(obj)) {
                    for (var i = 0; i < obj.length; i++) {
                        searchJSON(obj[i], parent);
                    }
                }
            }

            // Start the recursive search
            searchJSON(jsonContent);

            // Enhanced fallback search for NARDA if not found
            if (!nardaNumber) {
                log.debug('Primary NARDA search failed, trying comprehensive pattern-based fallback', {
                    fileName: fileName,
                    partNumber: partNumber
                });

                function findNardaByPattern(obj) {
                    if (obj && typeof obj === 'object') {
                        if (obj.text && obj.text['#text']) {
                            var text = obj.text['#text'].trim();
                            var x = parseFloat(obj.text['@x']);
                            var y = parseFloat(obj.text['@y']);

                            // Look for embedded NARDA patterns in any text within the expected area
                            if (x > 100 && x < 300 && y > 400 && y < 450) {
                                // Search for NARDA patterns within the text
                                var patternMatches = text.match(/\b(CONCDA|J\d{4,6}|[A-Z]\d{4,6})\b/gi);
                                if (patternMatches && patternMatches.length > 0) {
                                    // Take the last match (most likely to be the NARDA number)
                                    nardaNumber = patternMatches[patternMatches.length - 1].trim();
                                    log.debug('NARDA Number Found by comprehensive pattern fallback', {
                                        nardaNumber: nardaNumber,
                                        originalText: text,
                                        allMatches: patternMatches,
                                        coordinates: { x: x, y: y },
                                        fileName: fileName
                                    });
                                    return true;
                                }
                            }
                        }

                        for (var key in obj) {
                            if (obj.hasOwnProperty(key)) {
                                if (findNardaByPattern(obj[key])) {
                                    return true;
                                }
                            }
                        }
                    } else if (Array.isArray(obj)) {
                        for (var i = 0; i < obj.length; i++) {
                            if (findNardaByPattern(obj[i])) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
                findNardaByPattern(jsonContent);
            }

            // Enhanced fallback for Original Bill Number
            if (!originalBillNumber) {
                log.debug('Primary Original Bill Number search failed, trying pattern-based fallback', {
                    fileName: fileName,
                    partNumber: partNumber
                });

                function findBillNumberByPattern(obj) {
                    if (obj && typeof obj === 'object') {
                        if (obj.text && obj.text['#text']) {
                            var text = obj.text['#text'];
                            var x = parseFloat(obj.text['@x']);
                            var y = parseFloat(obj.text['@y']);

                            // Look for 8+ digit numbers in expected area
                            var numberMatch = text.match(/\d{8,10}/);
                            if (numberMatch && x > 100 && x < 200 && y > 420 && y < 445) {
                                originalBillNumber = numberMatch[0];
                                log.debug('Original Bill Number Found by pattern fallback', {
                                    originalBillNumber: originalBillNumber,
                                    fullText: text,
                                    coordinates: { x: x, y: y },
                                    fileName: fileName
                                });
                                return true;
                            }
                        }

                        for (var key in obj) {
                            if (obj.hasOwnProperty(key)) {
                                if (findBillNumberByPattern(obj[key])) {
                                    return true;
                                }
                            }
                        }
                    } else if (Array.isArray(obj)) {
                        for (var i = 0; i < obj.length; i++) {
                            if (findBillNumberByPattern(obj[i])) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
                findBillNumberByPattern(jsonContent);
            }

            // Log final extraction results
            log.debug('Final Extracted Data from PDF', {
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId,
                textElementsProcessed: textElementsFound,
                nardaNumber: nardaNumber,
                totalAmount: totalAmount,
                invoiceDate: invoiceDate,
                invoiceNumber: invoiceNumber,
                originalBillNumber: originalBillNumber,
                deliveryAmount: deliveryAmount,
                extractionStatus: {
                    nardaFound: nardaNumber !== null,
                    totalFound: totalAmount !== null,
                    invoiceDateFound: invoiceDate !== null,
                    invoiceNumberFound: invoiceNumber !== null,
                    originalBillNumberFound: originalBillNumber !== null
                }
            });

            return {
                nardaNumber: nardaNumber,
                totalAmount: totalAmount,
                invoiceDate: invoiceDate,
                invoiceNumber: invoiceNumber,
                originalBillNumber: originalBillNumber,
                deliveryAmount: deliveryAmount,
                fileName: fileName,
                partNumber: partNumber
            };

        } catch (error) {
            log.error('Error extracting data from JSON', {
                error: error.toString(),
                fileName: fileName,
                partNumber: partNumber,
                recordId: recordId
            });
            return {
                nardaNumber: null,
                totalAmount: null,
                invoiceDate: null,
                invoiceNumber: null,
                originalBillNumber: null,
                error: error.toString()
            };
        }
    }
    */

    function createJournalEntriesFromLineItems(splitPart, recordId) {
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
                        extractedData: extractedData
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
                        extractedData: extractedData
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
                            existingJournalEntry: consolidatedResult.existingJournalEntry
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

                var jeResult = createJournalEntryFromNardaGroup(splitPart, recordId, extractedData, singleGroup.nardaGroup, singleGroup.nardaNumber);

                if (jeResult.success) {
                    if (jeResult.isSkipped) {
                        skippedTransactions.push(jeResult);
                    } else {
                        journalEntryResults.push(jeResult);
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
                            existingJournalEntry: jeResult.existingJournalEntry
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

                // Search for matching VRA (existing logic, unchanged)
                var vraResults = searchForMatchingVRA(billNumber, recordId);

                if (vraResults.length > 0) {
                    // Attempt to create vendor credit from VRA (existing logic, unchanged)
                    var vcResult = createVendorCreditFromVRA(
                        splitPart,
                        recordId,
                        extractedData,
                        consolidatedNardaGroup,
                        vraResults,
                        billNumber
                    );

                    if (vcResult.success) {
                        if (vcResult.isVendorCredit) {
                            vendorCreditResults.push({
                                success: true,
                                isVendorCredit: true,
                                vendorCreditId: vcResult.vendorCreditId,
                                vendorCreditTranid: vcResult.vendorCreditTranid,
                                nardaNumber: consolidatedNardaGroup.nardaNumber,
                                nardaTypes: billGroup.nardaTypes,
                                totalAmount: vcResult.totalAmount,
                                matchedLineCount: vcResult.matchedLineCount,
                                originalBillNumber: billNumber,
                                matchingVRA: vcResult.matchingVRA,
                                extractedData: extractedData,
                                attachmentResult: vcResult.attachmentResult
                            });

                            log.debug('Vendor credit created successfully', {
                                vendorCreditId: vcResult.vendorCreditId,
                                billNumber: billNumber,
                                nardaTypes: billGroup.nardaTypes,
                                combinedNarda: consolidatedNardaGroup.nardaNumber,
                                fileName: splitPart.fileName
                            });
                        } else if (vcResult.isSkipped) {
                            skippedTransactions.push({
                                success: true,
                                isSkipped: true,
                                skipReason: vcResult.skipReason,
                                skipType: 'NO_VRA_MATCH',
                                nardaNumber: consolidatedNardaGroup.nardaNumber,
                                nardaTypes: billGroup.nardaTypes,
                                totalAmount: billGroup.totalAmount,
                                originalBillNumber: billNumber,
                                extractedData: extractedData,
                                matchingVRA: vcResult.matchingVRA
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
                                existingVendorCredit: vcResult.existingVendorCredit
                            });
                        } else {
                            return {
                                success: false,
                                error: vcResult.error
                            };
                        }
                    }
                } else {
                    log.debug('No VRA found for bill number', {
                        billNumber: billNumber,
                        nardaTypes: billGroup.nardaTypes,
                        fileName: splitPart.fileName,
                        recordId: recordId
                    });

                    skippedTransactions.push({
                        success: true,
                        isSkipped: true,
                        skipReason: 'No VRA found with matching bill number: ' + billNumber,
                        skipType: 'NO_VRA_MATCH',
                        nardaNumber: consolidatedNardaGroup.nardaNumber,
                        nardaTypes: billGroup.nardaTypes,
                        totalAmount: billGroup.totalAmount,
                        originalBillNumber: billNumber,
                        extractedData: extractedData
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
                    existingEntry: existingEntries[0], // Return the first one found
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

            // Create main memo based on actual scenario
            var mainMemo;
            if (nardaNumbers.length > 1) {
                // Multiple different NARDA values = Multi-NARDA Groups
                mainMemo = 'MARCONE CM' + extractedData.invoiceNumber + ' Multi-NARDA Groups';
            } else {
                // Single NARDA value but possibly multiple lines = check for consolidation
                var singleNarda = nardaNumbers[0];
                var group = journalEntryGroups[0];
                if (group.nardaGroup.lineItems && group.nardaGroup.lineItems.length > 1) {
                    // Multiple lines with same NARDA = Consolidated NARDA
                    mainMemo = 'MARCONE CM' + extractedData.invoiceNumber + ' Consolidated ' + singleNarda;
                } else {
                    // Single line with single NARDA = Regular NARDA
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
                    // Check if this is a "no matching invoice" case that should be skipped
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
                            extractedData: extractedData
                        };
                    } else {
                        // Other errors should still fail
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

            // Attach the PDF file to the journal entry
            var attachResult = attachFileToRecord(jeId, splitPart.fileId, recordId);

            return {
                success: true,
                journalEntryId: jeId,
                tranid: tranid,
                attachmentResult: attachResult,
                nardaGroups: nardaNumbers,
                grandTotal: grandTotal
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

    function createJournalEntryFromNardaGroup(splitPart, recordId, extractedData, nardaGroup, nardaNumber) {
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

            // Find Credit Line Entity based on NARDA number
            var creditLineEntity = findCreditLineEntity(nardaNumber, recordId);
            if (!creditLineEntity.success) {
                // Check if this is a "no matching invoice" case that should be skipped
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
                        extractedData: extractedData
                    };
                } else {
                    // Other errors should still fail
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

            return {
                success: true,
                journalEntryId: jeId,
                tranid: tranid,
                nardaGroups: [nardaNumber],
                totalAmount: nardaGroup.totalAmount,
                attachmentResult: attachResult
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

    function processVendorCreditGroup(splitPart, recordId, extractedData, nardaNumber, nardaGroup) {
        try {
            log.debug('Processing vendor credit group', {
                nardaNumber: nardaNumber,
                originalBillNumbers: nardaGroup.originalBillNumbers,
                fileName: splitPart.fileName,
                recordId: recordId
            });


            var vendorCreditTranid = extractedData.invoiceNumber;

            // Check for duplicate vendor credit
            var duplicateVCCheck = checkForDuplicateVendorCredit(vendorCreditTranid, recordId);
            if (!duplicateVCCheck.success) {
                return {
                    success: true,  // Changed to true since this is a "skip" not a failure
                    isSkipped: true,
                    skipReason: 'Duplicate vendor credit already exists (Existing VC ID: ' + (duplicateVCCheck.existingEntry ? duplicateVCCheck.existingEntry.internalId : 'Unknown') + ')',
                    skipType: 'DUPLICATE_VENDOR_CREDIT',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    isDuplicate: true,
                    existingVendorCredit: duplicateVCCheck.existingEntry
                };
            }

            // Handle specific NARDA values that should be skipped
            if (nardaNumber.toUpperCase() === 'SHORT') {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'SHORT NARDA - Process as a Vendor Receivables Short Ship Credit manually',
                    nardaNumber: nardaNumber,
                    extractedData: extractedData,
                    skipType: 'SHORT_SHIP'
                };
            }

            // Handle NF NARDA - should create vendor credit via VRA lookup
            if (nardaNumber.toUpperCase() === 'NF') {
                log.debug('NF NARDA detected - processing as vendor credit via VRA lookup', {
                    nardaNumber: nardaNumber,
                    originalBillNumbers: nardaGroup.originalBillNumbers,
                    fileName: splitPart.fileName,
                    recordId: recordId
                });
                // Continue with normal VRA processing for NF
            }

            // Handle CONCDA NARDA - should create vendor credit via VRA lookup
            if (nardaNumber.toUpperCase() === 'CONCDA') {
                log.debug('CONCDA NARDA detected - processing as vendor credit via VRA lookup', {
                    nardaNumber: nardaNumber,
                    originalBillNumbers: nardaGroup.originalBillNumbers,
                    fileName: splitPart.fileName,
                    recordId: recordId
                });
                // Continue with normal VRA processing for CONCDA
            }

            // Check for unidentified NARDA patterns (catch-all)
            var knownPatterns = [
                /^J\d+$/i,           // J followed by numbers (journal entries)
                /^INV\d+$/i,         // INV followed by numbers (journal entries)
                /^CONCDA$/i,         // CONCDA (vendor credit)
                /^SHORT$/i,          // SHORT (skip)
                /^NF$/i              // NF (vendor credit)
            ];

            var isKnownPattern = false;
            for (var i = 0; i < knownPatterns.length; i++) {
                if (knownPatterns[i].test(nardaNumber)) {
                    isKnownPattern = true;
                    break;
                }
            }

            if (!isKnownPattern) {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'Unidentified NARDA value - manual review required: ' + nardaNumber,
                    nardaNumber: nardaNumber,
                    extractedData: extractedData,
                    skipType: 'UNIDENTIFIED_NARDA'
                };
            }

            // For CONCDA and NF, check if we have original bill numbers for VRA search
            if ((nardaNumber.toUpperCase() === 'CONCDA' || nardaNumber.toUpperCase() === 'NF') &&
                (!nardaGroup.originalBillNumbers || nardaGroup.originalBillNumbers.length === 0)) {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'No original bill numbers found for ' + nardaNumber + ' vendor credit processing',
                    nardaNumber: nardaNumber,
                    extractedData: extractedData,
                    skipType: 'MISSING_BILL_NUMBERS'
                };
            }

            // Search for VRA using original bill numbers (for CONCDA and NF)
            if (nardaNumber.toUpperCase() === 'CONCDA' || nardaNumber.toUpperCase() === 'NF') {
                for (var i = 0; i < nardaGroup.originalBillNumbers.length; i++) {
                    var originalBillNumber = nardaGroup.originalBillNumbers[i];

                    var vraSearch = search.create({
                        type: search.Type.VENDOR_RETURN_AUTHORIZATION,
                        filters: [
                            ['type', 'anyof', 'VendAuth'],
                            'AND',
                            ['memo', 'contains', originalBillNumber]
                        ],
                        columns: [
                            'tranid',
                            'trandate',
                            'memo',
                            'internalid',
                            'entity',
                            'status',
                            'item',
                            'amount',
                            'line'
                        ]
                    });

                    var vraResults = [];
                    vraSearch.run().each(function (result) {
                        var lineMemo = result.getValue('memo');
                        if (lineMemo && lineMemo.indexOf(originalBillNumber) !== -1) {
                            vraResults.push({
                                internalId: result.getValue('internalid'),
                                tranid: result.getValue('tranid'),
                                trandate: result.getValue('trandate'),
                                memo: result.getValue('memo'),
                                entity: result.getValue('entity'),
                                status: result.getValue('status'),
                                lineItem: result.getValue('item'),
                                amount: result.getValue('amount'),
                                lineNumber: result.getValue('line')
                            });
                        }
                        return true;
                    });

                    if (vraResults.length > 0) {
                        // Found matching VRA - attempt vendor credit creation
                        var vcResult = createVendorCreditFromVRA(splitPart, recordId, extractedData, nardaGroup, vraResults, originalBillNumber);
                        return vcResult;
                    }
                }

                // No matching VRA found
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'No matching VRA found for ' + nardaNumber + ' with original bill numbers: ' + nardaGroup.originalBillNumbers.join(', '),
                    nardaNumber: nardaNumber,
                    extractedData: extractedData,
                    skipType: 'NO_VRA_MATCH'
                };
            }

            // This shouldn't be reached for vendor credit groups, but just in case
            return {
                success: true,
                isSkipped: true,
                skipReason: 'Unexpected vendor credit group processing path',
                nardaNumber: nardaNumber,
                extractedData: extractedData,
                skipType: 'UNEXPECTED_PATH'
            };

        } catch (error) {
            log.error('Error processing vendor credit group', {
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

    /* OLD CODE, PRESERVED FOR HISTORIC REFERENCE function createVendorCreditFromVRA_OLD
    function createVendorCreditFromVRA_OLD(splitPart, recordId, extractedData, nardaGroup, vraResults, originalBillNumber) {
        try {
            // Parse the total amount for comparison
            var totalAmount = nardaGroup.totalAmount;

            // Find matching VRA lines by amount
            var matchingVRALines = [];
            for (var i = 0; i < vraResults.length; i++) {
                var vraLine = vraResults[i];
                var lineAmountAbs = Math.abs(parseFloat(vraLine.amount));

                // Match on absolute values within tolerance
                if (Math.abs(lineAmountAbs - totalAmount) < 0.01) {
                    matchingVRALines.push(vraLine);
                }
            }

            if (matchingVRALines.length === 0) {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'No VRA lines found with matching amounts',
                    nardaNumber: nardaGroup.nardaNumber,
                    originalBillNumber: originalBillNumber,
                    extractedData: extractedData
                };
            }

            // Use the first matching VRA line
            var matchingVRALine = matchingVRALines[0];

            // Transform VRA to Vendor Credit
            var vendorCreditResult = transformVRAToVendorCredit(
                matchingVRALine.internalId,
                {
                    invoiceNumber: extractedData.invoiceNumber,
                    invoiceDate: extractedData.invoiceDate,
                    totalAmount: '($' + totalAmount.toFixed(2) + ')',
                    originalBillNumber: originalBillNumber,
                    deliveryAmount: extractedData.deliveryAmount
                },
                splitPart.fileId,
                recordId,
                matchingVRALine.lineNumber
            );

            if (vendorCreditResult.success) {
                return {
                    success: true,
                    isVendorCredit: true,
                    vendorCreditId: vendorCreditResult.vendorCreditId,
                    vendorCreditTranid: vendorCreditResult.vendorCreditTranid,
                    matchingVRA: {
                        internalId: matchingVRALine.internalId,
                        tranid: matchingVRALine.tranid,
                        memo: matchingVRALine.memo,
                        entity: matchingVRALine.entity,
                        status: matchingVRALine.status
                    },
                    extractedData: extractedData,
                    attachmentResult: vendorCreditResult.attachmentResult
                };
            } else {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'Vendor Credit creation failed: ' + vendorCreditResult.error,
                    nardaNumber: nardaGroup.nardaNumber,
                    originalBillNumber: originalBillNumber,
                    matchingVRA: {
                        internalId: matchingVRALine.internalId,
                        tranid: matchingVRALine.tranid,
                        memo: matchingVRALine.memo,
                        entity: matchingVRALine.entity,
                        status: matchingVRALine.status
                    },
                    extractedData: extractedData,
                    vendorCreditError: vendorCreditResult.error
                };
            }

        } catch (error) {
            log.error('Error creating vendor credit from VRA', {
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
    */

    function createVendorCreditFromVRA(splitPart, recordId, extractedData, nardaGroup, vraResults, originalBillNumber) {
        try {
            log.debug('Creating Vendor Credit from VRA for CONCDA/NF/CORE - Processing by Original Bill Number', {
                nardaNumber: nardaGroup.nardaNumber,
                originalBillNumber: originalBillNumber,
                vraResults: vraResults.length,
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
                    extractedData: extractedData
                };
            }

            // Filter VRA results to only include lines that contain this original bill number
            var matchingVRALines = vraResults.filter(function (vraLine) {
                return vraLine.memo && vraLine.memo.indexOf(originalBillNumber) !== -1;
            });

            log.debug('Filtered VRA lines for original bill number', {
                originalBillNumber: originalBillNumber,
                totalVRALines: vraResults.length,
                matchingVRALines: matchingVRALines.length,
                matchingLineNumbers: matchingVRALines.map(function (line) { return line.lineNumber; })
            });

            if (matchingVRALines.length === 0) {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: nardaGroup.nardaNumber + ' NARDA - no VRA lines found containing bill number: ' + originalBillNumber,
                    skipType: 'NO_VRA_MATCH',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData
                };
            }

            // NEW: Group VRA lines by their parent VRA internal ID
            var vraGroups = {};
            for (var i = 0; i < matchingVRALines.length; i++) {
                var vraLine = matchingVRALines[i];
                var vraId = vraLine.internalId;

                if (!vraGroups[vraId]) {
                    vraGroups[vraId] = [];
                }
                vraGroups[vraId].push(vraLine);
            }

            var vraIds = Object.keys(vraGroups);
            log.debug('VRA lines grouped by parent VRA', {
                originalBillNumber: originalBillNumber,
                totalVRAs: vraIds.length,
                vraIds: vraIds,
                linesPerVRA: vraIds.map(function (id) { return vraGroups[id].length; })
            });

            // NEW: Try each VRA until we find one that works
            var lastError = null;
            for (var vraIndex = 0; vraIndex < vraIds.length; vraIndex++) {
                var vraId = vraIds[vraIndex];
                var vraLinesForThisVRA = vraGroups[vraId];

                log.debug('Attempting VRA transformation', {
                    attemptNumber: vraIndex + 1,
                    totalAttempts: vraIds.length,
                    vraId: vraId,
                    linesInThisVRA: vraLinesForThisVRA.length,
                    originalBillNumber: originalBillNumber
                });

                // Attempt to match PDF line items to VRA lines by amount for this specific VRA
                var matchedPairs = matchPDFLinesToVRALines(billGroup, vraLinesForThisVRA, originalBillNumber);

                if (matchedPairs.length === 0) {
                    log.debug('No amount matches found for this VRA, trying next VRA', {
                        vraId: vraId,
                        attemptNumber: vraIndex + 1,
                        originalBillNumber: originalBillNumber,
                        pdfLineAmounts: billGroup.map(function (item) { return Math.abs(parseFloat(item.totalAmount.replace(/[()$,-]/g, ''))); }),
                        vraLineAmounts: vraLinesForThisVRA.map(function (line) { return Math.abs(parseFloat(line.amount)); })
                    });
                    continue; // Try next VRA
                }

                // Attempt to create vendor credit for this VRA
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
                        vraId: vraId,
                        attemptNumber: vraIndex + 1,
                        totalAttempts: vraIds.length,
                        vendorCreditId: vcResult.vendorCreditId,
                        originalBillNumber: originalBillNumber
                    });
                    return vcResult;
                } else if (vcResult.isSkipped) {
                    log.debug('VRA transformation skipped, trying next VRA', {
                        vraId: vraId,
                        attemptNumber: vraIndex + 1,
                        totalAttempts: vraIds.length,
                        skipReason: vcResult.skipReason,
                        skipType: vcResult.skipType,
                        originalBillNumber: originalBillNumber
                    });
                    lastError = vcResult;
                    continue; // Try next VRA
                } else {
                    log.error('VRA transformation failed, trying next VRA', {
                        vraId: vraId,
                        attemptNumber: vraIndex + 1,
                        totalAttempts: vraIds.length,
                        error: vcResult.error,
                        originalBillNumber: originalBillNumber
                    });
                    lastError = vcResult;
                    continue; // Try next VRA
                }
            }

            // If we get here, all VRAs failed or were skipped
            log.error('All VRA transformation attempts failed or were skipped', {
                originalBillNumber: originalBillNumber,
                totalVRAsAttempted: vraIds.length,
                nardaNumber: nardaGroup.nardaNumber,
                lastError: lastError
            });

            // Return the last error/skip result, or a generic failure
            if (lastError) {
                return lastError;
            } else {
                return {
                    success: true,
                    isSkipped: true,
                    skipReason: nardaGroup.nardaNumber + ' NARDA - all ' + vraIds.length + ' VRAs with bill number ' + originalBillNumber + ' failed transformation attempts',
                    skipType: 'ALL_VRA_ATTEMPTS_FAILED',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    vraAttemptsCount: vraIds.length
                };
            }

        } catch (error) {
            log.error('Error creating vendor credit from VRA', {
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

    /**
 * Consolidates vendor credit groups by original bill number
 * This ensures NF and CORE (and other VC types) with the same bill number are processed together
 * @param {Object} groupedLineItems - Line items grouped by NARDA number
 * @returns {Object} Groups consolidated by original bill number with all NARDA types combined
 */
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
                billNumbers: Object.keys(billNumberGroups),
                details: Object.keys(billNumberGroups).map(function (billNum) {
                    var group = billNumberGroups[billNum];
                    return {
                        billNumber: billNum,
                        nardaTypes: group.nardaTypes,
                        lineCount: group.lineItems.length,
                        totalAmount: group.totalAmount
                    };
                })
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

    function matchPDFLinesToVRALines(pdfLines, vraLines, originalBillNumber) {
        var matchedPairs = [];
        var usedVRALines = [];

        log.debug('Attempting to match PDF lines to VRA lines', {
            originalBillNumber: originalBillNumber,
            pdfLineCount: pdfLines.length,
            vraLineCount: vraLines.length
        });

        // Try to match each PDF line to a VRA line by amount
        for (var i = 0; i < pdfLines.length; i++) {
            var pdfLine = pdfLines[i];
            var pdfAmount = Math.abs(parseFloat(pdfLine.totalAmount.replace(/[()$,-]/g, '')));

            // Find matching VRA line that hasn't been used
            for (var j = 0; j < vraLines.length; j++) {
                var vraLine = vraLines[j];

                // Skip if this VRA line is already used
                if (usedVRALines.indexOf(vraLine.lineNumber) !== -1) {
                    continue;
                }

                var vraAmount = Math.abs(parseFloat(vraLine.amount));

                // Check if amounts match within tolerance
                if (Math.abs(vraAmount - pdfAmount) < 0.01) {
                    matchedPairs.push({
                        pdfLine: pdfLine,
                        vraLine: vraLine,
                        amount: pdfAmount
                    });

                    usedVRALines.push(vraLine.lineNumber);

                    log.debug('Matched PDF line to VRA line', {
                        pdfAmount: pdfAmount,
                        vraAmount: vraAmount,
                        vraLineNumber: vraLine.lineNumber,
                        originalBillNumber: originalBillNumber
                    });

                    break; // Move to next PDF line
                }
            }
        }

        log.debug('Line matching complete', {
            originalBillNumber: originalBillNumber,
            totalMatches: matchedPairs.length,
            unmatchedPDFLines: pdfLines.length - matchedPairs.length,
            matchedVRALineNumbers: matchedPairs.map(function (pair) { return pair.vraLine.lineNumber; })
        });

        return matchedPairs;
    }

    function createGroupedVendorCredit(splitPart, recordId, extractedData, nardaGroup, matchedPairs, originalBillNumber) {
        try {
            log.debug('Creating grouped vendor credit for original bill number', {
                originalBillNumber: originalBillNumber,
                nardaNumber: nardaGroup.nardaNumber,
                matchedPairsCount: matchedPairs.length,
                vraInternalId: matchedPairs[0].vraLine.internalId, // All should be from same VRA
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

            // Use the VRA from the first matched pair (all should be from same VRA)
            var vraInternalId = matchedPairs[0].vraLine.internalId;

            // SIMPLIFIED: Basic VRA validation without quantity checking
            var vraRecord;
            try {
                vraRecord = record.load({
                    type: record.Type.VENDOR_RETURN_AUTHORIZATION,
                    id: vraInternalId,
                    isDynamic: false
                });

                // Check VRA status 
                var vraStatus = vraRecord.getValue('status');
                var vraTransactionId = vraRecord.getValue('tranid');
                var statusText = vraRecord.getText('status') || '';

                log.debug('VRA basic validation check', {
                    vraInternalId: vraInternalId,
                    vraTransactionId: vraTransactionId,
                    vraStatus: vraStatus,
                    statusText: statusText,
                    originalBillNumber: originalBillNumber
                });

                // Check if VRA is in a valid status for transformation
                var invalidStatuses = ['Closed', 'Rejected', 'Cancelled'];
                var isInvalidStatus = false;

                for (var i = 0; i < invalidStatuses.length; i++) {
                    if (statusText.indexOf(invalidStatuses[i]) !== -1) {
                        isInvalidStatus = true;
                        break;
                    }
                }

                if (isInvalidStatus) {
                    log.debug('VRA cannot be transformed - invalid status', {
                        vraInternalId: vraInternalId,
                        vraTransactionId: vraTransactionId,
                        vraStatus: vraStatus,
                        statusText: statusText,
                        originalBillNumber: originalBillNumber
                    });

                    return {
                        success: true,
                        isSkipped: true,
                        skipReason: nardaGroup.nardaNumber + ' NARDA - VRA ' + vraTransactionId + ' cannot be credited (Status: ' + statusText + ')',
                        skipType: 'VRA_INVALID_STATUS',
                        nardaNumber: nardaGroup.nardaNumber,
                        extractedData: extractedData,
                        matchingVRA: {
                            internalId: vraInternalId,
                            tranid: vraTransactionId,
                            status: statusText
                        }
                    };
                }

                log.debug('VRA basic validation passed - attempting transformation', {
                    vraInternalId: vraInternalId,
                    vraTransactionId: vraTransactionId,
                    statusText: statusText,
                    originalBillNumber: originalBillNumber
                });

            } catch (vraLoadError) {
                log.error('Cannot load VRA record for validation', {
                    error: vraLoadError.toString(),
                    vraInternalId: vraInternalId,
                    originalBillNumber: originalBillNumber,
                    nardaNumber: nardaGroup.nardaNumber
                });

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: nardaGroup.nardaNumber + ' NARDA - Cannot access VRA ' + vraInternalId + ' (may be deleted or restricted)',
                    skipType: 'VRA_ACCESS_ERROR',
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    vraAccessError: vraLoadError.toString()
                };
            }

            // SIMPLIFIED: Test transformation without quantity validation
            var vendorCredit;
            try {
                // Transform VRA to Vendor Credit
                vendorCredit = record.transform({
                    fromType: record.Type.VENDOR_RETURN_AUTHORIZATION,
                    fromId: vraInternalId,
                    toType: record.Type.VENDOR_CREDIT,
                    isDynamic: true
                });

                log.debug('VRA transformation successful', {
                    vraInternalId: vraInternalId,
                    vraTransactionId: vraTransactionId,
                    originalBillNumber: originalBillNumber
                });

            } catch (transformError) {
                log.error('VRA transformation failed', {
                    error: transformError.toString(),
                    errorName: transformError.name,
                    errorCode: transformError.code,
                    vraInternalId: vraInternalId,
                    vraTransactionId: vraTransactionId,
                    originalBillNumber: originalBillNumber,
                    nardaNumber: nardaGroup.nardaNumber
                });

                // Provide specific error handling based on error type
                var skipReason;
                var skipType;

                if (transformError.name === 'INVALID_INITIALIZE_REF' ||
                    transformError.message.indexOf('invalid reference') !== -1) {
                    skipReason = nardaGroup.nardaNumber + ' NARDA - VRA ' + vraTransactionId + ' cannot be transformed (fully credited or invalid state)';
                    skipType = 'VRA_FULLY_CREDITED';
                } else if (transformError.name === 'INSUFFICIENT_PERMISSION') {
                    skipReason = nardaGroup.nardaNumber + ' NARDA - Insufficient permissions to transform VRA ' + vraTransactionId;
                    skipType = 'VRA_PERMISSION_ERROR';
                } else {
                    skipReason = nardaGroup.nardaNumber + ' NARDA - VRA ' + vraTransactionId + ' transformation failed: ' + transformError.message;
                    skipType = 'VRA_TRANSFORM_ERROR';
                }

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: skipReason,
                    skipType: skipType,
                    nardaNumber: nardaGroup.nardaNumber,
                    extractedData: extractedData,
                    matchingVRA: {
                        internalId: vraInternalId,
                        tranid: vraTransactionId,
                        status: vraStatus
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

            // Set memo to include reference information
            var nardaTypesList = nardaGroup.allNardaTypes ? nardaGroup.allNardaTypes.join('+') : nardaGroup.nardaNumber.toUpperCase();
            var vcMemo = nardaTypesList + ' Credit - ' + extractedData.invoiceNumber + ' - Bill: ' + originalBillNumber + ' - VRA: ' + vraRecord.getValue('tranid');

            vendorCredit.setValue({
                fieldId: 'memo',
                value: vcMemo
            });

            // SIMPLIFIED: Remove all lines except the matched ones (without quantity validation)
            var vcLineCount = vendorCredit.getLineCount({ sublistId: 'item' });
            var targetLineNumbers = matchedPairs.map(function (pair) { return pair.vraLine.lineNumber; });

            log.debug('Filtering Vendor Credit lines to matched VRA lines only', {
                totalVCLines: vcLineCount,
                targetLineNumbers: targetLineNumbers,
                vraInternalId: vraInternalId,
                originalBillNumber: originalBillNumber
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

                    log.debug('Removed non-matching VRA line from Vendor Credit', {
                        removedLineIndex: j,
                        removedLineKey: currentLineKey,
                        keepingLineKeys: targetLineNumbers
                    });
                } else {
                    log.debug('Keeping matching VRA line in Vendor Credit', {
                        keptLineIndex: j,
                        keptLineKey: currentLineKey,
                        targetLineKeys: targetLineNumbers
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
                            department: CONFIG.ENTITIES.SERVICE_DEPARTMENT,
                            originalBillNumber: originalBillNumber
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
                vraInternalId: vraInternalId,
                originalBillNumber: originalBillNumber,
                matchedLineCount: matchedPairs.length,
                targetLineNumbers: targetLineNumbers,
                memo: vcMemo,
                nardaNumber: nardaGroup.nardaNumber,
                recordId: recordId
            });

            // Attach the PDF file to the vendor credit
            var attachResult = attachFileToRecord(vendorCreditId, splitPart.fileId, recordId, record.Type.VENDOR_CREDIT);

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
                matchingVRA: {
                    internalId: vraInternalId,
                    tranid: vraRecord.getValue('tranid'),
                    entity: vraRecord.getValue('entity'),
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

    function createIndividualVendorCredit(splitPart, recordId, extractedData, nardaGroup, matchingVRALine, lineItem, lineIndex) {
        try {
            log.debug('Creating individual vendor credit from matching VRA line', {
                nardaNumber: nardaGroup.nardaNumber,
                lineIndex: lineIndex,
                lineItemAmount: lineItem.totalAmount,
                vraInternalId: matchingVRALine.internalId,
                vraLineNumber: matchingVRALine.lineNumber,
                vraAmount: matchingVRALine.amount,
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

            // Create vendor credit tranid - include line index for uniqueness if multiple line items
            var vendorCreditTranid;
            if (nardaGroup.lineItems.length > 1) {
                vendorCreditTranid = extractedData.invoiceNumber + '-L' + (lineIndex + 1);
            } else {
                vendorCreditTranid = extractedData.invoiceNumber;
            }

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

            // Load the VRA to get transaction details
            var vraRecord = record.load({
                type: record.Type.VENDOR_RETURN_AUTHORIZATION,
                id: matchingVRALine.internalId,
                isDynamic: false
            });

            // Transform VRA to Vendor Credit
            var vendorCredit = record.transform({
                fromType: record.Type.VENDOR_RETURN_AUTHORIZATION,
                fromId: matchingVRALine.internalId,
                toType: record.Type.VENDOR_CREDIT,
                isDynamic: true
            });

            // Set header fields
            vendorCredit.setValue({
                fieldId: 'tranid',
                value: vendorCreditTranid
            });

            vendorCredit.setValue({
                fieldId: 'trandate',
                value: vcDate
            });

            // Set memo to include reference information
            var vcMemo = nardaGroup.nardaNumber.toUpperCase() + ' Credit - ' + extractedData.invoiceNumber;
            if (nardaGroup.lineItems.length > 1) {
                vcMemo += ' (Line ' + (lineIndex + 1) + ')';
            }
            vcMemo += ' - VRA: ' + vraRecord.getValue('tranid');

            vendorCredit.setValue({
                fieldId: 'memo',
                value: vcMemo
            });

            // Remove all lines except the matching one
            var vcLineCount = vendorCredit.getLineCount({ sublistId: 'item' });

            log.debug('Filtering Vendor Credit lines to match specific VRA line', {
                totalVCLines: vcLineCount,
                targetVRALineNumber: matchingVRALine.lineNumber,
                vraInternalId: matchingVRALine.internalId,
                lineIndex: lineIndex
            });

            // Remove lines in reverse order to avoid index shifting issues
            for (var j = vcLineCount - 1; j >= 0; j--) {
                var currentLineKey = vendorCredit.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'line',
                    line: j
                });

                // If this is not our target line, remove it
                if (currentLineKey != matchingVRALine.lineNumber) {
                    vendorCredit.removeLine({
                        sublistId: 'item',
                        line: j
                    });

                    log.debug('Removed VRA line from Vendor Credit', {
                        removedLineIndex: j,
                        removedLineKey: currentLineKey,
                        keepingLineKey: matchingVRALine.lineNumber
                    });
                } else {
                    log.debug('Keeping matching VRA line in Vendor Credit', {
                        keptLineIndex: j,
                        keptLineKey: currentLineKey,
                        targetLineKey: matchingVRALine.lineNumber
                    });
                }
            }

            // Add delivery amount as expense line if it exists and is greater than $0.00
            // Only add to the first line item to avoid duplicating delivery charges
            if (lineIndex === 0 && extractedData.deliveryAmount && extractedData.deliveryAmount !== '$0.00') {
                try {
                    // Parse delivery amount - remove $ and convert to positive number
                    var deliveryAmountValue = parseFloat(extractedData.deliveryAmount.replace(/[$(),]/g, ''));

                    if (!isNaN(deliveryAmountValue) && deliveryAmountValue > 0) {
                        log.debug('Adding delivery amount as expense line (first line item only)', {
                            deliveryAmount: extractedData.deliveryAmount,
                            parsedAmount: deliveryAmountValue,
                            account: CONFIG.ACCOUNTS.FREIGHT_IN,
                            department: CONFIG.ENTITIES.SERVICE_DEPARTMENT,
                            lineIndex: lineIndex,
                            vraInternalId: matchingVRALine.internalId
                        });

                        // Add expense line for delivery
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

                        log.debug('Delivery expense line added successfully', {
                            account: CONFIG.ACCOUNTS.FREIGHT_IN,
                            amount: deliveryAmountValue,
                            department: CONFIG.ENTITIES.SERVICE_DEPARTMENT,
                            memo: 'Delivery - ' + extractedData.invoiceNumber,
                            vraInternalId: matchingVRALine.internalId
                        });
                    } else {
                        log.debug('Delivery amount is zero or invalid, skipping expense line', {
                            deliveryAmount: extractedData.deliveryAmount,
                            parsedAmount: deliveryAmountValue,
                            vraInternalId: matchingVRALine.internalId
                        });
                    }
                } catch (deliveryError) {
                    log.error('Error adding delivery expense line (continuing with vendor credit creation)', {
                        error: deliveryError.toString(),
                        deliveryAmount: extractedData.deliveryAmount,
                        vraInternalId: matchingVRALine.internalId
                    });
                    // Continue with vendor credit creation even if delivery line fails
                }
            } else if (lineIndex > 0) {
                log.debug('Skipping delivery amount for subsequent line items to avoid duplication', {
                    lineIndex: lineIndex,
                    deliveryAmount: extractedData.deliveryAmount,
                    vraInternalId: matchingVRALine.internalId
                });
            }

            // Save the vendor credit
            var vendorCreditId = vendorCredit.save();

            log.debug('Individual Vendor Credit created successfully from VRA', {
                vendorCreditId: vendorCreditId,
                vendorCreditTranid: vendorCreditTranid,
                vraInternalId: matchingVRALine.internalId,
                vraTransactionId: matchingVRALine.tranid,
                targetLineNumber: matchingVRALine.lineNumber,
                memo: vcMemo,
                lineIndex: lineIndex,
                lineItemAmount: lineItem.totalAmount,
                deliveryAmountAdded: lineIndex === 0 && extractedData.deliveryAmount && extractedData.deliveryAmount !== '$0.00',
                nardaNumber: nardaGroup.nardaNumber,
                recordId: recordId
            });

            // Attach the PDF file to the vendor credit
            var attachResult = attachFileToRecord(vendorCreditId, splitPart.fileId, recordId, record.Type.VENDOR_CREDIT);

            return {
                success: true,
                isVendorCredit: true,
                vendorCreditId: vendorCreditId,
                vendorCreditTranid: vendorCreditTranid,
                nardaNumber: nardaGroup.nardaNumber,
                totalAmount: Math.abs(parseFloat(lineItem.totalAmount.replace(/[()$,-]/g, ''))),
                lineIndex: lineIndex,
                matchingVRA: {
                    internalId: matchingVRALine.internalId,
                    tranid: matchingVRALine.tranid,
                    memo: matchingVRALine.memo,
                    entity: matchingVRALine.entity,
                    status: matchingVRALine.status,
                    lineNumber: matchingVRALine.lineNumber,
                    amount: matchingVRALine.amount
                },
                extractedData: extractedData,
                attachmentResult: attachResult,
                deliveryAmountProcessed: lineIndex === 0 && extractedData.deliveryAmount && extractedData.deliveryAmount !== '$0.00'
            };

        } catch (error) {
            log.error('Error creating individual Vendor Credit from VRA', {
                error: error.toString(),
                nardaNumber: nardaGroup.nardaNumber,
                lineIndex: lineIndex,
                vraInternalId: matchingVRALine.internalId,
                fileName: splitPart.fileName,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    /* OLD CODE, PRESERVED FOR HISTORIC REFERENCE function createJournalEntry
    function createJournalEntry(splitPart, recordId) {
        try {
            // Get extracted data from JSON results
            var extractedData = null;
            if (splitPart.jsonResult && splitPart.jsonResult.success && splitPart.jsonResult.extractedData) {
                extractedData = splitPart.jsonResult.extractedData;
            }

            // Validate we have required data
            if (!extractedData || !extractedData.invoiceNumber || !extractedData.invoiceDate || !extractedData.totalAmount || !extractedData.nardaNumber) {
                log.error('Missing required data for journal entry from JSON results', {
                    extractedData: extractedData,
                    fileName: splitPart.fileName,
                    partNumber: splitPart.partNumber,
                    recordId: recordId,
                    jsonResult: splitPart.jsonResult
                });
                return { success: false, error: 'Missing required extracted data from JSON results' };
            }

            // Check if NARDA number is "CONCDA" and handle VRA search
            if (extractedData.nardaNumber && extractedData.nardaNumber.toUpperCase() === 'CONCDA') {
                log.debug('CONCDA NARDA detected - searching for Vendor Return Authorization', {
                    nardaNumber: extractedData.nardaNumber,
                    originalBillNumber: extractedData.originalBillNumber,
                    fileName: splitPart.fileName,
                    partNumber: splitPart.partNumber,
                    recordId: recordId
                });

                // Check if we have an original bill number to search with
                if (!extractedData.originalBillNumber) {
                    log.error('CONCDA NARDA found but no Original Bill Number available for VRA search', {
                        nardaNumber: extractedData.nardaNumber,
                        fileName: splitPart.fileName,
                        partNumber: splitPart.partNumber,
                        recordId: recordId
                    });
                    return {
                        success: false,
                        error: 'CONCDA NARDA requires Original Bill Number for VRA search but none was extracted',
                        isSkipped: false
                    };
                }

                // Search for Vendor Return Authorization and find matching line items
                var vraSearch = search.create({
                    type: search.Type.VENDOR_RETURN_AUTHORIZATION,
                    filters: [
                        ['type', 'anyof', 'VendAuth'],
                        'AND',
                        ['memo', 'contains', extractedData.originalBillNumber]
                    ],
                    columns: [
                        'tranid',
                        'trandate',
                        'memo',
                        'internalid',
                        'entity',
                        'status',
                        'item',
                        'amount',
                        'line'
                    ]
                });

                var vraResults = [];
                vraSearch.run().each(function (result) {
                    var lineMemo = result.getValue('memo');

                    // Check if this line's memo contains our original bill number
                    if (lineMemo && lineMemo.indexOf(extractedData.originalBillNumber) !== -1) {
                        vraResults.push({
                            internalId: result.getValue('internalid'),
                            tranid: result.getValue('tranid'),
                            trandate: result.getValue('trandate'),
                            memo: result.getValue('memo'),
                            entity: result.getValue('entity'),
                            status: result.getValue('status'),
                            lineItem: result.getValue('item'),
                            amount: result.getValue('amount'),
                            lineNumber: result.getValue('line')
                        });
                    }
                    return true; // Continue to get all results
                });

                log.debug('Vendor Return Authorization search results for CONCDA', {
                    originalBillNumber: extractedData.originalBillNumber,
                    searchResults: vraResults,
                    totalResults: vraResults.length,
                    fileName: splitPart.fileName,
                    partNumber: splitPart.partNumber,
                    recordId: recordId
                });

                if (vraResults.length > 0) {
                    // Parse the extracted total amount for comparison
                    var extractedAmount = parseFloat(extractedData.totalAmount.replace(/[()$,]/g, ''));
                    if (isNaN(extractedAmount)) {
                        return {
                            success: true,
                            isSkipped: true,
                            skipReason: 'CONCDA NARDA - Invalid extracted amount: ' + extractedData.totalAmount,
                            extractedData: extractedData
                        };
                    }

                    var extractedAmountAbs = Math.abs(extractedAmount);

                    // Find the line with matching amount
                    var matchingVRALine = null;
                    for (var i = 0; i < vraResults.length; i++) {
                        var vraLine = vraResults[i];
                        var lineAmountAbs = Math.abs(parseFloat(vraLine.lineAmount));

                        log.debug('Comparing amounts for CONCDA VRA line', {
                            vraInternalId: vraLine.internalId,
                            vraTransactionId: vraLine.tranid,
                            lineNumber: vraLine.lineNumber,
                            lineAmount: vraLine.lineAmount,
                            lineAmountAbs: lineAmountAbs,
                            extractedAmount: extractedAmount,
                            extractedAmountAbs: extractedAmountAbs,
                            amountDifference: Math.abs(lineAmountAbs - extractedAmountAbs),
                            lineMemo: vraLine.lineMemo,
                            originalBillNumber: extractedData.originalBillNumber
                        });

                        // Check if absolute amounts match (within a small tolerance for rounding)
                        if (Math.abs(lineAmountAbs - extractedAmountAbs) < 0.01) {
                            matchingVRALine = vraLine;
                            log.debug('Found matching VRA line for CONCDA', {
                                vraInternalId: vraLine.internalId,
                                vraTransactionId: vraLine.tranid,
                                lineNumber: vraLine.lineNumber,
                                lineAmount: vraLine.lineAmount,
                                lineAmountAbs: lineAmountAbs,
                                extractedAmount: extractedAmount,
                                extractedAmountAbs: extractedAmountAbs,
                                amountDifference: Math.abs(lineAmountAbs - extractedAmountAbs),
                                lineMemo: vraLine.lineMemo,
                                originalBillNumber: extractedData.originalBillNumber
                            });
                            break;
                        }
                    }

                    if (matchingVRALine) {
                        // Found matching VRA line - attempt to create Vendor Credit
                        log.debug('CONCDA - matching VRA line found, attempting Vendor Credit creation', {
                            nardaNumber: extractedData.nardaNumber,
                            originalBillNumber: extractedData.originalBillNumber,
                            matchingVRALine: {
                                vraInternalId: matchingVRALine.internalId,
                                vraTransactionId: matchingVRALine.tranid,
                                lineNumber: matchingVRALine.lineNumber,
                                lineAmount: matchingVRALine.lineAmount,
                                lineMemo: matchingVRALine.lineMemo,
                                entity: matchingVRALine.entity,
                                status: matchingVRALine.status
                            },
                            fileName: splitPart.fileName,
                            partNumber: splitPart.partNumber,
                            recordId: recordId
                        });

                        // Transform VRA to Vendor Credit - pass the specific line information
                        var vendorCreditResult = transformVRAToVendorCredit(
                            matchingVRALine.internalId,
                            extractedData,
                            splitPart.fileId,
                            recordId,
                            matchingVRALine.lineNumber  // Pass the specific line number
                        );

                        if (vendorCreditResult.success) {
                            return {
                                success: true,
                                isVendorCredit: true,
                                vendorCreditId: vendorCreditResult.vendorCreditId,
                                vendorCreditTranid: vendorCreditResult.vendorCreditTranid,
                                matchingVRA: {
                                    internalId: matchingVRALine.internalId,
                                    tranid: matchingVRALine.tranid,
                                    memo: matchingVRALine.memo,
                                    entity: matchingVRALine.entity,
                                    status: matchingVRALine.status
                                },
                                extractedData: extractedData,
                                attachmentResult: vendorCreditResult.attachmentResult
                            };
                        } else {
                            // Vendor Credit creation failed - skip and report error
                            return {
                                success: true,
                                isSkipped: true,
                                skipReason: 'CONCDA NARDA - Vendor Credit creation failed: ' + vendorCreditResult.error,
                                matchingVRA: {
                                    internalId: matchingVRALine.internalId,
                                    tranid: matchingVRALine.tranid,
                                    memo: matchingVRALine.memo,
                                    entity: matchingVRALine.entity,
                                    status: matchingVRALine.status
                                },
                                extractedData: extractedData,
                                vendorCreditError: vendorCreditResult.error
                            };
                        }
                    } else {
                        // Found VRA with matching bill number but no matching amount
                        log.debug('CONCDA NARDA found VRA with bill number but no matching amount - skipping', {
                            nardaNumber: extractedData.nardaNumber,
                            originalBillNumber: extractedData.originalBillNumber,
                            extractedAmount: extractedAmount,
                            extractedAmountAbs: extractedAmountAbs,
                            vraLinesFound: vraResults.length,
                            vraLines: vraResults.map(function (line) {
                                return {
                                    vraId: line.internalId,
                                    tranid: line.tranid,
                                    lineAmount: line.lineAmount,
                                    lineAmountAbs: Math.abs(parseFloat(line.lineAmount)),
                                    amountDifference: Math.abs(Math.abs(parseFloat(line.lineAmount)) - extractedAmountAbs)
                                };
                            }),
                            fileName: splitPart.fileName,
                            partNumber: splitPart.partNumber,
                            recordId: recordId
                        });

                        return {
                            success: true,
                            isSkipped: true,
                            skipReason: 'CONCDA NARDA - found VRA with bill number but no matching line amount',
                            extractedData: extractedData
                        };
                    }
                } else {
                    // No matching VRA found - log this but still skip
                    log.debug('CONCDA NARDA found but no matching VRA line - skipping journal entry', {
                        nardaNumber: extractedData.nardaNumber,
                        originalBillNumber: extractedData.originalBillNumber,
                        fileName: splitPart.fileName,
                        partNumber: splitPart.partNumber,
                        recordId: recordId
                    });

                    return {
                        success: true,
                        isSkipped: true,
                        skipReason: 'CONCDA NARDA - no VRA line found with matching bill number',
                        extractedData: extractedData
                    };
                }
            }

            // ... rest of existing journal entry creation logic remains unchanged ...
            log.debug('Creating Journal Entry from JSON extracted data', {
                invoiceNumber: extractedData.invoiceNumber,
                invoiceDate: extractedData.invoiceDate,
                totalAmount: extractedData.totalAmount,
                nardaNumber: extractedData.nardaNumber,
                fileName: splitPart.fileName,
                partNumber: splitPart.partNumber,
                recordId: recordId
            });

            // Create tranid first to check for duplicates
            var tranid = extractedData.invoiceNumber + ' CM';

            // Check for duplicate journal entry by tranid
            var duplicateCheck = checkForDuplicateJournalEntry(tranid, recordId);
            if (!duplicateCheck.success) {
                log.error('Duplicate journal entry detected', {
                    tranid: tranid,
                    existingJournalEntry: duplicateCheck.existingEntry,
                    fileName: splitPart.fileName,
                    partNumber: splitPart.partNumber,
                    recordId: recordId
                });
                return {
                    success: false,
                    error: 'Duplicate journal entry exists with tranid: ' + tranid + ' (ID: ' + duplicateCheck.existingEntry.internalId + ')',
                    isDuplicate: true,
                    existingJournalEntry: duplicateCheck.existingEntry
                };
            }

            // Find Credit Line Entity based on NARDA number
            var creditLineEntity = findCreditLineEntity(extractedData.nardaNumber, recordId);
            if (!creditLineEntity.success) {
                log.error('Could not find Credit Line Entity', {
                    nardaNumber: extractedData.nardaNumber,
                    error: creditLineEntity.error,
                    recordId: recordId
                });
                return { success: false, error: 'Could not find Credit Line Entity: ' + creditLineEntity.error };
            }

            // Parse the total amount (remove parentheses and convert to positive number)
            var totalAmount = parseFloat(extractedData.totalAmount.replace(/[()$,]/g, ''));
            if (isNaN(totalAmount)) {
                log.error('Invalid total amount from JSON results', {
                    totalAmount: extractedData.totalAmount,
                    parsedAmount: totalAmount,
                    recordId: recordId
                });
                return { success: false, error: 'Invalid total amount: ' + extractedData.totalAmount };
            }

            // Parse the date
            var jeDate = new Date(extractedData.invoiceDate);
            if (isNaN(jeDate.getTime())) {
                log.error('Invalid invoice date from JSON results', {
                    invoiceDate: extractedData.invoiceDate,
                    recordId: recordId
                });
                return { success: false, error: 'Invalid invoice date: ' + extractedData.invoiceDate };
            }

            // Create memo
            var mainMemo = 'MARCONE CM' + extractedData.invoiceNumber + ' ' + extractedData.nardaNumber;
            var creditLineMemo = mainMemo; // Same memo for credit line

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

            // First line - Debit to Account 111 (Accounts Payable)
            journalEntry.selectNewLine({
                sublistId: 'line'
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'account',
                value: 111
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'debit',
                value: totalAmount
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'memo',
                value: mainMemo
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'entity',
                value: 2106 // Marcone
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
                value: 119
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'credit',
                value: totalAmount
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'memo',
                value: creditLineMemo
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

            log.debug('Journal Entry Created Successfully from JSON data', {
                journalEntryId: jeId,
                tranid: tranid,
                date: jeDate,
                totalAmount: totalAmount,
                mainMemo: mainMemo,
                creditLineEntity: creditLineEntity.entityId,
                fileName: splitPart.fileName,
                partNumber: splitPart.partNumber,
                recordId: recordId
            });

            // Attach the PDF file to the journal entry
            var attachResult = attachFileToRecord(jeId, splitPart.fileId, recordId);

            return {
                success: true,
                journalEntryId: jeId,
                tranid: tranid,
                attachmentResult: attachResult
            };

        } catch (error) {
            log.error('Error creating journal entry from JSON data', {
                error: error.toString(),
                fileName: splitPart.fileName,
                partNumber: splitPart.partNumber,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }
    */


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
                originalRecordId: originalRecordId,
                recordType: targetRecordType
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
                originalRecordId: originalRecordId,
                recordType: targetRecordType
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

    function sendResultsEmail(totalZoneCaptureRecords, singlePdfCount, splitPdfCount, totalPdfPartsProcessed,
        jsonConversions, journalEntriesCreated, vendorCreditsCreated,
        processedDetails, failedEntries, recordsInactivated, recordsLeftActive, skippedEntries) {
        try {
            // Get script parameters for email configuration
            var script = runtime.getCurrentScript();
            var recipientEmail = script.getParameter({
                name: 'custscript_bas_marcone_warr_bc_conf_emai'
            });
            var copyRecipientEmail = script.getParameter({
                name: 'custscript_bas_marcone_warr_bc_conf_copy'
            });

            // Get NetSuite domain for URL construction
            var currentUrl = 'https://system.netsuite.com';
            try {
                var accountId = runtime.accountId;
                if (accountId) {
                    currentUrl = 'https://' + accountId.toLowerCase().replace('_', '-') + '.app.netsuite.com';
                }
            } catch (urlError) {
                log.debug('Could not determine specific NetSuite URL, using default', urlError);
                currentUrl = 'https://system.netsuite.com';
            }

            // Build email recipients array
            var recipients = [];
            if (recipientEmail) {
                recipients.push(recipientEmail);
            }

            var ccRecipients = [];
            if (copyRecipientEmail) {
                ccRecipients.push(copyRecipientEmail);
            }

            // Calculate split parts for display
            var splitPartsCount = totalPdfPartsProcessed - singlePdfCount;

            // NEW: Separate duplicates from other skipped entries
            var duplicateEntries = [];
            var manualProcessingEntries = [];

            for (var k = 0; k < skippedEntries.length; k++) {
                var skippedDetail = skippedEntries[k];
                if (skippedDetail.skipType === 'DUPLICATE_VENDOR_CREDIT' ||
                    skippedDetail.skipType === 'DUPLICATE_JOURNAL_ENTRY') {
                    duplicateEntries.push(skippedDetail);
                } else {
                    manualProcessingEntries.push(skippedDetail);
                }
            }

            // Build email body with summary section
            var emailBody = 'Marcone Warranty Bill Credit PDF Processing Complete\n\n';
            emailBody += 'SUMMARY:\n';
            emailBody += '========\n';
            emailBody += '- Total ZoneCapture Records Processed: ' + totalZoneCaptureRecords + '\n';
            emailBody += '- Single PDF Files Found: ' + singlePdfCount + '\n';
            if (splitPdfCount > 0) {
                emailBody += '- Combined PDF Files Found: ' + splitPdfCount + ' (Split into ' + splitPartsCount + ' Parts)\n';
            } else {
                emailBody += '- Combined PDF Files Found: 0\n';
            }
            emailBody += '- Total PDFs Processed: ' + totalPdfPartsProcessed + '\n';
            emailBody += '- Journal Entries Created: ' + journalEntriesCreated + '\n';
            emailBody += '- Vendor Credits Created: ' + vendorCreditsCreated + '\n';
            emailBody += '- Duplicate Transactions Found (resolved): ' + duplicateEntries.length + '\n';  // NEW
            emailBody += '- Transaction Processing Failed: ' + failedEntries.length + '\n';
            emailBody += '- Transactions Requiring Manual Processing: ' + manualProcessingEntries.length + '\n';  // UPDATED
            emailBody += '- ZoneCapture Source Records Deactivated: ' + recordsInactivated + '\n';
            emailBody += '- ZoneCapture Source Records Left Active: ' + recordsLeftActive + '\n\n';

            // Add validation check
            var totalTransactions = journalEntriesCreated + vendorCreditsCreated + failedEntries.length + skippedEntries.length;
            var totalRecords = recordsInactivated + recordsLeftActive;

            emailBody += 'VALIDATION CHECK:\n';
            emailBody += '================\n';
            emailBody += '- Total PDF Processing: ' + totalTransactions + ' = ' + journalEntriesCreated + ' JE + ' + vendorCreditsCreated + ' VC + ' + failedEntries.length + ' Failed + ' + skippedEntries.length + ' Skipped\n';
            emailBody += '- Expected Total PDFs: ' + totalPdfPartsProcessed + '\n';
            emailBody += '- Match: ' + (totalTransactions === totalPdfPartsProcessed ? 'YES ✓' : 'NO ✗ (DISCREPANCY)') + '\n';
            emailBody += '- Total ZoneCapture Records: ' + totalRecords + ' = ' + recordsInactivated + ' Inactivated + ' + recordsLeftActive + ' Active\n';
            emailBody += '- Expected Total Records: ' + totalZoneCaptureRecords + '\n';
            emailBody += '- Match: ' + (totalRecords === totalZoneCaptureRecords ? 'YES ✓' : 'NO ✗ (DISCREPANCY)') + '\n\n';

            // SUCCESSFUL TRANSACTIONS SECTION
            if (processedDetails.length > 0) {
                emailBody += 'SUCCESSFUL TRANSACTIONS:\n';
                emailBody += '========================\n\n';

                for (var i = 0; i < processedDetails.length; i++) {
                    var detail = processedDetails[i];

                    if (detail.isVendorCredit) {
                        var vendorCreditUrl = currentUrl + '/app/accounting/transactions/vendcred.nl?id=' + detail.vendorCreditId;
                        var vraUrl = currentUrl + '/app/accounting/transactions/vendauth.nl?id=' + detail.matchingVRA.internalId;

                        // Get the NARDA description
                        var nardaDescription = detail.nardaNumber || 'CONCDA/NF/CORE';
                        if (detail.nardaTypes && detail.nardaTypes.length > 1) {
                            nardaDescription = detail.nardaTypes.join('+') + ' (Combined)';
                        }

                        // Enhanced description with line count
                        var vcDescription = nardaDescription;
                        if (detail.matchedLineCount && detail.matchedLineCount > 1) {
                            vcDescription += ' - ' + detail.matchedLineCount + ' VRA Lines Grouped';
                        }

                        emailBody += 'VENDOR CREDIT #' + (i + 1) + ' (' + vcDescription + '):\n';
                        emailBody += '  Source Record ID: ' + detail.recordId + '\n';
                        emailBody += '  Original PDF: ' + detail.sourceFileName + '\n';
                        emailBody += '  Split Part: ' + detail.splitPartNumber + ' (' + detail.splitFileName + ')\n';
                        emailBody += '  PDF URL: ' + detail.pdfUrl + '\n';
                        emailBody += '  Vendor Credit ID: ' + detail.vendorCreditId + '\n';
                        emailBody += '  Vendor Credit URL: ' + vendorCreditUrl + '\n';
                        emailBody += '  Vendor Credit Tranid: ' + detail.vendorCreditTranid + '\n';
                        emailBody += '  Source VRA ID: ' + detail.matchingVRA.internalId + '\n';
                        emailBody += '  Source VRA Tranid: ' + detail.matchingVRA.tranid + '\n';
                        emailBody += '  Source VRA URL: ' + vraUrl + '\n';

                        // Add VRA line matching details for grouped vendor credits
                        if (detail.matchedLineCount) {
                            emailBody += '  VRA Lines Matched: ' + detail.matchedLineCount + '\n';
                            if (detail.matchingVRA.matchedLineNumbers && detail.matchingVRA.matchedLineNumbers.length > 0) {
                                emailBody += '  VRA Line Numbers: ' + detail.matchingVRA.matchedLineNumbers.join(', ') + '\n';
                            }
                        } else if (detail.matchingVRA.lineNumber) {
                            emailBody += '  VRA Line Number: ' + detail.matchingVRA.lineNumber + '\n';
                        }

                    } else {
                        // JOURNAL ENTRY PROCESSING
                        var journalEntryUrl = currentUrl + '/app/accounting/transactions/journal.nl?id=' + detail.journalEntryId;

                        emailBody += 'JOURNAL ENTRY #' + (i + 1) + ':\n';
                        emailBody += '  Source Record ID: ' + detail.recordId + '\n';
                        emailBody += '  Original PDF: ' + detail.sourceFileName + '\n';
                        emailBody += '  Split Part: ' + detail.splitPartNumber + ' (' + detail.splitFileName + ')\n';
                        emailBody += '  PDF URL: ' + detail.pdfUrl + '\n';
                        emailBody += '  Journal Entry ID: ' + detail.journalEntryId + '\n';
                        emailBody += '  Journal Entry URL: ' + journalEntryUrl + '\n';
                        emailBody += '  Transaction ID: ' + detail.tranid + '\n';

                        // Enhanced multi-NARDA reporting with proper terminology
                        if (detail.nardaGroups && detail.nardaGroups.length > 1) {
                            emailBody += '  Type: Multi-NARDA Groups\n';
                            emailBody += '  NARDA Groups: ' + detail.nardaGroups.join(', ') + '\n';
                            emailBody += '  Grand Total: $' + (detail.totalAmount ? detail.totalAmount.toFixed(2) : '0.00') + '\n';
                        } else if (detail.nardaGroups && detail.nardaGroups.length === 1) {
                            // Check if this is consolidated (multiple lines same NARDA) or single line
                            if (detail.consolidatedLines && detail.consolidatedLines > 1) {
                                emailBody += '  Type: Consolidated NARDA (' + detail.consolidatedLines + ' lines)\n';
                                emailBody += '  NARDA: ' + detail.nardaGroups[0] + '\n';
                            } else {
                                emailBody += '  Type: Single NARDA\n';
                                emailBody += '  NARDA: ' + detail.nardaGroups[0] + '\n';
                            }
                            emailBody += '  Total: $' + (detail.totalAmount ? detail.totalAmount.toFixed(2) : '0.00') + '\n';
                        }
                    }

                    // EXTRACTED DATA SECTION - Common for both JE and VC
                    var nardaNumber = 'Not found';
                    var totalAmount = 'Not found';
                    var invoiceNumber = 'Not found';
                    var invoiceDate = 'Not found';
                    var deliveryAmount = '$0.00';
                    var originalBillNumber = 'Not found';

                    // Process NARDA number
                    if (detail.isVendorCredit) {
                        if (detail.nardaNumber) {
                            nardaNumber = detail.nardaNumber;
                        }
                    } else {
                        // For journal entries
                        if (detail.nardaNumber) {
                            nardaNumber = detail.nardaNumber;
                        } else if (detail.nardaGroups && detail.nardaGroups.length > 0) {
                            nardaNumber = 'Multiple: ' + detail.nardaGroups.join(', ');
                        }
                    }

                    // Process total amount
                    if (detail.isVendorCredit) {
                        // FIXED: Use grouped total amount for vendor credits
                        if (detail.totalAmount !== undefined) {
                            totalAmount = '($' + Math.abs(detail.totalAmount).toFixed(2) + ')';
                        } else if (detail.matchingVRA && detail.matchingVRA.amount) {
                            // Fallback for backwards compatibility
                            totalAmount = '($' + Math.abs(parseFloat(detail.matchingVRA.amount)).toFixed(2) + ')';
                        }
                    } else {
                        // For journal entries
                        if (detail.totalAmount) {
                            totalAmount = '$' + detail.totalAmount.toFixed(2);
                        } else if (detail.grandTotal) {
                            totalAmount = '$' + detail.grandTotal.toFixed(2);
                        }
                    }

                    // Get common data from extractedData (document-level info)
                    if (detail.extractedData) {
                        if (detail.extractedData.invoiceNumber) {
                            invoiceNumber = detail.extractedData.invoiceNumber;
                        }
                        if (detail.extractedData.invoiceDate) {
                            invoiceDate = detail.extractedData.invoiceDate;
                        }
                        if (detail.extractedData.deliveryAmount) {
                            deliveryAmount = detail.extractedData.deliveryAmount;
                        }
                    }

                    // Process original bill number for vendor credits
                    if (detail.isVendorCredit) {
                        // FIXED: Use the original bill number directly from the detail
                        if (detail.originalBillNumber) {
                            originalBillNumber = detail.originalBillNumber;
                        } else if (detail.extractedData && detail.extractedData.groupedLineItems) {
                            // Fallback to old method for backwards compatibility
                            var groups = Object.keys(detail.extractedData.groupedLineItems);
                            for (var g = 0; g < groups.length; g++) {
                                var groupName = groups[g];
                                if (groupName.toUpperCase() === 'CONCDA' ||
                                    groupName.toUpperCase() === 'NF' ||
                                    groupName.toUpperCase() === 'CORE') {
                                    var group = detail.extractedData.groupedLineItems[groupName];
                                    if (group.originalBillNumbers && group.originalBillNumbers.length > 0) {
                                        originalBillNumber = group.originalBillNumbers.join(', ');
                                    }
                                    break;
                                }
                            }
                        }
                    }

                    // Display extracted information
                    if (detail.nardaTypes && detail.nardaTypes.length > 0) {
                        emailBody += '  NARDA Types: ' + detail.nardaTypes.join(', ') + '\n';
                    }
                    emailBody += '  NARDA Number: ' + nardaNumber + '\n';
                    emailBody += '  Invoice Number: ' + invoiceNumber + '\n';
                    emailBody += '  Invoice Date: ' + invoiceDate + '\n';
                    emailBody += '  Total Amount: ' + totalAmount + '\n';
                    emailBody += '  Delivery Amount: ' + deliveryAmount + '\n';
                    if (detail.isVendorCredit) {
                        emailBody += '  Original Bill Number: ' + originalBillNumber + '\n';
                    }

                    // File attachment status
                    if (detail.attachmentResult) {
                        if (detail.attachmentResult.attachmentFailed) {
                            emailBody += '  File Attachment: FAILED - ' + detail.attachmentResult.error + '\n';
                        } else {
                            emailBody += '  File Attachment: SUCCESS\n';
                        }
                    }

                    // Delivery processing status for vendor credits
                    if (detail.isVendorCredit && detail.deliveryAmountProcessed !== undefined) {
                        emailBody += '  Delivery Expense Added: ' + (detail.deliveryAmountProcessed ? 'YES' : 'NO') + '\n';
                    }

                    emailBody += '\n';
                }
            }

            // NEW: DUPLICATE TRANSACTIONS SECTION (NO ACTION NEEDED)
            if (duplicateEntries.length > 0) {
                emailBody += 'DUPLICATE TRANSACTIONS FOUND (NO ACTION NEEDED):\n';
                emailBody += '===============================================\n\n';

                for (var d = 0; d < duplicateEntries.length; d++) {
                    var dupDetail = duplicateEntries[d];
                    emailBody += 'DUPLICATE #' + (d + 1) + ':\n';
                    emailBody += '  Source Record ID: ' + dupDetail.recordId + '\n';
                    emailBody += '  Original PDF: ' + dupDetail.sourceFileName + '\n';
                    emailBody += '  Skip Reason: ' + (dupDetail.skipReason || 'Not specified') + '\n';
                    emailBody += '  Skip Type: ' + (dupDetail.skipType || 'DUPLICATE') + '\n';
                    emailBody += '  ZoneCapture Status: RECORD DEACTIVATED (duplicate resolved)\n';
                    emailBody += '  ACTION REQUIRED: NONE - Existing transaction is valid\n';

                    if (dupDetail.existingJournalEntry) {
                        emailBody += '  Existing JE ID: ' + dupDetail.existingJournalEntry.internalId + '\n';
                        emailBody += '  Existing JE URL: ' + currentUrl + '/app/accounting/transactions/journal.nl?id=' + dupDetail.existingJournalEntry.internalId + '\n';
                    }
                    if (dupDetail.existingVendorCredit) {
                        emailBody += '  Existing VC ID: ' + dupDetail.existingVendorCredit.internalId + '\n';
                        emailBody += '  Existing VC URL: ' + currentUrl + '/app/accounting/transactions/vendcred.nl?id=' + dupDetail.existingVendorCredit.internalId + '\n';
                    }
                    emailBody += '\n';
                }
            }

            // MANUAL PROCESSING TRANSACTIONS SECTION
            if (manualProcessingEntries.length > 0) {
                emailBody += 'TRANSACTIONS REQUIRING MANUAL PROCESSING:\n';
                emailBody += '========================================\n\n';

                for (var m = 0; m < manualProcessingEntries.length; m++) {
                    var manualDetail = manualProcessingEntries[m];
                    emailBody += 'MANUAL PROCESSING #' + (m + 1) + ':\n';
                    emailBody += '  Source Record ID: ' + manualDetail.recordId + '\n';
                    emailBody += '  Original PDF: ' + manualDetail.sourceFileName + '\n';
                    emailBody += '  Split Part: ' + manualDetail.splitPartNumber + ' (' + manualDetail.splitFileName + ')\n';
                    emailBody += '  PDF URL: ' + manualDetail.pdfUrl + '\n';
                    emailBody += '  Skip Reason: ' + (manualDetail.skipReason || 'Not specified') + '\n';
                    emailBody += '  Skip Type: ' + (manualDetail.skipType || 'GENERAL') + '\n';
                    emailBody += '  ZoneCapture Status: SENT TO ZONECAPTURE FOR MANUAL PROCESSING\n';

                    // NEW: Simplified NEXT STEPS based on skipType
                    emailBody += '  NEXT STEPS: ';
                    if (manualDetail.skipType === 'SHORT_SHIP') {
                        emailBody += 'ACCOUNTING REVIEW, SHORT SHIP: Find Original Bill # ' + (manualDetail.originalBillNumber || 'from PDF') + ' and Credit it as a Short Ship Against Vendor Receivables Account 130411\n';
                    } else if (manualDetail.skipType === 'NO_VRA_MATCH') {
                        emailBody += 'SERVICE REVIEW: Needs VRMA\n';
                    } else if (manualDetail.skipType === 'NO_LINE_ITEMS_FOUND') {
                        emailBody += 'MANUAL DATA EXTRACTION: Review PDF and manually extract NARDA numbers, amounts, and line items for processing\n';
                    } else if (manualDetail.skipType === 'UNIDENTIFIED_NARDA') {
                        emailBody += 'NARDA REVIEW: Identify correct NARDA value and determine appropriate processing method\n';
                    } else if (manualDetail.skipType === 'MISSING_BILL_NUMBERS') {
                        emailBody += 'BILL NUMBER EXTRACTION: Extract original bill numbers from PDF and reprocess\n';
                    } else {
                        // Generic fallback for unknown skip types
                        emailBody += 'MANUAL REVIEW: Review PDF content and processing logs to determine appropriate action\n';
                    }

                    // Add VRA information if available
                    if (manualDetail.matchingVRA) {
                        var vraUrl = currentUrl + '/app/accounting/transactions/vendauth.nl?id=' + manualDetail.matchingVRA.internalId;
                        emailBody += '  Matching VRA ID: ' + manualDetail.matchingVRA.internalId + '\n';
                        emailBody += '  Matching VRA Tranid: ' + manualDetail.matchingVRA.tranid + '\n';
                        emailBody += '  Matching VRA URL: ' + vraUrl + '\n';
                        emailBody += '  VRA Memo: ' + (manualDetail.matchingVRA.memo || 'None') + '\n';
                    } else {
                        emailBody += '  Matching VRA: None found\n';
                    }

                    // Extract data for manual processing entries
                    var manualNardaNumber = 'Not found';
                    var manualTotalAmount = 'Not found';
                    var manualInvoiceNumber = 'Not found';
                    var manualInvoiceDate = 'Not found';
                    var manualDeliveryAmount = '$0.00';
                    var manualOriginalBillNumber = 'Not found';

                    if (manualDetail.nardaNumber) {
                        manualNardaNumber = manualDetail.nardaNumber;
                    }

                    if (manualDetail.totalAmount !== undefined && manualDetail.totalAmount !== null) {
                        manualTotalAmount = '($' + Math.abs(manualDetail.totalAmount).toFixed(2) + ')';
                    }

                    if (manualDetail.extractedData) {
                        if (manualDetail.extractedData.invoiceNumber) {
                            manualInvoiceNumber = manualDetail.extractedData.invoiceNumber;
                        }
                        if (manualDetail.extractedData.invoiceDate) {
                            manualInvoiceDate = manualDetail.extractedData.invoiceDate;
                        }
                        if (manualDetail.extractedData.deliveryAmount) {
                            manualDeliveryAmount = manualDetail.extractedData.deliveryAmount;
                        }

                        // Get NARDA and amounts from grouped line items
                        if (manualDetail.extractedData.groupedLineItems) {
                            var groups = Object.keys(manualDetail.extractedData.groupedLineItems);
                            if (groups.length > 0) {
                                if (manualNardaNumber === 'Not found') {
                                    manualNardaNumber = groups.join(', ');
                                }

                                // Get total amount across all groups if not already found
                                if (manualTotalAmount === 'Not found') {
                                    var totalAmt = 0;
                                    for (var g = 0; g < groups.length; g++) {
                                        var group = manualDetail.extractedData.groupedLineItems[groups[g]];
                                        if (group.totalAmount) {
                                            totalAmt += group.totalAmount;
                                        }
                                    }
                                    if (totalAmt > 0) {
                                        manualTotalAmount = '($' + totalAmt.toFixed(2) + ')';
                                    }
                                }

                                // Get original bill numbers
                                for (var g = 0; g < groups.length; g++) {
                                    var group = manualDetail.extractedData.groupedLineItems[groups[g]];
                                    if (group.originalBillNumbers && group.originalBillNumbers.length > 0) {
                                        manualOriginalBillNumber = group.originalBillNumbers.join(', ');
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    emailBody += '  NARDA Number: ' + manualNardaNumber + '\n';
                    emailBody += '  Invoice Number: ' + manualInvoiceNumber + '\n';
                    emailBody += '  Invoice Date: ' + manualInvoiceDate + '\n';
                    emailBody += '  Total Amount: ' + manualTotalAmount + '\n';
                    emailBody += '  Delivery Amount: ' + manualDeliveryAmount + '\n';
                    emailBody += '  Original Bill Number: ' + manualOriginalBillNumber + '\n';
                    emailBody += '\n';
                }
            }

            // FAILED TRANSACTIONS SECTION
            if (failedEntries.length > 0) {
                emailBody += 'FAILED TRANSACTIONS:\n';
                emailBody += '====================\n\n';

                for (var j = 0; j < failedEntries.length; j++) {
                    var failedDetail = failedEntries[j];
                    emailBody += 'FAILED ENTRY #' + (j + 1) + ':\n';
                    emailBody += '  Source Record ID: ' + failedDetail.recordId + '\n';
                    emailBody += '  Original PDF: ' + failedDetail.sourceFileName + '\n';
                    emailBody += '  Split Part: ' + failedDetail.splitPartNumber + ' (' + failedDetail.splitFileName + ')\n';
                    emailBody += '  PDF URL: ' + failedDetail.pdfUrl + '\n';

                    // Add ZoneCapture status indicator for failed entries
                    emailBody += '  ZoneCapture Status: SENT TO ZONECAPTURE FOR MANUAL PROCESSING\n';

                    if (failedDetail.isDuplicate) {
                        emailBody += '  Failure Type: DUPLICATE TRANSACTION\n';
                        if (failedDetail.existingJournalEntry) {
                            emailBody += '  Existing JE ID: ' + failedDetail.existingJournalEntry.internalId + '\n';
                            emailBody += '  Existing JE URL: ' + currentUrl + '/app/accounting/transactions/journal.nl?id=' + failedDetail.existingJournalEntry.internalId + '\n';
                        }
                        if (failedDetail.existingVendorCredit) {
                            emailBody += '  Existing VC ID: ' + failedDetail.existingVendorCredit.internalId + '\n';
                            emailBody += '  Existing VC URL: ' + currentUrl + '/app/accounting/transactions/vendcred.nl?id=' + failedDetail.existingVendorCredit.internalId + '\n';
                        }
                    } else {
                        emailBody += '  Failure Type: PROCESSING ERROR\n';
                    }

                    emailBody += '  Error: ' + failedDetail.error + '\n';

                    // Add failed PDF information
                    if (failedDetail.failedPdfSaved) {
                        emailBody += '  Failed PDF Saved: YES (ID: ' + failedDetail.failedPdfId + ') to folder ' + CONFIG.FOLDERS.FAILED + '\n';
                    } else {
                        emailBody += '  Failed PDF Saved: NO';
                        if (failedDetail.failedPdfError) {
                            emailBody += ' - Error: ' + failedDetail.failedPdfError;
                        }
                        emailBody += '\n';
                    }

                    // Extract data for failed entries
                    var failedNardaNumber = 'Not found';
                    var failedTotalAmount = 'Not found';
                    var failedInvoiceNumber = 'Not found';
                    var failedInvoiceDate = 'Not found';
                    var failedDeliveryAmount = '$0.00';

                    if (failedDetail.extractedData) {
                        if (failedDetail.extractedData.invoiceNumber) {
                            failedInvoiceNumber = failedDetail.extractedData.invoiceNumber;
                        }
                        if (failedDetail.extractedData.invoiceDate) {
                            failedInvoiceDate = failedDetail.extractedData.invoiceDate;
                        }
                        if (failedDetail.extractedData.deliveryAmount) {
                            failedDeliveryAmount = failedDetail.extractedData.deliveryAmount;
                        }

                        // Get NARDA and amounts from grouped line items
                        if (failedDetail.extractedData.groupedLineItems) {
                            var groups = Object.keys(failedDetail.extractedData.groupedLineItems);
                            if (groups.length > 0) {
                                failedNardaNumber = groups.join(', ');

                                var totalAmt = 0;
                                for (var g = 0; g < groups.length; g++) {
                                    var group = failedDetail.extractedData.groupedLineItems[groups[g]];
                                    if (group.totalAmount) {
                                        totalAmt += group.totalAmount;
                                    }
                                }
                                if (totalAmt > 0) {
                                    failedTotalAmount = '($' + totalAmt.toFixed(2) + ')';
                                }
                            }
                        }
                    }

                    emailBody += '  NARDA Number: ' + failedNardaNumber + '\n';
                    emailBody += '  Invoice Number: ' + failedInvoiceNumber + '\n';
                    emailBody += '  Invoice Date: ' + failedInvoiceDate + '\n';
                    emailBody += '  Total Amount: ' + failedTotalAmount + '\n';
                    emailBody += '  Delivery Amount: ' + failedDeliveryAmount + '\n';
                    emailBody += '\n';
                }
            }

            if (processedDetails.length === 0 && failedEntries.length === 0) {
                emailBody += 'No warranty bill credit applications were processed during this run.\n\n';
            }

            // PROCESS DETAILS SECTION
            emailBody += 'PROCESS DETAILS:\n';
            emailBody += '================\n';
            emailBody += 'This automated process:\n';
            emailBody += '1. Searches for warranty bill credit PDF files\n';
            emailBody += '2. Copies PDFs to file cabinet folder ' + CONFIG.FOLDERS.SOURCE + ' with public access\n';
            emailBody += '3. Splits multi-page PDFs using PDF.co API\n';
            emailBody += '4. Converts each split PDF to JSON for data extraction\n';
            emailBody += '5. Extracts invoice details and groups line items by NARDA number\n';
            emailBody += '6. Creates single journal entries with multiple lines for J#### and INV#### NARDA values\n';
            emailBody += '7. Creates vendor credits from matching VRAs for CONCDA, NF, and CORE NARDA values\n';
            emailBody += '8. Groups vendor credit lines by original bill number for efficiency\n';
            emailBody += '9. Skips SHORT NARDA values for manual short ship credit processing\n';
            emailBody += '10. Skips unidentified NARDA values for manual review\n';
            emailBody += '11. Attaches PDF files to created transactions\n';
            emailBody += '12. Stores all processed PDFs in file cabinet folder ' + CONFIG.FOLDERS.PROCESSED + '\n';
            emailBody += '13. Marks ZoneCapture source custom records as rejected (inactive) after successful processing or duplicate detection\n';
            emailBody += '14. Sends skipped/failed PDF parts to ZoneCapture via email for manual processing\n';
            if (journalEntriesCreated > 0 || vendorCreditsCreated > 0) {
                emailBody += '15. Triggers AR Application Script (customscript_bas_je_ar_appl_script) to process created transactions\n';
            }
            emailBody += '\n';

            // ZONECAPTURE INTEGRATION SECTION
            emailBody += 'ZONECAPTURE INTEGRATION:\n';
            emailBody += '=======================\n';
            emailBody += 'Any PDF parts that cannot be automatically processed are forwarded to ZoneCapture:\n';
            emailBody += '- Skipped transactions (SHORT, unidentified NARDA, no VRA match, etc.)\n';
            emailBody += '- Failed transactions (processing errors, missing data, etc.)\n';
            emailBody += '- PDF file is attached to ZoneCapture email for manual processing\n';
            emailBody += '- Detailed processing information and recommended actions included\n';
            emailBody += '- Monitor ZoneCapture queue for these manual processing requests\n';
            emailBody += '- NOTE: Duplicate transactions are NOT sent to ZoneCapture (automatically resolved)\n\n';

            // NARDA VALUE PROCESSING RULES SECTION
            emailBody += 'NARDA VALUE PROCESSING RULES:\n';
            emailBody += '=============================\n';
            emailBody += '- J#### (Job numbers): Creates journal entries\n';
            emailBody += '- INV#### (Invoice numbers): Creates journal entries\n';
            emailBody += '- Multiple different J#### or INV#### values: Creates ONE journal entry with multiple credit lines (Multi-NARDA Groups)\n';
            emailBody += '- Multiple lines with SAME J#### or INV#### value: Creates ONE journal entry with consolidated amount (Consolidated NARDA)\n';
            emailBody += '- Single line with J#### or INV#### value: Creates standard journal entry (Single NARDA)\n';
            emailBody += '- CONCDA (Concealed damage): Creates vendor credits from matching VRAs grouped by original bill number\n';
            emailBody += '- NF (Not Needed): Creates vendor credits from matching VRAs grouped by original bill number\n';
            emailBody += '- CORE (Core exchange): Creates vendor credits from matching VRAs grouped by original bill number\n';
            emailBody += '- SHORT (Short shipment): Skipped for manual short ship credit processing\n';
            emailBody += '- Other values: Skipped for manual review and processing\n';
            emailBody += '\n';
            emailBody += 'NOTE: Delivery amounts are automatically added as expense lines (Account ' + CONFIG.ACCOUNTS.FREIGHT_IN + ' - FREIGHT IN) to vendor credits when present and greater than $0.00.\n\n';

            // QUICK ACCESS LINKS SECTION
            emailBody += 'QUICK ACCESS LINKS:\n';
            emailBody += '==================\n';
            if (processedDetails.length > 0) {
                emailBody += 'Click any transaction URL above to view the transaction directly in NetSuite.\n';
                emailBody += 'Journal entries follow the naming convention: [Invoice Number] CM\n';
                emailBody += 'Vendor credits use the invoice number as the transaction ID\n';
                emailBody += 'Grouped vendor credits combine multiple VRA lines for the same original bill number\n\n';
            }

            emailBody += 'Process completed at: ' + new Date().toString() + '\n';
            emailBody += 'Script: Marcone Product Warranty Bill Credit Processing Step 1';

            // Send email
            var emailOptions = {
                author: 151135,
                recipients: recipients,
                subject: 'Marcone Warranty Bill Credit PDF\'s Processed',
                body: emailBody
            };

            // Add CC recipients if specified
            if (ccRecipients.length > 0) {
                emailOptions.cc = ccRecipients;
            }

            email.send(emailOptions);

            log.debug('Email sent successfully', {
                recipients: recipients,
                ccRecipients: ccRecipients,
                successfulEntries: processedDetails.length,
                failedEntries: failedEntries.length,
                skippedEntries: skippedEntries.length,
                duplicateEntries: duplicateEntries.length,
                manualProcessingEntries: manualProcessingEntries.length,
                journalEntriesCreated: journalEntriesCreated,
                vendorCreditsCreated: vendorCreditsCreated,
                totalZoneCaptureRecordsProcessed: totalZoneCaptureRecords,
                totalPdfPartsProcessed: totalPdfPartsProcessed,
                netSuiteBaseUrl: currentUrl
            });

        } catch (emailError) {
            log.error('Error sending email', {
                error: emailError.toString(),
                recipients: recipients,
                ccRecipients: ccRecipients
            });
        }
    }

    function updateCustomRecordMemo(recordId, processingResults) {
        try {
            log.debug('Updating custom record memo with processing results', {
                recordId: recordId,
                needsSplitting: processingResults.needsSplitting,
                processingResults: processingResults
            });

            // Build detailed memo content
            var memoLines = [];
            var timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);

            // Summary section first - to calculate parts and next steps
            var totalParts = processingResults.splitParts ? processingResults.splitParts.length : 0;
            var successfulParts = 0;
            var failedParts = 0;
            var skippedParts = 0;
            var duplicateParts = 0;
            var unprocessedParts = [];
            var nextStepsNeeded = []; // Track what next steps are needed

            if (processingResults.splitParts) {
                for (var i = 0; i < processingResults.splitParts.length; i++) {
                    var part = processingResults.splitParts[i];
                    if (part.success) {
                        var hasTransactions = false;
                        var hasDuplicates = false;

                        if (part.transactions && part.transactions.length > 0) {
                            for (var j = 0; j < part.transactions.length; j++) {
                                var transaction = part.transactions[j];

                                if (transaction.type === 'journalEntry' || transaction.type === 'vendorCredit') {
                                    hasTransactions = true;
                                    break;
                                }

                                // Check for duplicate skip types
                                if (transaction.type === 'skipped' &&
                                    (transaction.skipType === 'DUPLICATE_VENDOR_CREDIT' ||
                                        transaction.skipType === 'DUPLICATE_JOURNAL_ENTRY')) {
                                    hasDuplicates = true;

                                    // Add duplicate next steps
                                    if (transaction.skipType === 'DUPLICATE_VENDOR_CREDIT') {
                                        nextStepsNeeded.push('DUPLICATE BC: Reject from ZoneCapture');
                                    } else if (transaction.skipType === 'DUPLICATE_JOURNAL_ENTRY') {
                                        nextStepsNeeded.push('DUPLICATE JE: Reject from ZoneCapture');
                                    }
                                }

                                // Check for other skip types that need next steps
                                if (transaction.type === 'skipped') {
                                    if (transaction.skipType === 'SHORT_SHIP') {
                                        var originalBillNumber = 'from PDF';
                                        // Try to get original bill number from transaction or extracted data
                                        if (transaction.originalBillNumber) {
                                            originalBillNumber = transaction.originalBillNumber;
                                        } else if (part.extractedData && part.extractedData.groupedLineItems) {
                                            var groups = Object.keys(part.extractedData.groupedLineItems);
                                            for (var g = 0; g < groups.length; g++) {
                                                var group = part.extractedData.groupedLineItems[groups[g]];
                                                if (group.originalBillNumbers && group.originalBillNumbers.length > 0) {
                                                    originalBillNumber = group.originalBillNumbers[0];
                                                    break;
                                                }
                                            }
                                        }
                                        nextStepsNeeded.push('A/P REVIEW, SHORT SHIP: Find Original Bill # ' + originalBillNumber + ' and Credit it as a Short Ship Against Vendor Receivables Account 130411');
                                    } else if (transaction.skipType === 'NO_VRA_MATCH') {
                                        nextStepsNeeded.push('SERVICE REVIEW: Needs VRMA');
                                    } else if (transaction.skipType === 'NO_MATCHING_OPEN_INVOICE') {
                                        nextStepsNeeded.push('A/R REVIEW: Confirm Invoice and Process Accordingly. If Invoice is Correctly Paid in Full, Book to COGS');
                                    } else if (transaction.skipType === 'NO_LINE_ITEMS_FOUND') {
                                        nextStepsNeeded.push('A/P REVIEW: Review PDF and Manually Identify Data for Processing');
                                    }
                                }
                            }
                        }

                        // Count parts
                        if (hasTransactions || hasDuplicates) {
                            if (hasDuplicates) {
                                duplicateParts++;
                            } else {
                                successfulParts++;
                            }
                        } else {
                            skippedParts++;
                            unprocessedParts.push(part);
                        }
                    } else {
                        failedParts++;
                        unprocessedParts.push(part);
                    }
                }
            }

            // NEXT STEPS SECTION - NOW AT THE VERY TOP
            if (nextStepsNeeded.length > 0) {
                memoLines.push('NEXT STEPS:');
                memoLines.push('-'.repeat(20));

                // Remove duplicates and add each unique next step
                var uniqueNextSteps = [];
                for (var n = 0; n < nextStepsNeeded.length; n++) {
                    if (uniqueNextSteps.indexOf(nextStepsNeeded[n]) === -1) {
                        uniqueNextSteps.push(nextStepsNeeded[n]);
                    }
                }

                for (var u = 0; u < uniqueNextSteps.length; u++) {
                    memoLines.push('• ' + uniqueNextSteps[u]);
                }
                memoLines.push('');
                memoLines.push('=' + '='.repeat(60));
                memoLines.push('');
            } else if (successfulParts > 0 || duplicateParts > 0) {
                memoLines.push('NEXT STEPS:');
                memoLines.push('-'.repeat(20));
                memoLines.push('• All parts processed successfully - No further action required');
                memoLines.push('');
                memoLines.push('=' + '='.repeat(60));
                memoLines.push('');
            }

            // NOW ADD THE HEADER AND REST OF THE MEMO
            memoLines.push('MARCONE WARRANTY PROCESSING RESULTS - ' + timestamp);
            memoLines.push('=' + '='.repeat(60));
            memoLines.push('');

            // Add split status information
            memoLines.push('PROCESSING TYPE: ' + (processingResults.needsSplitting ? 'SPLIT PDF' : 'SINGLE PDF'));
            memoLines.push('');

            // Determine overall processing status
            var overallStatus;
            if (processingResults.needsSplitting) {
                overallStatus = unprocessedParts.length > 0 ? 'PARTIAL - UNPROCESSED PARTS SENT TO ZONECAPTURE' : 'COMPLETED';
            } else {
                // For single PDFs, include duplicates as completed
                if (successfulParts > 0 || duplicateParts > 0) {
                    overallStatus = 'COMPLETED';
                } else if (unprocessedParts.length > 0) {
                    overallStatus = 'PENDING - REQUIRES MANUAL PROCESSING';
                } else {
                    overallStatus = 'FAILED';
                }
            }

            memoLines.push('SUMMARY:');
            memoLines.push('- Total PDF parts: ' + totalParts);
            memoLines.push('- Successfully processed: ' + successfulParts);
            memoLines.push('- Duplicate transactions found (no action needed): ' + duplicateParts);
            memoLines.push('- Failed processing: ' + failedParts);
            memoLines.push('- Skipped for manual review: ' + skippedParts);
            memoLines.push('- Unprocessed parts requiring action: ' + unprocessedParts.length);
            memoLines.push('- Overall status: ' + overallStatus);
            memoLines.push('');


            // Detailed results for each part
            if (processingResults.splitParts && processingResults.splitParts.length > 0) {
                memoLines.push('DETAILED RESULTS:');
                memoLines.push('-'.repeat(40));

                for (var i = 0; i < processingResults.splitParts.length; i++) {
                    var part = processingResults.splitParts[i];

                    memoLines.push('');
                    memoLines.push('PART ' + (i + 1) + ': ' + (part.fileName || 'Unknown file'));

                    if (part.success && part.transactions && part.transactions.length > 0) {
                        // Process each transaction for this part
                        for (var j = 0; j < part.transactions.length; j++) {
                            var transaction = part.transactions[j];

                            if (transaction.type === 'journalEntry') {
                                memoLines.push('  Status: JOURNAL ENTRY CREATED');
                                memoLines.push('  JE ID: ' + (transaction.id || 'Not found'));
                                memoLines.push('  JE Tranid: ' + (transaction.tranid || 'Not found'));
                                memoLines.push('  NARDA: ' + (transaction.nardaNumber || 'Not found'));
                                memoLines.push('  Amount: $' + (transaction.totalAmount ? transaction.totalAmount.toFixed(2) : '0.00'));
                            } else if (transaction.type === 'vendorCredit') {
                                memoLines.push('  Status: VENDOR CREDIT CREATED');
                                memoLines.push('  VC ID: ' + (transaction.id || 'Not found'));
                                memoLines.push('  VC Tranid: ' + (transaction.tranid || 'Not found'));
                                memoLines.push('  NARDA: ' + (transaction.nardaNumber || 'Not found'));
                                memoLines.push('  Amount: ($' + (transaction.totalAmount ? Math.abs(transaction.totalAmount).toFixed(2) : '0.00') + ')');
                                if (transaction.matchingVRA) {
                                    memoLines.push('  Source VRA: ' + (transaction.matchingVRA.tranid || 'Unknown') + ' (ID: ' + (transaction.matchingVRA.internalId || 'Unknown') + ')');
                                }
                            } else if (transaction.type === 'skipped') {
                                // Handle skipped transactions
                                if (transaction.skipType === 'DUPLICATE_VENDOR_CREDIT') {
                                    memoLines.push('  Status: DUPLICATE BC - RECORD DEACTIVATED');
                                    memoLines.push('  Reason: Duplicate vendor credit found');
                                } else if (transaction.skipType === 'DUPLICATE_JOURNAL_ENTRY') {
                                    memoLines.push('  Status: DUPLICATE JE - RECORD DEACTIVATED');
                                    memoLines.push('  Reason: Duplicate journal entry found');
                                } else {
                                    memoLines.push('  Status: SKIPPED - SENT TO ZONECAPTURE');
                                    memoLines.push('  Reason: ' + (transaction.skipReason || 'Unknown'));
                                }

                                memoLines.push('  Type: ' + (transaction.skipType || 'GENERAL'));
                                memoLines.push('  NARDA: ' + (transaction.nardaNumber || 'Not found'));
                            } else if (transaction.type === 'failed') {
                                memoLines.push('  Status: FAILED - SENT TO ZONECAPTURE');
                                memoLines.push('  Error: ' + (transaction.error || 'Unknown error'));

                                if (transaction.isDuplicate) {
                                    memoLines.push('  Type: DUPLICATE TRANSACTION');
                                } else {
                                    memoLines.push('  Type: PROCESSING ERROR');
                                }
                            }
                        }

                        // Extract and display common data
                        if (part.extractedData) {
                            memoLines.push('  Invoice: ' + (part.extractedData.invoiceNumber || 'Not found'));
                            memoLines.push('  Date: ' + (part.extractedData.invoiceDate || 'Not found'));
                            if (part.extractedData.deliveryAmount && part.extractedData.deliveryAmount !== '$0.00') {
                                memoLines.push('  Delivery: ' + part.extractedData.deliveryAmount);
                            }
                        }
                    } else if (part.success) {
                        // Success but no transactions - this indicates skipped processing
                        memoLines.push('  Status: SKIPPED - SENT TO ZONECAPTURE');
                        memoLines.push('  Reason: No transactions created');

                        // Extract basic data if available
                        if (part.extractedData) {
                            memoLines.push('  Invoice: ' + (part.extractedData.invoiceNumber || 'Not found'));
                            if (part.extractedData.groupedLineItems) {
                                var nardaGroups = Object.keys(part.extractedData.groupedLineItems);
                                memoLines.push('  NARDA Groups: ' + (nardaGroups.length > 0 ? nardaGroups.join(', ') : 'None found'));
                            }
                        }
                    } else {
                        // Failed processing
                        memoLines.push('  Status: FAILED - SENT TO ZONECAPTURE');
                        memoLines.push('  Error: ' + (part.error || 'JSON conversion failed'));
                    }
                }
            }

            // Processing metadata
            memoLines.push('');
            memoLines.push('METADATA:');
            memoLines.push('- Processed by: Marcone Warranty Bill Credit Processing Script');
            memoLines.push('- Process ID: ' + recordId);
            memoLines.push('- Timestamp: ' + timestamp);

            // Determine if record should be deactivated
            var shouldDeactivate;
            if (processingResults.needsSplitting) {
                // Always deactivate split PDFs (original combined record no longer needed)
                shouldDeactivate = true;
                memoLines.push('- Record Status: DEACTIVATED (split PDF - original combined record no longer needed)');
            } else {
                // NEW: Deactivate single PDFs if successfully processed OR if duplicates found
                shouldDeactivate = (successfulParts > 0) || (duplicateParts > 0);
                if (shouldDeactivate) {
                    if (duplicateParts > 0 && successfulParts === 0) {
                        memoLines.push('- Record Status: DEACTIVATED (duplicate transactions found - no action needed)');
                    } else {
                        memoLines.push('- Record Status: DEACTIVATED (single PDF processed successfully)');
                    }
                } else {
                    memoLines.push('- Record Status: ACTIVE (single PDF requires manual processing)');
                }
            }

            // Join all lines and truncate if necessary
            var fullMemo = memoLines.join('\n');
            var maxLength = 3900;
            if (fullMemo.length > maxLength) {
                var truncatedMemo = fullMemo.substring(0, maxLength - 50);
                truncatedMemo += '\n\n[TRUNCATED - Full details in processing logs]';
                fullMemo = truncatedMemo;
            }

            // Load and update the custom record
            var customRecord = record.load({
                type: 'customrecord_eff_nsp2p_xml2nstrans',
                id: recordId
            });

            customRecord.setValue({
                fieldId: 'custrecord_eff_nsp2p_trans_memo',
                value: fullMemo
            });

            // Set transaction type to "Bill Credit"
            customRecord.setValue({
                fieldId: 'custrecord_eff_nsp2p_trans_transtype',
                value: '20'  // or 'vendcred' - use whichever format the field expects
            });

            // Set active/inactive status based on logic above
            customRecord.setValue({
                fieldId: 'isinactive',
                value: shouldDeactivate
            });

            var savedRecordId = customRecord.save();

            log.debug('Custom record memo updated', {
                recordId: recordId,
                savedRecordId: savedRecordId,
                memoLength: fullMemo.length,
                needsSplitting: processingResults.needsSplitting,
                shouldDeactivate: shouldDeactivate,
                successfulParts: successfulParts,
                duplicateParts: duplicateParts,
                failedParts: failedParts,
                skippedParts: skippedParts,
                unprocessedParts: unprocessedParts.length
            });

            // MODIFIED: Handle unprocessed parts - send to ZoneCapture ONLY for split PDFs
            var zoneCaptureSentCount = 0;
            var zoneCaptureFailedCount = 0;

            if (unprocessedParts.length > 0 && processingResults.needsSplitting) {
                // ONLY send to ZoneCapture if this was a split PDF
                log.debug('Processing unprocessed parts for ZoneCapture submission (SPLIT PDF only)', {
                    unprocessedPartsCount: unprocessedParts.length,
                    needsSplitting: processingResults.needsSplitting,
                    recordId: recordId
                });

                for (var i = 0; i < unprocessedParts.length; i++) {
                    var unprocessedPart = unprocessedParts[i];
                    var emailResult = sendUnprocessedPartToZoneCapture(unprocessedPart, recordId);

                    if (emailResult.success) {
                        zoneCaptureSentCount++;
                    } else {
                        zoneCaptureFailedCount++;
                    }
                }

                log.debug('ZoneCapture submission complete', {
                    totalUnprocessed: unprocessedParts.length,
                    sentToZoneCapture: zoneCaptureSentCount,
                    failedToSend: zoneCaptureFailedCount,
                    recordId: recordId
                });
            } else if (unprocessedParts.length > 0 && !processingResults.needsSplitting) {
                // For single PDFs, just log that we're NOT sending to ZoneCapture
                log.debug('Single PDF with unprocessed parts - NOT sending to ZoneCapture (record remains active)', {
                    unprocessedPartsCount: unprocessedParts.length,
                    needsSplitting: processingResults.needsSplitting,
                    recordId: recordId,
                    reason: 'Single PDF failures remain as active records for manual processing'
                });
            }

            return {
                success: true,
                recordId: savedRecordId,
                memoUpdated: true,
                recordDeactivated: shouldDeactivate,
                summary: {
                    totalParts: totalParts,
                    successfulParts: successfulParts,
                    duplicateParts: duplicateParts,
                    failedParts: failedParts,
                    skippedParts: skippedParts,
                    unprocessedParts: unprocessedParts.length,
                    zoneCaptureSentCount: zoneCaptureSentCount,
                    zoneCaptureFailedCount: zoneCaptureFailedCount,
                    needsSplitting: processingResults.needsSplitting
                }
            };

        } catch (error) {
            log.error('Error updating custom record memo', {
                error: error.toString(),
                recordId: recordId
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function sendUnprocessedPartToZoneCapture(unprocessedPart, originalRecordId) {
        try {
            // Get script parameters for ZoneCapture email configuration
            var script = runtime.getCurrentScript();
            var zoneCaptureEmail = script.getParameter({
                name: 'custscript_bas_marcone_warranty_zc_email'
            });

            if (!zoneCaptureEmail) {
                log.error('ZoneCapture email parameter not configured', {
                    parameterName: 'custscript_bas_marcone_warranty_zc_email',
                    unprocessedPartFileName: unprocessedPart.fileName,
                    originalRecordId: originalRecordId
                });
                return {
                    success: false,
                    error: 'ZoneCapture email parameter not configured'
                };
            }

            // Extract invoice number for subject line
            var invoiceNumber = 'Unknown';
            if (unprocessedPart.extractedData && unprocessedPart.extractedData.invoiceNumber) {
                invoiceNumber = unprocessedPart.extractedData.invoiceNumber;
            }

            // Build email subject
            var emailSubject = 'Automated Processing Skipped / Failed - Invoice: ' + invoiceNumber;

            // Build detailed email body
            var emailBody = 'MARCONE WARRANTY BILL CREDIT - AUTOMATED PROCESSING INCOMPLETE\n\n';
            emailBody += 'An automated processing attempt was made but could not be completed.\n';
            emailBody += 'The record in ZoneCapture will remain active.\n';
            emailBody += 'This PDF has been forwarded for for manual review.\n\n';

            emailBody += 'PROCESSING SUMMARY:\n';
            emailBody += '==================\n';
            emailBody += 'Original Record ID: ' + originalRecordId + '\n';
            emailBody += 'PDF File Name: ' + (unprocessedPart.fileName || 'Unknown') + '\n';
            emailBody += 'Processing Status: ' + (unprocessedPart.success ? 'SKIPPED' : 'FAILED') + '\n';
            emailBody += 'Processing Date: ' + new Date().toString() + '\n\n';

            if (unprocessedPart.success && unprocessedPart.isSkipped) {
                emailBody += 'SKIP DETAILS:\n';
                emailBody += '=============\n';
                emailBody += 'Skip Reason: ' + (unprocessedPart.skipReason || 'Unknown reason') + '\n';
                emailBody += 'Skip Type: ' + (unprocessedPart.skipType || 'GENERAL') + '\n\n';

                // Add specific instructions based on skip type
                emailBody += 'RECOMMENDED ACTIONS:\n';
                emailBody += '===================\n';
                if (unprocessedPart.skipType === 'SHORT_SHIP') {
                    emailBody += '- Process as a Vendor Receivables Short Ship Credit\n';
                    emailBody += '- Use manual short ship credit processing procedures\n';
                } else if (unprocessedPart.skipType === 'UNIDENTIFIED_NARDA') {
                    emailBody += '- Review and identify the correct NARDA value\n';
                    emailBody += '- Determine appropriate processing method\n';
                } else if (unprocessedPart.skipType === 'NO_VRA_MATCH') {
                    emailBody += '- Verify original bill numbers are correct\n';
                    emailBody += '- Search for matching VRA manually\n';
                    emailBody += '- Create vendor credit manually if appropriate\n';
                } else if (unprocessedPart.skipType === 'MISSING_BILL_NUMBERS') {
                    emailBody += '- Review PDF for original bill number information\n';
                    emailBody += '- Extract bill numbers manually\n';
                    emailBody += '- Process manually with correct bill numbers\n';
                } else {
                    emailBody += '- Review PDF content and processing logs\n';
                    emailBody += '- Determine appropriate manual processing method\n';
                }
                emailBody += '\n';
            } else {
                emailBody += 'FAILURE DETAILS:\n';
                emailBody += '===============\n';
                emailBody += 'Error: ' + (unprocessedPart.error || 'Unknown error') + '\n';

                if (unprocessedPart.isDuplicate) {
                    emailBody += 'Failure Type: DUPLICATE TRANSACTION\n';
                    if (unprocessedPart.existingJournalEntry) {
                        emailBody += 'Existing Journal Entry ID: ' + unprocessedPart.existingJournalEntry.internalId + '\n';
                    }
                    if (unprocessedPart.existingVendorCredit) {
                        emailBody += 'Existing Vendor Credit ID: ' + unprocessedPart.existingVendorCredit.internalId + '\n';
                    }
                } else {
                    emailBody += 'Failure Type: PROCESSING ERROR\n';
                }
                emailBody += '\n';

                emailBody += 'RECOMMENDED ACTIONS:\n';
                emailBody += '===================\n';
                if (unprocessedPart.isDuplicate) {
                    emailBody += '- Review existing transaction to confirm it is correct\n';
                    emailBody += '- If existing transaction is incorrect, delete and reprocess\n';
                    emailBody += '- If existing transaction is correct, no further action needed\n';
                } else {
                    emailBody += '- Review processing error details\n';
                    emailBody += '- Manually process PDF using standard procedures\n';
                    emailBody += '- Check for data extraction issues\n';
                }
                emailBody += '\n';
            }

            // Add extracted data if available
            if (unprocessedPart.extractedData) {
                emailBody += 'EXTRACTED DATA:\n';
                emailBody += '==============\n';
                emailBody += 'NARDA Number: ' + (unprocessedPart.extractedData.nardaNumber || 'Not found') + '\n';
                emailBody += 'Invoice Number: ' + (unprocessedPart.extractedData.invoiceNumber || 'Not found') + '\n';
                emailBody += 'Invoice Date: ' + (unprocessedPart.extractedData.invoiceDate || 'Not found') + '\n';
                emailBody += 'Total Amount: ' + (unprocessedPart.extractedData.totalAmount || 'Not found') + '\n';
                emailBody += 'Delivery Amount: ' + (unprocessedPart.extractedData.deliveryAmount || '$0.00') + '\n';
                if (unprocessedPart.extractedData.originalBillNumber) {
                    emailBody += 'Original Bill Number: ' + unprocessedPart.extractedData.originalBillNumber + '\n';
                }
                emailBody += '\n';
            }

            // Add VRA match information if available
            if (unprocessedPart.matchingVRA) {
                emailBody += 'MATCHING VRA INFORMATION:\n';
                emailBody += '========================\n';
                emailBody += 'VRA Internal ID: ' + unprocessedPart.matchingVRA.internalId + '\n';
                emailBody += 'VRA Transaction ID: ' + unprocessedPart.matchingVRA.tranid + '\n';
                emailBody += 'VRA Memo: ' + (unprocessedPart.matchingVRA.memo || 'None') + '\n';
                emailBody += '\n';
            }

            emailBody += 'NEXT STEPS:\n';
            emailBody += '==========\n';
            emailBody += '1. Review the attached PDF file\n';
            emailBody += '2. Follow the recommended actions above\n';
            emailBody += '3. Process manually using standard procedures\n';
            emailBody += '4. Update systems as appropriate\n\n';

            emailBody += 'This email was automatically generated by:\n';
            emailBody += 'Marcone Product Warranty Bill Credit Processing Script\n';
            emailBody += 'Original processing attempt: ' + new Date().toString();

            // Get the PDF file to attach
            var attachmentFile = null;
            if (unprocessedPart.fileId) {
                try {
                    attachmentFile = file.load({
                        id: unprocessedPart.fileId
                    });
                    log.debug('Loaded PDF file for ZoneCapture email attachment', {
                        fileId: unprocessedPart.fileId,
                        fileName: attachmentFile.name,
                        fileType: attachmentFile.fileType
                    });
                } catch (fileError) {
                    log.error('Could not load PDF file for ZoneCapture email attachment', {
                        error: fileError.toString(),
                        fileId: unprocessedPart.fileId,
                        unprocessedPartFileName: unprocessedPart.fileName
                    });
                }
            }

            // Send email with attachment
            var emailOptions = {
                author: 151135, // Sender ID
                recipients: [zoneCaptureEmail],
                subject: emailSubject,
                body: emailBody
            };

            // Add attachment if available
            if (attachmentFile) {
                emailOptions.attachments = [attachmentFile];
            }

            email.send(emailOptions);

            log.debug('Unprocessed part sent to ZoneCapture successfully', {
                zoneCaptureEmail: zoneCaptureEmail,
                invoiceNumber: invoiceNumber,
                fileName: unprocessedPart.fileName,
                fileId: unprocessedPart.fileId,
                attachmentIncluded: !!attachmentFile,
                originalRecordId: originalRecordId
            });

            return {
                success: true,
                zoneCaptureEmail: zoneCaptureEmail,
                invoiceNumber: invoiceNumber,
                attachmentIncluded: !!attachmentFile
            };

        } catch (error) {
            log.error('Error sending unprocessed part to ZoneCapture', {
                error: error.toString(),
                unprocessedPartFileName: unprocessedPart.fileName,
                originalRecordId: originalRecordId
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    function deactivateCustomRecord(recordId, processingResults) {
        // Replace the old deactivation logic with memo update, which will inactivate if all split PDF's from the custom record have processed
        return updateCustomRecordMemo(recordId, processingResults);
    }

    function savePDFToFailedFolder(originalFileId, originalFileName, recordId, extractedData) {
        try {
            log.debug('Saving failed PDF to folder ' + CONFIG.FOLDERS.FAILED, {
                originalFileId: originalFileId,
                originalFileName: originalFileName,
                recordId: recordId,
                targetFolderId: CONFIG.FOLDERS.FAILED,
                extractedData: extractedData
            });

            // Load the original PDF file
            var originalFile = file.load({
                id: originalFileId
            });

            // Create a new file name - use invoice number if available, otherwise use original name
            var timestamp = new Date().getTime();
            var fileExtension = originalFileName.substring(originalFileName.lastIndexOf('.'));
            var baseName;

            if (extractedData && extractedData.invoiceNumber) {
                // Use invoice number as the base name
                baseName = extractedData.invoiceNumber;
                log.debug('Using invoice number for failed PDF filename', {
                    invoiceNumber: extractedData.invoiceNumber,
                    originalFileName: originalFileName
                });
            } else {
                // Fall back to original filename
                baseName = originalFileName.substring(0, originalFileName.lastIndexOf('.'));
                log.debug('Using original filename for failed PDF (no invoice number available)', {
                    originalFileName: originalFileName
                });
            }

            var failedFileName = 'FAILED_' + baseName + '_' + timestamp + fileExtension;

            // Create new file in the failed folder (2466592)
            var failedFile = file.create({
                name: failedFileName,
                fileType: originalFile.fileType,
                contents: originalFile.getContents(),
                folder: CONFIG.FOLDERS.FAILED, // Failed PDFs folder
                isOnline: false // Failed files don't need to be public
            });

            // Save the failed file
            var failedFileId = failedFile.save();

            log.debug('Failed PDF saved successfully to folder ' + CONFIG.FOLDERS.FAILED, {
                originalFileId: originalFileId,
                originalFileName: originalFileName,
                failedFileId: failedFileId,
                failedFileName: failedFileName,
                targetFolderId: CONFIG.FOLDERS.FAILED,
                recordId: recordId,
                usedInvoiceNumber: !!(extractedData && extractedData.invoiceNumber)
            });

            return {
                success: true,
                fileId: failedFileId,
                fileName: failedFileName,
                folderId: CONFIG.FOLDERS.FAILED
            };

        } catch (error) {
            log.error('Error saving failed PDF to folder ' + CONFIG.FOLDERS.FAILED, {
                error: error.toString(),
                originalFileId: originalFileId,
                originalFileName: originalFileName,
                targetFolderId: CONFIG.FOLDERS.FAILED,
                recordId: recordId
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    /* OLD CODE, PRESERVED FOR HISTORIC REFERENCE function transformVRAToVendorCredit
    function transformVRAToVendorCredit(vraInternalId, extractedData, pdfFileId, recordId, targetLineNumber) {
        try {
            log.debug('Transforming VRA to Vendor Credit', {
                vraInternalId: vraInternalId,
                extractedInvoiceNumber: extractedData.invoiceNumber,
                extractedInvoiceDate: extractedData.invoiceDate,
                extractedTotalAmount: extractedData.totalAmount,
                extractedOriginalBillNumber: extractedData.originalBillNumber,
                extractedDeliveryAmount: extractedData.deliveryAmount,
                targetLineNumber: targetLineNumber,
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

            // Load the VRA to get transaction details
            var vraRecord = record.load({
                type: record.Type.VENDOR_RETURN_AUTHORIZATION,
                id: vraInternalId,
                isDynamic: false
            });

            // Transform VRA to Vendor Credit
            var vendorCredit = record.transform({
                fromType: record.Type.VENDOR_RETURN_AUTHORIZATION,
                fromId: vraInternalId,
                toType: record.Type.VENDOR_CREDIT,
                isDynamic: true
            });

            // Set header fields
            vendorCredit.setValue({
                fieldId: 'tranid',
                value: extractedData.invoiceNumber
            });

            vendorCredit.setValue({
                fieldId: 'trandate',
                value: vcDate
            });

            // Set memo to include reference information
            var vcMemo = 'CONCDA Credit - ' + extractedData.invoiceNumber + ' - VRA: ' + vraRecord.getValue('tranid');
            vendorCredit.setValue({
                fieldId: 'memo',
                value: vcMemo
            });

            // If we have a specific target line, remove all other lines
            if (targetLineNumber !== undefined && targetLineNumber !== null) {
                var vcLineCount = vendorCredit.getLineCount({ sublistId: 'item' });

                log.debug('Removing non-matching lines from Vendor Credit', {
                    totalLines: vcLineCount,
                    targetLineNumber: targetLineNumber,
                    vraInternalId: vraInternalId
                });

                // Remove lines in reverse order to avoid index shifting issues
                for (var j = vcLineCount - 1; j >= 0; j--) {
                    var currentLineKey = vendorCredit.getSublistValue({
                        sublistId: 'item',
                        fieldId: 'line',
                        line: j
                    });

                    // If this is not our target line, remove it
                    if (currentLineKey != targetLineNumber) {
                        vendorCredit.removeLine({
                            sublistId: 'item',
                            line: j
                        });

                        log.debug('Removed VRA line from Vendor Credit', {
                            removedLineIndex: j,
                            removedLineKey: currentLineKey,
                            keepingLineKey: targetLineNumber
                        });
                    }
                }
            }



            // Add delivery amount as expense line if it exists and is not $0.00
            if (extractedData.deliveryAmount && extractedData.deliveryAmount !== '$0.00') {
                try {
                    // Parse delivery amount - remove $ and convert to positive number
                    var deliveryAmountValue = parseFloat(extractedData.deliveryAmount.replace(/[$(),]/g, ''));

                    if (!isNaN(deliveryAmountValue) && deliveryAmountValue > 0) {
                        log.debug('Adding delivery amount as expense line', {
                            deliveryAmount: extractedData.deliveryAmount,
                            parsedAmount: deliveryAmountValue,
                            account: 367,
                            department: 13,
                            vraInternalId: vraInternalId
                        });

                        // Add expense line for delivery
                        vendorCredit.selectNewLine({
                            sublistId: 'expense'
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'account',
                            value: 367 // FREIGHT IN account
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'amount',
                            value: deliveryAmountValue // Positive value for expense
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'department',
                            value: 13 // Service department
                        });

                        vendorCredit.setCurrentSublistValue({
                            sublistId: 'expense',
                            fieldId: 'memo',
                            value: 'Delivery'
                        });

                        vendorCredit.commitLine({
                            sublistId: 'expense'
                        });

                        log.debug('Delivery expense line added successfully', {
                            account: 367,
                            amount: deliveryAmountValue,
                            department: 13,
                            memo: 'Delivery',
                            vraInternalId: vraInternalId
                        });
                    } else {
                        log.debug('Delivery amount is zero or invalid, skipping expense line', {
                            deliveryAmount: extractedData.deliveryAmount,
                            parsedAmount: deliveryAmountValue,
                            vraInternalId: vraInternalId
                        });
                    }
                } catch (deliveryError) {
                    log.error('Error adding delivery expense line (continuing with vendor credit creation)', {
                        error: deliveryError.toString(),
                        deliveryAmount: extractedData.deliveryAmount,
                        vraInternalId: vraInternalId
                    });
                    // Continue with vendor credit creation even if delivery line fails
                }
            } else {
                log.debug('No delivery amount to add or delivery amount is $0.00', {
                    deliveryAmount: extractedData.deliveryAmount,
                    vraInternalId: vraInternalId
                });
            }


            // Save the vendor credit
            var vendorCreditId = vendorCredit.save();

            log.debug('Vendor Credit created successfully from VRA', {
                vendorCreditId: vendorCreditId,
                vendorCreditTranid: extractedData.invoiceNumber,
                vraInternalId: vraInternalId,
                targetLineNumber: targetLineNumber,
                memo: vcMemo,
                deliveryAmountAdded: extractedData.deliveryAmount && extractedData.deliveryAmount !== '$0.00',
                recordId: recordId
            });


            // Attach the PDF file to the vendor credit
            var attachResult = attachFileToRecord(vendorCreditId, pdfFileId, recordId, record.Type.VENDOR_CREDIT);

            return {
                success: true,
                vendorCreditId: vendorCreditId,
                vendorCreditTranid: extractedData.invoiceNumber,
                attachmentResult: attachResult
            };

        } catch (error) {
            log.error('Error transforming VRA to Vendor Credit', {
                error: error.toString(),
                vraInternalId: vraInternalId,
                extractedData: extractedData,
                targetLineNumber: targetLineNumber,
                recordId: recordId
            });
            return {
                success: false,
                error: error.toString()
            };
        }
    }
    */

    /* OLD CODE, PRESERVED FOR HISTORIC REFERENCE function createJournalEntriesFromLineItems
    function createJournalEntriesFromLineItems(splitPart, recordId) {
        try {
            // Get extracted line items from JSON results
            var extractedData = null;
            if (splitPart.jsonResult && splitPart.jsonResult.success && splitPart.jsonResult.extractedData) {
                extractedData = splitPart.jsonResult.extractedData;
            }

            if (!extractedData || !extractedData.success || !extractedData.groupedLineItems) {
                return { success: false, error: 'Missing required line item data from JSON results' };
            }

            var journalEntryResults = [];
            var vendorCreditResults = [];
            var skippedResults = [];

            // Process each NARDA group separately
            var nardaGroups = Object.keys(extractedData.groupedLineItems);

            for (var i = 0; i < nardaGroups.length; i++) {
                var nardaNumber = nardaGroups[i];
                var nardaGroup = extractedData.groupedLineItems[nardaNumber];

                log.debug('Processing NARDA group', {
                    nardaNumber: nardaNumber,
                    lineItemCount: nardaGroup.lineItems.length,
                    totalAmount: nardaGroup.totalAmount,
                    fileName: splitPart.fileName
                });

                // Create a pseudo-extractedData object for this NARDA group
                var nardaExtractedData = {
                    nardaNumber: nardaNumber,
                    totalAmount: '($' + nardaGroup.totalAmount.toFixed(2) + ')', // Format as currency
                    invoiceDate: extractedData.invoiceDate,
                    invoiceNumber: extractedData.invoiceNumber,
                    deliveryAmount: extractedData.deliveryAmount,
                    lineItems: nardaGroup.lineItems,
                    originalBillNumber: null // Will need to extract this if needed for CONCDA
                };

                // Use existing journal entry creation logic for each NARDA group
                var jeResult = createJournalEntryForNARDAGroup(splitPart, recordId, nardaExtractedData, i + 1);

                if (jeResult.success) {
                    if (jeResult.isVendorCredit) {
                        vendorCreditResults.push(jeResult);
                    } else if (jeResult.isSkipped) {
                        skippedResults.push(jeResult);
                    } else {
                        journalEntryResults.push(jeResult);
                    }
                } else {
                    // Handle failed journal entry
                    return jeResult; // Return the error
                }
            }

            return {
                success: true,
                journalEntries: journalEntryResults,
                vendorCredits: vendorCreditResults,
                skippedEntries: skippedResults,
                totalNARDAGroups: nardaGroups.length
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
    */

    function searchForMatchingVRA(originalBillNumber, recordId) {
        try {
            log.debug('Searching for matching VRA records by line memo', {
                originalBillNumber: originalBillNumber,
                recordId: recordId
            });

            var vraResults = [];

            // Use the original working approach - search line memo field
            // REMOVED: Status filters to see all VRA statuses
            var vraSearch = search.create({
                type: search.Type.VENDOR_RETURN_AUTHORIZATION,
                filters: [
                    ['type', 'anyof', 'VendAuth'],
                    'AND',
                    ['memo', 'contains', originalBillNumber] // Search line memo field
                ],
                columns: [
                    'internalid',
                    'tranid', // VRA transaction ID
                    'trandate',
                    'memo', // Line memo field
                    'entity',
                    'status',
                    'item',
                    'amount',
                    'line'
                ]
            });

            var searchResults = vraSearch.run();

            searchResults.each(function (result) {
                var lineMemo = result.getValue('memo');

                // Check if this line's memo contains our original bill number
                if (lineMemo && lineMemo.indexOf(originalBillNumber) !== -1) {
                    vraResults.push({
                        internalId: result.getValue('internalid'),
                        tranid: result.getValue('tranid'),
                        trandate: result.getValue('trandate'),
                        memo: lineMemo,
                        entity: result.getValue('entity'),
                        status: result.getValue('status'),
                        lineItem: result.getValue('item'),
                        amount: result.getValue('amount'),
                        lineNumber: result.getValue('line')
                    });

                    log.debug('VRA match found in line memo', {
                        vraInternalId: result.getValue('internalid'),
                        vraTransactionId: result.getValue('tranid'),
                        lineNumber: result.getValue('line'),
                        lineMemo: lineMemo,
                        amount: result.getValue('amount'),
                        status: result.getValue('status'), // Added status logging
                        originalBillNumber: originalBillNumber,
                        recordId: recordId
                    });
                }

                return true; // Continue processing results
            });

            log.debug('VRA search completed', {

                originalBillNumber: originalBillNumber,
                totalMatches: vraResults.length,
                recordId: recordId,
                vraTransactionIds: vraResults.map(function (vra) { return vra.tranid; }),
                vraStatuses: vraResults.map(function (vra) { return vra.status; }) // Added status logging
            });

            return vraResults;

        } catch (error) {
            log.error('Error searching for matching VRA', {
                error: error.toString(),
                originalBillNumber: originalBillNumber,
                recordId: recordId
            });
            return [];
        }
    }

    return {
        execute: execute
    };
});