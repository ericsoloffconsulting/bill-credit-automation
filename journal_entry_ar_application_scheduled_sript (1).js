/**
 * @NApiVersion 2.1
 * @NScriptType ScheduledScript
 */
define(['N/search', 'N/log', 'N/record', 'N/email', 'N/runtime'], function (search, log, record, email, runtime) {

    function execute(context) {
        try {
            log.debug('Script Start', 'Starting JE AR Invoice Application processing');

            // Load the saved search
            var savedSearchId = 'customsearch_je_ar_inv_application';
            var savedSearchResults = search.load({
                id: savedSearchId
            });

            var searchResultCount = 0;
            var totalMatches = 0;
            var totalPaymentsCreated = 0;
            var processedDetails = []; // Store details for email

            // Process each result from the saved search
            savedSearchResults.run().each(function (result) {
                searchResultCount++;

                // Get the memo field value
                var memo = result.getValue({
                    name: 'memo'
                });

                var transactionId = result.id;
                var transactionNumber = result.getValue({
                    name: 'tranid'
                }) || result.getValue({
                    name: 'number'
                });

                // Get the credit amount
                var creditAmount = parseFloat(result.getValue({
                    name: 'creditamount'
                })) || 0;

                log.debug('Processing Transaction', {
                    id: transactionId,
                    number: transactionNumber,
                    memo: memo,
                    creditAmount: creditAmount
                });

                if (memo) {
                    // Split memo by words and find words starting with "J"
                    var words = memo.split(/\s+/);
                    var jobIds = [];

                    // First, look for words starting with "J"
                    for (var i = 0; i < words.length; i++) {
                        var word = words[i].trim();
                        if (word.charAt(0).toLowerCase() === 'j') {
                            jobIds.push(word);
                        }
                    }

                    // If no "J" words found, look for words starting with "INV"
                    if (jobIds.length === 0) {
                        log.debug('No Job IDs starting with "J" found, searching for words starting with "INV"', {
                            transactionId: transactionId,
                            memo: memo
                        });

                        for (var i = 0; i < words.length; i++) {
                            var word = words[i].trim();
                            if (word.toLowerCase().indexOf('inv') === 0) {
                                jobIds.push(word);
                            }
                        }
                    }

                    log.debug('Found Job IDs', {
                        transactionId: transactionId,
                        jobIds: jobIds,
                        searchMethod: jobIds.length > 0 ? (jobIds[0].charAt(0).toLowerCase() === 'j' ? 'J prefix' : 'INV prefix') : 'none found'
                    });

                    // For each Job ID found, search for matching open invoices
                    for (var j = 0; j < jobIds.length; j++) {
                        var jobId = jobIds[j];
                        var matchingInvoices = findMatchingOpenInvoices(jobId);

                        if (matchingInvoices.length > 0) {
                            totalMatches += matchingInvoices.length;
                            log.debug('MATCH FOUND', {
                                sourceTransaction: transactionNumber,
                                sourceTransactionId: transactionId,
                                jobId: jobId,
                                matchingInvoices: matchingInvoices
                            });

                            // Create customer payment for each matching invoice
                            for (var k = 0; k < matchingInvoices.length; k++) {
                                var invoice = matchingInvoices[k];
                                try {
                                    var paymentResult = createCustomerPayment(invoice, transactionId, creditAmount);
                                    if (paymentResult && paymentResult.success) {
                                        totalPaymentsCreated++;

                                        // Store details for email
                                        processedDetails.push({
                                            sourceTransaction: transactionNumber,
                                            sourceTransactionId: transactionId,
                                            creditAmount: creditAmount,
                                            jobId: jobId,
                                            invoiceId: invoice.internalId,
                                            invoiceNumber: invoice.tranId,
                                            invoiceAmount: invoice.amountDue,
                                            customer: invoice.entityText,
                                            appliedAmount: paymentResult.appliedAmount,
                                            paymentDeleted: !paymentResult.deletionFailed,
                                            deletedPaymentId: paymentResult.deletedPaymentId,
                                            paymentId: paymentResult.paymentId // Only present if deletion failed
                                        });

                                        log.debug('Credit Application Complete', {
                                            appliedAmount: paymentResult.appliedAmount,
                                            invoiceId: invoice.internalId,
                                            invoiceNumber: invoice.tranId,
                                            sourceTransaction: transactionNumber,
                                            paymentDeleted: !paymentResult.deletionFailed,
                                            deletedPaymentId: paymentResult.deletedPaymentId
                                        });
                                    }
                                } catch (paymentError) {
                                    log.error('Error creating payment', {
                                        error: paymentError,
                                        invoice: invoice,
                                        sourceTransaction: transactionNumber
                                    });
                                }
                            }
                        } else {
                            log.debug('No Matches', {
                                sourceTransaction: transactionNumber,
                                jobId: jobId
                            });
                        }
                    }
                }

                return true; // Continue processing
            });

            log.debug('Script Complete', {
                totalTransactionsProcessed: searchResultCount,
                totalMatches: totalMatches,
                totalPaymentsCreated: totalPaymentsCreated
            });

            // Send email with results
            sendResultsEmail(searchResultCount, totalMatches, totalPaymentsCreated, processedDetails);

        } catch (error) {
            log.error('Script Error', error);
        }
    }

    function sendResultsEmail(transactionsProcessed, totalMatches, paymentsCreated, details) {
        try {
            // Get script parameters for email configuration
            var script = runtime.getCurrentScript();
            var recipientEmail = script.getParameter({
                name: 'custscript_bas_je_ar_appl_recipient'
            });
            var copyRecipientEmail = script.getParameter({
                name: 'custscript_bas_je_ar_appl_recipient_copy'
            });

            // Build email recipients array
            var recipients = [];
            if (recipientEmail) {
                recipients.push(recipientEmail);
            }

            var ccRecipients = [];
            if (copyRecipientEmail) {
                ccRecipients.push(copyRecipientEmail);
            }

            var emailBody = 'Journal Entry AR Invoice Application Processing Complete\n\n';
            emailBody += 'SUMMARY:\n';
            emailBody += '- Total Journal Entry transactions processed: ' + transactionsProcessed + '\n';
            emailBody += '- Total invoice matches found: ' + totalMatches + '\n';
            emailBody += '- Total customer payments created: ' + paymentsCreated + '\n\n';

            if (details.length > 0) {
                emailBody += 'DETAILED RESULTS:\n';
                emailBody += '================\n\n';

                for (var i = 0; i < details.length; i++) {
                    var detail = details[i];
                    emailBody += 'APPLICATION #' + (i + 1) + ':\n';
                    emailBody += '  Source JE Transaction: ' + detail.sourceTransaction + ' (ID: ' + detail.sourceTransactionId + ')\n';
                    emailBody += '  Credit Amount Available: $' + detail.creditAmount.toFixed(2) + '\n';
                    emailBody += '  Job ID Match: ' + detail.jobId + '\n';
                    emailBody += '  Target Invoice: ' + detail.invoiceNumber + ' (ID: ' + detail.invoiceId + ')\n';
                    emailBody += '  Customer: ' + detail.customer + '\n';
                    emailBody += '  Invoice Amount Due: $' + detail.invoiceAmount.toFixed(2) + '\n';
                    emailBody += '  Amount Applied: $' + detail.appliedAmount.toFixed(2) + '\n';
                    
                    if (detail.paymentDeleted) {
                        emailBody += '  Temporary Payment: Created and deleted (ID: ' + detail.deletedPaymentId + ')\n';
                        emailBody += '  Net Effect: $0.00 (Credit applied directly)\n\n';
                    } else {
                        emailBody += '  Payment Created: ID ' + detail.paymentId + ' (Deletion failed)\n';
                        emailBody += '  Net Effect: $0.00 (Credit offset by payment)\n\n';
                    }
                }
            } else {
                emailBody += 'No applications were created during this run.\n\n';
            }

            emailBody += 'This automated process matches Job IDs found in Journal Entry memos with open invoices and creates customer payments that net to zero.\n';
            emailBody += 'Process completed at: ' + new Date().toString();

            // Build email options
            var emailOptions = {
                author: 151135,
                recipients: recipients,
                subject: 'Results: Journal Entries Applied to A/R Invoices',
                body: emailBody
            };

            // Add CC recipients if specified
            if (ccRecipients.length > 0) {
                emailOptions.cc = ccRecipients;
            }

            // Send email only if we have recipients
            if (recipients.length > 0) {
                email.send(emailOptions);

                log.debug('Email sent successfully', {
                    recipients: recipients,
                    ccRecipients: ccRecipients,
                    detailCount: details.length
                });
            } else {
                log.debug('No email sent - no recipients configured', {
                    recipientParam: 'custscript_bas_je_ar_appl_recipient',
                    copyRecipientParam: 'custscript_bas_je_ar_appl_recipient_copy'
                });
            }

        } catch (emailError) {
            log.error('Error sending email', {
                error: emailError.toString(),
                recipients: recipients,
                ccRecipients: ccRecipients
            });
        }
    }

    function findMatchingOpenInvoices(jobId) {
        try {
            var invoiceSearch = search.create({
                type: search.Type.INVOICE,
                filters: [
                    [
                        ['custbody_f4n_job_id', 'is', jobId],
                        'OR',
                        ['tranid', 'is', jobId]
                    ],
                    'AND',
                    ['status', 'anyof', 'CustInvc:A'], // Open status
                    'AND',
                    ['mainline', 'is', true] // Only main line
                ],
                columns: [
                    'tranid',
                    'internalid',
                    'entity',
                    'total',
                    'amountremaining',
                    'custbody_f4n_job_id'
                ]
            });

            var matches = [];
            invoiceSearch.run().each(function (result) {
                var amountRemaining = parseFloat(result.getValue('amountremaining')) || 0;
                var totalAmount = parseFloat(result.getValue('total')) || 0;

                matches.push({
                    internalId: result.id,
                    tranId: result.getValue('tranid'),
                    entity: result.getValue('entity'), // Get entity ID, not text
                    entityText: result.getText('entity'),
                    totalAmount: totalAmount,
                    amountDue: amountRemaining,
                    jobId: result.getValue('custbody_f4n_job_id')
                });

                return true; // Continue processing (up to 4000 results)
            });

            return matches;

        } catch (error) {
            log.error('Error searching invoices for Job ID: ' + jobId, error);
            return [];
        }
    }

    function createCustomerPayment(invoice, sourceTransactionId, creditAmount) {
        try {
            // Transform the invoice into a customer payment
            var customerPayment = record.transform({
                fromType: record.Type.INVOICE,
                fromId: invoice.internalId,
                toType: record.Type.CUSTOMER_PAYMENT
            });

            log.debug('Transformed invoice to payment', {
                invoiceId: invoice.internalId,
                invoiceTranId: invoice.tranId
            });

            // Set basic payment fields
            customerPayment.setValue({
                fieldId: 'trandate',
                value: new Date()
            });

            customerPayment.setValue({
                fieldId: 'paymentmethod',
                value: 15 // ACCT'G payment method
            });

            customerPayment.setValue({
                fieldId: 'memo',
                value: 'Auto-applied from JE transaction ' + sourceTransactionId + ' for Job ID match - TEMP RECORD TO BE DELETED'
            });

            // Calculate the maximum amount we can apply (limited by both credit and invoice amounts)
            var maxApplicationAmount = Math.min(creditAmount, invoice.amountDue);

            log.debug('Application amount calculation', {
                creditAmount: creditAmount,
                invoiceAmountDue: invoice.amountDue,
                maxApplicationAmount: maxApplicationAmount
            });

            // Set payment amount to maxApplicationAmount for proper netting
            customerPayment.setValue({
                fieldId: 'payment',
                value: maxApplicationAmount
            });

            // STEP 1: CLEAR ALL AUTO-SELECTED APPLY LINES FIRST
            var applyLineCount = customerPayment.getLineCount({
                sublistId: 'apply'
            });

            log.debug('Clearing auto-selected apply lines', {
                applyLineCount: applyLineCount
            });

            // Clear all apply lines that were auto-selected by transform
            for (var j = 0; j < applyLineCount; j++) {
                try {
                    var isApplied = customerPayment.getSublistValue({
                        sublistId: 'apply',
                        fieldId: 'apply',
                        line: j
                    });

                    if (isApplied) {
                        log.debug('Clearing auto-selected apply line', {
                            line: j,
                            wasApplied: isApplied
                        });

                        customerPayment.setSublistValue({
                            sublistId: 'apply',
                            fieldId: 'apply',
                            line: j,
                            value: false
                        });

                        // Also clear the amount
                        customerPayment.setSublistValue({
                            sublistId: 'apply',
                            fieldId: 'amount',
                            line: j,
                            value: 0
                        });
                    }
                } catch (clearError) {
                    log.debug('Could not clear apply line', {
                        line: j,
                        error: clearError.toString()
                    });
                }
            }

            // STEP 2: Find and select the credit transaction first
            var creditLineCount = 0;
            var creditLineUpdated = false;
            var actualCreditAmount = 0;

            try {
                creditLineCount = customerPayment.getLineCount({
                    sublistId: 'credit'
                });
                log.debug('Credit sublist found', {
                    lineCount: creditLineCount
                });
            } catch (creditError) {
                log.debug('Credit sublist not found or empty');
            }

            if (creditLineCount > 0) {
                log.debug('Selecting credit transaction', {
                    sourceTransactionId: sourceTransactionId,
                    creditLineCount: creditLineCount,
                    targetAmount: maxApplicationAmount
                });

                for (var c = 0; c < creditLineCount; c++) {
                    var creditDocId = customerPayment.getSublistValue({
                        sublistId: 'credit',
                        fieldId: 'doc',
                        line: c
                    });

                    var creditRefNum = customerPayment.getSublistValue({
                        sublistId: 'credit',
                        fieldId: 'refnum',
                        line: c
                    });

                    log.debug('Checking credit line', {
                        line: c,
                        creditDocId: creditDocId,
                        creditRefNum: creditRefNum,
                        targetTransactionId: sourceTransactionId
                    });

                    if (creditDocId == sourceTransactionId || creditRefNum == sourceTransactionId) {
                        try {
                            // Apply the credit
                            customerPayment.setSublistValue({
                                sublistId: 'credit',
                                fieldId: 'apply',
                                line: c,
                                value: true
                            });

                            // Set the amount
                            customerPayment.setSublistValue({
                                sublistId: 'credit',
                                fieldId: 'amount',
                                line: c,
                                value: maxApplicationAmount
                            });

                            // Verify the credit amount was set correctly
                            var updatedCreditAmount = customerPayment.getSublistValue({
                                sublistId: 'credit',
                                fieldId: 'amount',
                                line: c
                            });

                            actualCreditAmount = updatedCreditAmount;
                            creditLineUpdated = true;

                            log.debug('Selected credit transaction', {
                                line: c,
                                creditDocId: creditDocId,
                                creditRefNum: creditRefNum,
                                targetAmount: maxApplicationAmount,
                                actualCreditAmount: updatedCreditAmount,
                                success: (updatedCreditAmount == maxApplicationAmount)
                            });

                        } catch (creditSetError) {
                            log.error('Error setting credit line', {
                                error: creditSetError.toString(),
                                line: c,
                                creditDocId: creditDocId
                            });
                        }
                        break; // Exit once we find the source transaction
                    }
                }
            }

            // STEP 3: Now select the invoice with the SAME amount as the credit
            var invoiceLineUpdated = false;
            var actualApplyAmount = 0;

            // Refresh apply line count after clearing
            applyLineCount = customerPayment.getLineCount({
                sublistId: 'apply'
            });

            log.debug('Selecting invoice for application', {
                targetInvoiceId: invoice.internalId,
                targetAmount: maxApplicationAmount,
                applyLineCount: applyLineCount
            });

            for (var j = 0; j < applyLineCount; j++) {
                var docId = customerPayment.getSublistValue({
                    sublistId: 'apply',
                    fieldId: 'doc',
                    line: j
                });

                var refNum = customerPayment.getSublistValue({
                    sublistId: 'apply',
                    fieldId: 'refnum',
                    line: j
                });

                // Match the invoice by its internal ID
                if (docId == invoice.internalId) {
                    try {
                        log.debug('Selecting invoice line', {
                            line: j,
                            docId: docId,
                            refNum: refNum,
                            targetAmount: maxApplicationAmount
                        });

                        // Apply the invoice
                        customerPayment.setSublistValue({
                            sublistId: 'apply',
                            fieldId: 'apply',
                            line: j,
                            value: true
                        });

                        // Set the amount to match the credit amount exactly
                        customerPayment.setSublistValue({
                            sublistId: 'apply',
                            fieldId: 'amount',
                            line: j,
                            value: maxApplicationAmount
                        });

                        // Verify the amount was set correctly
                        var updatedAmount = customerPayment.getSublistValue({
                            sublistId: 'apply',
                            fieldId: 'amount',
                            line: j
                        });

                        actualApplyAmount = updatedAmount;
                        invoiceLineUpdated = true;

                        log.debug('Selected invoice for application', {
                            line: j,
                            docId: docId,
                            refNum: refNum,
                            targetAmount: maxApplicationAmount,
                            actualUpdatedAmount: updatedAmount,
                            success: (updatedAmount == maxApplicationAmount)
                        });

                    } catch (setError) {
                        log.error('Error selecting invoice line', {
                            error: setError.toString(),
                            line: j,
                            docId: docId
                        });
                    }
                    break; // Exit once we find the target invoice
                }
            }

            // STEP 4: CRITICAL VALIDATION - Ensure everything matches before saving
            var netEffect = actualApplyAmount - actualCreditAmount;
            var amountsMatch = (actualApplyAmount == actualCreditAmount) && (actualApplyAmount == maxApplicationAmount);

            log.debug('FINAL VALIDATION BEFORE SAVE', {
                expectedAmount: maxApplicationAmount,
                actualApplyAmount: actualApplyAmount,
                actualCreditAmount: actualCreditAmount,
                netEffect: netEffect,
                amountsMatch: amountsMatch,
                invoiceLineUpdated: invoiceLineUpdated,
                creditLineUpdated: creditLineUpdated,
                invoiceId: invoice.internalId,
                sourceTransactionId: sourceTransactionId
            });

            // VALIDATION CHECKS
            if (!invoiceLineUpdated) {
                log.error('VALIDATION FAILED - Could not select target invoice', {
                    targetInvoiceId: invoice.internalId,
                    applyLineCount: applyLineCount
                });
                return null;
            }

            if (!creditLineUpdated) {
                log.error('VALIDATION FAILED - Could not select credit transaction', {
                    sourceTransactionId: sourceTransactionId,
                    creditLineCount: creditLineCount
                });
                return null;
            }

            if (!amountsMatch) {
                log.error('VALIDATION FAILED - Amounts do not match', {
                    expectedAmount: maxApplicationAmount,
                    actualApplyAmount: actualApplyAmount,
                    actualCreditAmount: actualCreditAmount,
                    netEffect: netEffect
                });
                return null;
            }

            if (Math.abs(netEffect) > 0.01) { // Allow for minor rounding differences
                log.error('VALIDATION FAILED - Net effect is not zero', {
                    netEffect: netEffect,
                    actualApplyAmount: actualApplyAmount,
                    actualCreditAmount: actualCreditAmount
                });
                return null;
            }

            // STEP 5: SAVE THE PAYMENT TO APPLY THE CREDIT
            log.debug('ALL VALIDATIONS PASSED - Saving payment to apply credit', {
                paymentAmount: maxApplicationAmount,
                applyAmount: actualApplyAmount,
                creditAmount: actualCreditAmount,
                netEffect: netEffect,
                invoiceId: invoice.internalId,
                sourceTransactionId: sourceTransactionId
            });

            var paymentId = customerPayment.save();

            log.debug('Customer payment saved - credit applied successfully', {
                paymentId: paymentId,
                netEffect: '$0.00',
                appliedAmount: actualApplyAmount,
                creditAmount: actualCreditAmount,
                invoiceId: invoice.internalId,
                sourceTransactionId: sourceTransactionId
            });

            // STEP 6: DELETE THE PAYMENT RECORD SINCE IT'S NO LONGER NEEDED
            // The credit application has been processed, but we don't need the payment record
            try {
                log.debug('Deleting temporary payment record', {
                    paymentId: paymentId,
                    reason: 'Credit application complete - payment record not needed'
                });

                record.delete({
                    type: record.Type.CUSTOMER_PAYMENT,
                    id: paymentId
                });

                log.debug('Temporary payment record deleted successfully', {
                    deletedPaymentId: paymentId,
                    invoiceId: invoice.internalId,
                    sourceTransactionId: sourceTransactionId,
                    result: 'Credit applied without payment record'
                });

                // Return success indicator but not the payment ID since it's deleted
                return {
                    success: true,
                    appliedAmount: actualApplyAmount,
                    deletedPaymentId: paymentId,
                    invoiceId: invoice.internalId,
                    sourceTransactionId: sourceTransactionId
                };

            } catch (deleteError) {
                log.error('Error deleting temporary payment record', {
                    error: deleteError.toString(),
                    paymentId: paymentId,
                    invoiceId: invoice.internalId,
                    sourceTransactionId: sourceTransactionId
                });

                // Return the payment ID even if deletion failed so we can track it
                return {
                    success: true,
                    appliedAmount: actualApplyAmount,
                    paymentId: paymentId,
                    deletionFailed: true,
                    deleteError: deleteError.toString(),
                    invoiceId: invoice.internalId,
                    sourceTransactionId: sourceTransactionId
                };
            }

        } catch (error) {
            log.error('Error in createCustomerPayment', {
                error: error.toString(),
                invoice: invoice,
                sourceTransactionId: sourceTransactionId,
                creditAmount: creditAmount
            });
            return null;
        }
    }

    return {
        execute: execute
    };
});