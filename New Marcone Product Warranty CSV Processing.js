/**
 * @NApiVersion 2.1
 * @NScriptType ScheduledScript
 * @NAmdConfig /SuiteScripts/ericsoloffconsulting/JsLibraryConfig.json
 */
define(['N/search', 'N/log', 'N/file', 'N/record', 'N/email', 'N/runtime'], function (search, log, file, record, email, runtime) {

    var CONFIG = {
        FOLDERS: {
            CSV_SOURCE: 2676075,
            CSV_ATTACHMENTS: 2676076,
            CSV_PROCESSED: 2676077
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
            MAX_ORDERNOS_PER_RUN: 75,
            MIN_GOVERNANCE_RESERVE: 100
        },
        NARDA_PATTERNS: {
            JOURNAL_ENTRY: /^(J\d+|INV\d+)$/i,
            VENDOR_CREDIT: /^(CONCDA|CONCDAM|NF|CORE|CONCESSION)$/i,
            SKIP: /^(SHORT|BOX|REBATE)$/i
        }
    };

    /**
     * Main scheduled script execution function
     * Processes CSV file containing Marcone warranty credit data
     * @param {Object} context - Script execution context
     */
    function execute(context) {
        try {
            // GOVERNANCE: Track starting governance
            var startingGovernance = runtime.getCurrentScript().getRemainingUsage();

            log.audit('Script Start', {
                message: 'Beginning CSV Processing for Marcone Warranty Credits',
                startingGovernance: startingGovernance
            });

            // Initialize tracking variables with governance tracking
            var stats = {
                totalOrderNos: 0,
                processedOrderNos: 0,
                journalEntriesCreated: 0,
                vendorCreditsCreated: 0,
                validationFailures: 0,
                skippedTransactions: 0,
                processedDetails: [],
                failedEntries: [],
                skippedEntries: [],
                parsedData: null,
                // GOVERNANCE: Add governance tracking
                governance: {
                    starting: startingGovernance,
                    afterCSVLoad: 0,
                    perOrderNo: [],
                    perJournalEntry: [],
                    perVendorCredit: [],
                    afterFileOperations: 0,
                    ending: 0,
                    totalUsed: 0
                }
            };

            // Get script parameter for CSV file ID
            var csvFileId = runtime.getCurrentScript().getParameter({
                name: 'custscript_csv_file_id'
            });

            if (!csvFileId) {
                log.error('Missing Parameter', 'CSV file ID parameter not provided');
                return;
            }

            log.debug('Script Parameters', {
                csvFileId: csvFileId,
                maxOrderNos: CONFIG.LIMITS.MAX_ORDERNOS_PER_RUN
            });

            // Load and parse CSV file
            var parsedData = loadAndParseCSV(csvFileId);

            // GOVERNANCE: Track after CSV load
            stats.governance.afterCSVLoad = runtime.getCurrentScript().getRemainingUsage();
            var csvLoadCost = startingGovernance - stats.governance.afterCSVLoad;

            log.debug('Governance After CSV Load', {
                remaining: stats.governance.afterCSVLoad,
                used: csvLoadCost
            });

            if (!parsedData.success) {
                log.error('CSV Load Failed', parsedData.error);
                return;
            }

            stats.parsedData = parsedData;

            log.audit('CSV Parsed Successfully', {
                totalRows: parsedData.totalRows,
                uniqueOrderNos: parsedData.uniqueOrderNos.length
            });

            stats.totalOrderNos = parsedData.uniqueOrderNos.length;

            // Determine OrderNos to process
            var orderNosToProcess = parsedData.uniqueOrderNos.slice(0, CONFIG.LIMITS.MAX_ORDERNOS_PER_RUN);
            var remainingOrderNos = parsedData.uniqueOrderNos.slice(CONFIG.LIMITS.MAX_ORDERNOS_PER_RUN);

            log.debug('Batch Planning', {
                totalOrderNos: stats.totalOrderNos,
                processingNow: orderNosToProcess.length,
                remaining: remainingOrderNos.length
            });

            // Process each OrderNo with governance tracking
            for (var i = 0; i < orderNosToProcess.length; i++) {
                var orderNo = orderNosToProcess[i];

                // GOVERNANCE: Check before processing
                var governanceBeforeOrder = runtime.getCurrentScript().getRemainingUsage();

                if (governanceBeforeOrder < CONFIG.LIMITS.MIN_GOVERNANCE_RESERVE) {
                    log.audit('Low Governance - Stopping Early', {
                        remainingUnits: governanceBeforeOrder,
                        processedSoFar: stats.processedOrderNos,
                        remainingInBatch: orderNosToProcess.length - i,
                        reserveThreshold: CONFIG.LIMITS.MIN_GOVERNANCE_RESERVE
                    });
                    break;
                }

                // Process this OrderNo
                var processResult = processOrderNo(parsedData, orderNo);

                // GOVERNANCE: Track after processing this order
                var governanceAfterOrder = runtime.getCurrentScript().getRemainingUsage();
                var orderCost = governanceBeforeOrder - governanceAfterOrder;

                stats.governance.perOrderNo.push({
                    orderNo: orderNo,
                    governanceBefore: governanceBeforeOrder,
                    governanceAfter: governanceAfterOrder,
                    governanceUsed: orderCost,
                    journalEntriesCreated: processResult.journalEntries ? processResult.journalEntries.length : 0,
                    vendorCreditsCreated: processResult.vendorCredits ? processResult.vendorCredits.length : 0
                });

                log.debug('Governance After OrderNo ' + orderNo, {
                    remaining: governanceAfterOrder,
                    usedForThisOrder: orderCost,
                    journalEntries: processResult.journalEntries ? processResult.journalEntries.length : 0,
                    vendorCredits: processResult.vendorCredits ? processResult.vendorCredits.length : 0
                });

                // Update statistics
                if (processResult.success) {
                    stats.processedOrderNos++;

                    if (processResult.journalEntries) {
                        stats.journalEntriesCreated += processResult.journalEntries.length;
                        stats.processedDetails = stats.processedDetails.concat(processResult.journalEntries);

                        // GOVERNANCE: Track per JE
                        for (var j = 0; j < processResult.journalEntries.length; j++) {
                            if (processResult.journalEntries[j].governanceUsed) {
                                stats.governance.perJournalEntry.push({
                                    orderNo: orderNo,
                                    tranid: processResult.journalEntries[j].tranid,
                                    governanceUsed: processResult.journalEntries[j].governanceUsed
                                });
                            }
                        }
                    }

                    if (processResult.vendorCredits) {
                        stats.vendorCreditsCreated += processResult.vendorCredits.length;
                        stats.processedDetails = stats.processedDetails.concat(processResult.vendorCredits);

                        // GOVERNANCE: Track per VC
                        for (var j = 0; j < processResult.vendorCredits.length; j++) {
                            if (processResult.vendorCredits[j].governanceUsed) {
                                stats.governance.perVendorCredit.push({
                                    orderNo: orderNo,
                                    tranid: processResult.vendorCredits[j].tranid,
                                    governanceUsed: processResult.vendorCredits[j].governanceUsed
                                });
                            }
                        }
                    }

                    if (processResult.skippedTransactions) {
                        stats.skippedTransactions += processResult.skippedTransactions.length;
                        stats.skippedEntries = stats.skippedEntries.concat(processResult.skippedTransactions);
                    }
                } else {
                    stats.failedEntries.push({
                        orderNo: orderNo,
                        error: processResult.error,
                        skipReason: processResult.skipReason
                    });

                    if (processResult.isValidationFailure) {
                        stats.validationFailures++;
                    }
                }
            }

            // GOVERNANCE: Track before file operations
            var governanceBeforeFiles = runtime.getCurrentScript().getRemainingUsage();

            // File management
            var unprocessedFileId = null;
            if (remainingOrderNos.length > 0) {
                unprocessedFileId = saveUnprocessedCSV(parsedData, remainingOrderNos);
                log.audit('Unprocessed CSV Saved', {
                    fileId: unprocessedFileId,
                    remainingOrderNos: remainingOrderNos.length
                });
            }

            moveProcessedCSV(csvFileId);

            // GOVERNANCE: Track after file operations
            stats.governance.afterFileOperations = runtime.getCurrentScript().getRemainingUsage();
            var fileOperationsCost = governanceBeforeFiles - stats.governance.afterFileOperations;

            log.debug('Governance After File Operations', {
                remaining: stats.governance.afterFileOperations,
                usedForFileOps: fileOperationsCost
            });

            // Send results email
            sendResultsEmail(stats, unprocessedFileId);

            // GOVERNANCE: Final governance tracking
            stats.governance.ending = runtime.getCurrentScript().getRemainingUsage();
            stats.governance.totalUsed = startingGovernance - stats.governance.ending;

            // Calculate averages
            var avgPerOrderNo = stats.governance.perOrderNo.length > 0 ?
                stats.governance.perOrderNo.reduce(function (sum, item) { return sum + item.governanceUsed; }, 0) / stats.governance.perOrderNo.length : 0;

            var avgPerJE = stats.governance.perJournalEntry.length > 0 ?
                stats.governance.perJournalEntry.reduce(function (sum, item) { return sum + item.governanceUsed; }, 0) / stats.governance.perJournalEntry.length : 0;

            var avgPerVC = stats.governance.perVendorCredit.length > 0 ?
                stats.governance.perVendorCredit.reduce(function (sum, item) { return sum + item.governanceUsed; }, 0) / stats.governance.perVendorCredit.length : 0;

            log.audit('Script Complete', {
                totalOrderNos: stats.totalOrderNos,
                processedOrderNos: stats.processedOrderNos,
                journalEntriesCreated: stats.journalEntriesCreated,
                vendorCreditsCreated: stats.vendorCreditsCreated,
                validationFailures: stats.validationFailures,
                skippedTransactions: stats.skippedTransactions,
                unprocessedRemaining: remainingOrderNos.length
            });

            // GOVERNANCE: Detailed final report
            log.audit('Governance Summary', {
                startingGovernance: startingGovernance,
                endingGovernance: stats.governance.ending,
                totalUsed: stats.governance.totalUsed,
                csvLoadCost: csvLoadCost,
                fileOperationsCost: fileOperationsCost,
                averagePerOrderNo: Math.round(avgPerOrderNo),
                averagePerJournalEntry: Math.round(avgPerJE),
                averagePerVendorCredit: Math.round(avgPerVC),
                totalOrderNosProcessed: stats.governance.perOrderNo.length,
                totalJournalEntriesCreated: stats.governance.perJournalEntry.length,
                totalVendorCreditsCreated: stats.governance.perVendorCredit.length
            });

            // GOVERNANCE: Detailed breakdown by transaction type
            if (stats.governance.perOrderNo.length > 0) {
                log.debug('Per-OrderNo Governance Details', {
                    orders: stats.governance.perOrderNo.map(function (item) {
                        return {
                            orderNo: item.orderNo,
                            used: item.governanceUsed,
                            jes: item.journalEntriesCreated,
                            vcs: item.vendorCreditsCreated
                        };
                    })
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
    }

    /**
     * Parse CSV line handling quoted values with commas
     * @param {string} line - CSV line to parse
     * @returns {Array} Array of field values
     */
    function parseCSVLine(line) {
        var result = [];
        var current = '';
        var inQuotes = false;

        for (var i = 0; i < line.length; i++) {
            var char = line.charAt(i);
            var nextChar = i + 1 < line.length ? line.charAt(i + 1) : '';

            if (char === '"') {
                if (inQuotes && nextChar === '"') {
                    // Escaped quote
                    current += '"';
                    i++; // Skip next quote
                } else {
                    // Toggle quote state
                    inQuotes = !inQuotes;
                }
            } else if (char === ',' && !inQuotes) {
                // End of field
                result.push(current.trim());
                current = '';
            } else {
                current += char;
            }
        }

        // Add last field
        result.push(current.trim());

        return result;
    }

    /**
     * Load and parse CSV file from NetSuite file cabinet
     * Uses string parsing with proper quote handling for CSV files
     * @param {number} fileId - NetSuite file ID
     * @returns {Object} Parsed CSV data structure
     */
    function loadAndParseCSV(fileId) {
        try {
            log.debug('Loading CSV File', { fileId: fileId });

            // Load file from NetSuite
            var fileObj = file.load({ id: fileId });
            var fileContents = fileObj.getContents();

            log.debug('CSV File Loaded', {
                fileName: fileObj.name,
                fileSize: fileObj.size
            });

            // Parse CSV string into structured data
            var lines = fileContents.split('\n');
            var headers = parseCSVLine(lines[0]);

            log.debug('CSV Headers Found', {
                headerCount: headers.length,
                headers: headers
            });

            // Parse all data rows
            var rows = [];
            for (var i = 1; i < lines.length; i++) {
                var line = lines[i].trim();
                if (!line) continue; // Skip empty lines

                var values = parseCSVLine(line);
                if (values.length < headers.length) {
                    log.debug('Skipping incomplete row', {
                        lineNumber: i + 1,
                        expectedColumns: headers.length,
                        actualColumns: values.length,
                        linePreview: line.substring(0, 100)
                    });
                    continue; // Skip incomplete rows
                }

                var rowObj = {};
                for (var j = 0; j < headers.length; j++) {
                    rowObj[headers[j]] = values[j] ? values[j].trim() : '';
                }

                // Only include rows with OrderNo
                if (rowObj.OrderNo) {
                    rows.push(rowObj);
                }
            }

            log.debug('CSV Rows Parsed', { totalRows: rows.length });

            // Extract unique OrderNos
            var uniqueOrderNos = [];
            var orderNoMap = {};

            for (var i = 0; i < rows.length; i++) {
                var orderNo = rows[i].OrderNo;
                if (orderNo && !orderNoMap[orderNo]) {
                    orderNoMap[orderNo] = true;
                    uniqueOrderNos.push(orderNo);
                }
            }

            log.audit('CSV Parsing Complete', {
                totalRows: rows.length,
                uniqueOrderNos: uniqueOrderNos.length,
                fileName: fileObj.name
            });

            return {
                success: true,
                fileName: fileObj.name,
                headers: headers,
                rows: rows,
                totalRows: rows.length,
                uniqueOrderNos: uniqueOrderNos
            };

        } catch (error) {
            log.error('CSV Load Error', {
                error: error.toString(),
                fileId: fileId,
                stack: error.stack
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    /**
     * Process a single OrderNo - extract data, validate, create transactions
     * @param {Object} parsedData - Full CSV data
     * @param {string} orderNo - OrderNo to process
     * @returns {Object} Processing result
     */
    function processOrderNo(parsedData, orderNo) {
        try {
            log.debug('Processing OrderNo', { orderNo: orderNo });

            // Extract all data for this OrderNo
            var orderData = extractOrderData(parsedData, orderNo);

            if (!orderData.success) {
                return {
                    success: false,
                    error: orderData.error,
                    orderNo: orderNo
                };
            }

            // Validate order totals
            var validation = validateOrderTotals(orderData);

            if (!validation.isValid) {
                log.error('Order Validation Failed', {
                    orderNo: orderNo,
                    calculatedTotal: validation.calculatedTotal,
                    reportedTotal: validation.reportedTotal,
                    difference: validation.difference
                });

                return {
                    success: false,
                    isValidationFailure: true,
                    skipReason: 'Total validation failed: Calculated ' +
                        validation.calculatedTotal + ' vs Reported ' +
                        validation.reportedTotal,
                    orderNo: orderNo
                };
            }

            log.debug('Order Validation Passed', {
                orderNo: orderNo,
                totalAmount: validation.reportedTotal
            });

            // Process transactions based on NARDA types
            var result = processOrderTransactions(orderData, parsedData);

            return result;

        } catch (error) {
            log.error('OrderNo Processing Error', {
                error: error.toString(),
                orderNo: orderNo,
                stack: error.stack
            });

            return {
                success: false,
                error: error.toString(),
                orderNo: orderNo
            };
        }
    }


    /**
     * Determine NARDA type (Journal Entry, Vendor Credit, or Skip)
     * @param {string} nardaNumber - NARDA number to classify
     * @returns {string} Type: 'JE', 'VC', or 'SKIP'
     */
    function determineNardaType(nardaNumber) {
        if (!nardaNumber) return 'SKIP';

        if (CONFIG.NARDA_PATTERNS.JOURNAL_ENTRY.test(nardaNumber)) {
            return 'JE';
        }

        if (CONFIG.NARDA_PATTERNS.VENDOR_CREDIT.test(nardaNumber)) {
            return 'VC';
        }

        if (CONFIG.NARDA_PATTERNS.SKIP.test(nardaNumber)) {
            return 'SKIP';
        }

        // Unknown pattern - skip for manual review
        return 'SKIP';
    }

    /**
     * Extract original bill number from Description column
     * Pattern: N or W prefix followed by 8-10 digits (returns digits only)
     * @param {string} description - Description column value
     * @returns {string|null} Extracted bill number (digits only) or null
     */
    function extractOriginalBillNumber(description) {
        if (!description) return null;

        // Pattern: N or W followed by 8-10 digits - capture just the digits
        var matches = description.match(/[NW](\d{8,10})/);

        if (matches && matches.length > 1) {
            return matches[1]; // Return captured group (just the digits)
        }

        return null;
    }

    /**
     * Format currency value for display
     * @param {number} amount - Numeric amount
     * @returns {string} Formatted currency string
     */
    function formatCurrency(amount) {
        if (isNaN(amount)) return '$0.00';

        var absAmount = Math.abs(amount);
        var formatted = '$' + absAmount.toFixed(2);

        // Add parentheses for negative values
        if (amount < 0) {
            formatted = '(' + formatted + ')';
        }

        return formatted;
    }

    /**
     * Parse date string from CSV
     * @param {string} dateStr - Date string from CSV (MM/DD/YYYY format)
     * @returns {Date} Parsed date object
     */
    function parseCSVDate(dateStr) {
        if (!dateStr) return new Date();

        // Expected format: MM/DD/YYYY
        var parts = dateStr.split('/');
        if (parts.length !== 3) return new Date();

        var month = parseInt(parts[0], 10) - 1; // JavaScript months are 0-indexed
        var day = parseInt(parts[1], 10);
        var year = parseInt(parts[2], 10);

        return new Date(year, month, day);
    }

    /**
     * Extract all data for specific OrderNo from CSV
     * Groups by NARDA, calculates totals, extracts bill numbers
     * @param {Object} parsedData - From loadAndParseCSV()
     * @param {string} orderNo - OrderNo to extract
     * @returns {Object} Structured order data
     */
    function extractOrderData(parsedData, orderNo) {
        try {
            log.debug('Extracting Order Data', { orderNo: orderNo });

            // Filter rows to this OrderNo
            var orderRows = [];
            for (var i = 0; i < parsedData.rows.length; i++) {
                if (parsedData.rows[i].OrderNo === orderNo) {
                    orderRows.push(parsedData.rows[i]);
                }
            }

            if (orderRows.length === 0) {
                return {
                    success: false,
                    error: 'No rows found for OrderNo: ' + orderNo
                };
            }

            log.debug('Order Rows Found', {
                orderNo: orderNo,
                rowCount: orderRows.length
            });

            // Extract header data from first row
            var firstRow = orderRows[0];
            var invoiceDate = firstRow['Date Ordered'];
            var totalAmount = firstRow.Total;

            // Group line items by NARDA Number
            var groupedLineItems = {};

            for (var i = 0; i < orderRows.length; i++) {
                var row = orderRows[i];
                var nardaNumber = row['NARDA Number'] ? row['NARDA Number'].trim() : null;

                if (!nardaNumber) {
                    log.debug('Skipping row without NARDA', {
                        orderNo: orderNo,
                        rowIndex: i
                    });
                    continue;
                }

                // Calculate extended price (Price × Quantity)
                var price = parseFloat(row.Price.replace(/[$(),]/g, ''));
                var quantity = parseInt(row.Quantity, 10);
                var extendedPrice = Math.abs(price * quantity);

                // Extract original bill number from Description
                var originalBillNumber = extractOriginalBillNumber(row.Description);

                // Create line item object
                var lineItem = {
                    part: row.Part,
                    description: row.Description,
                    price: row.Price,
                    quantity: row.Quantity,
                    extendedPrice: extendedPrice,
                    originalBillNumber: originalBillNumber,
                    nardaNumber: nardaNumber
                };

                // Initialize NARDA group if needed
                if (!groupedLineItems[nardaNumber]) {
                    groupedLineItems[nardaNumber] = {
                        lineItems: [],
                        totalAmount: 0,
                        originalBillNumbers: []
                    };
                }

                // Add to group
                groupedLineItems[nardaNumber].lineItems.push(lineItem);
                groupedLineItems[nardaNumber].totalAmount += extendedPrice;

                // Add unique bill number
                if (originalBillNumber &&
                    groupedLineItems[nardaNumber].originalBillNumbers.indexOf(originalBillNumber) === -1) {
                    groupedLineItems[nardaNumber].originalBillNumbers.push(originalBillNumber);
                }
            }

            log.debug('Order Data Extracted', {
                orderNo: orderNo,
                invoiceDate: invoiceDate,
                totalAmount: totalAmount,
                nardaGroups: Object.keys(groupedLineItems).length
            });

            return {
                success: true,
                invoiceNumber: orderNo,
                invoiceDate: invoiceDate,
                totalAmount: totalAmount,
                deliveryAmount: '$0.00', // Always $0.00 in CSV
                groupedLineItems: groupedLineItems,
                orderRows: orderRows
            };

        } catch (error) {
            log.error('Order Data Extraction Error', {
                error: error.toString(),
                orderNo: orderNo,
                stack: error.stack
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    /**
    * Validate order totals match line item extended prices
    * @param {Object} orderData - Extracted order data
    * @returns {Object} Validation result with details
    */
    function validateOrderTotals(orderData) {
        try {
            log.debug('Starting Total Validation', {
                orderNo: orderData.invoiceNumber,
                totalNARDAGroups: Object.keys(orderData.groupedLineItems).length,
                nardaNumbers: Object.keys(orderData.groupedLineItems)
            });

            // Calculate sum of all extended prices
            var calculatedTotal = 0;
            var nardaNumbers = Object.keys(orderData.groupedLineItems);

            // ENHANCED LOGGING: Track each NARDA contribution
            var nardaBreakdown = [];

            for (var i = 0; i < nardaNumbers.length; i++) {
                var nardaNumber = nardaNumbers[i];
                var nardaGroup = orderData.groupedLineItems[nardaNumber];
                var nardaSubtotal = nardaGroup.totalAmount;

                // Log this NARDA's contribution
                nardaBreakdown.push({
                    narda: nardaNumber,
                    lineCount: nardaGroup.lineItems.length,
                    subtotal: nardaSubtotal,
                    lineItems: nardaGroup.lineItems.map(function (item) {
                        return {
                            part: item.part,
                            quantity: item.quantity,
                            price: item.price,
                            extendedPrice: item.extendedPrice
                        };
                    })
                });

                calculatedTotal += nardaSubtotal;
            }

            // Get reported total (MAX value from Total column)
            var reportedTotal = Math.abs(
                parseFloat(orderData.totalAmount.replace(/[$,()]/g, ''))
            );

            // Compare with tolerance
            var difference = Math.abs(calculatedTotal - reportedTotal);
            var isValid = difference < 0.01;

            // ENHANCED LOGGING: Show detailed breakdown
            log.debug('Total Validation Breakdown', {
                orderNo: orderData.invoiceNumber,
                nardaCount: nardaNumbers.length,
                nardaBreakdown: nardaBreakdown,
                calculatedTotal: calculatedTotal,
                reportedTotal: reportedTotal,
                difference: difference,
                isValid: isValid
            });

            if (!isValid) {
                log.error('Total Validation Details', {
                    orderNo: orderData.invoiceNumber,
                    calculatedTotal: calculatedTotal,
                    reportedTotal: reportedTotal,
                    difference: difference,
                    nardaBreakdown: nardaBreakdown,
                    totalColumnValue: orderData.totalAmount,
                    lineItemCount: orderData.orderRows.length
                });
            }

            return {
                isValid: isValid,
                calculatedTotal: calculatedTotal,
                reportedTotal: reportedTotal,
                difference: difference,
                orderNo: orderData.invoiceNumber
            };

        } catch (error) {
            log.error('Validation Error', {
                error: error.toString(),
                orderNo: orderData.invoiceNumber
            });

            return {
                isValid: false,
                error: error.toString()
            };
        }
    }

    /**
     * Process all transactions for an order based on NARDA types
     * @param {Object} orderData - Extracted and validated order data
     * @param {Object} parsedData - Full CSV data for file creation
     * @returns {Object} Processing results
     */
    function processOrderTransactions(orderData, parsedData) {
        try {
            var journalEntries = [];
            var vendorCredits = [];
            var skippedTransactions = [];

            var nardaNumbers = Object.keys(orderData.groupedLineItems);

            log.debug('Processing Transactions', {
                orderNo: orderData.invoiceNumber,
                nardaGroupCount: nardaNumbers.length,
                nardaNumbers: nardaNumbers
            });

            // Separate NARDA groups by type
            var jeGroups = [];
            var vcGroups = [];

            for (var i = 0; i < nardaNumbers.length; i++) {
                var nardaNumber = nardaNumbers[i];
                var nardaType = determineNardaType(nardaNumber);

                if (nardaType === 'SKIP') {
                    log.debug('Skipping NARDA', {
                        nardaNumber: nardaNumber,
                        reason: 'NARDA pattern matched SKIP list'
                    });

                    skippedTransactions.push({
                        orderNo: orderData.invoiceNumber,
                        nardaNumber: nardaNumber,
                        skipReason: 'Manual processing required for NARDA type: ' + nardaNumber,
                        skipType: 'NARDA_SKIP_PATTERN'
                    });
                    continue;
                }

                if (nardaType === 'JE') {
                    jeGroups.push(nardaNumber);
                } else if (nardaType === 'VC') {
                    vcGroups.push(nardaNumber);
                }
            }

            log.debug('NARDA Classification', {
                journalEntryGroups: jeGroups.length,
                vendorCreditGroups: vcGroups.length,
                skippedGroups: skippedTransactions.length
            });

            // Create filtered CSV for this OrderNo
            var filteredCsvId = createFilteredCSV(parsedData, orderData.invoiceNumber);

            // Process Journal Entry groups
            if (jeGroups.length > 0) {
                var jeResult = processJournalEntryGroups(
                    orderData,
                    jeGroups,
                    filteredCsvId
                );

                if (jeResult.success && jeResult.journalEntries) {
                    journalEntries = journalEntries.concat(jeResult.journalEntries);
                }
            }

            // Process Vendor Credit groups
            if (vcGroups.length > 0) {
                var vcResult = processVendorCreditGroups(
                    orderData,
                    vcGroups,
                    filteredCsvId
                );

                if (vcResult.success && vcResult.vendorCredits) {
                    vendorCredits = vendorCredits.concat(vcResult.vendorCredits);
                }

                if (vcResult.skipped) {
                    skippedTransactions = skippedTransactions.concat(vcResult.skipped);
                }
            }

            return {
                success: true,
                orderNo: orderData.invoiceNumber,
                journalEntries: journalEntries,
                vendorCredits: vendorCredits,
                skippedTransactions: skippedTransactions
            };

        } catch (error) {
            log.error('Transaction Processing Error', {
                error: error.toString(),
                orderNo: orderData.invoiceNumber,
                stack: error.stack
            });

            return {
                success: false,
                error: error.toString(),
                orderNo: orderData.invoiceNumber
            };
        }
    }

    /**
     * Process Journal Entry groups for an order
     * @param {Object} orderData - Extracted order data
     * @param {Array} jeGroups - Array of NARDA numbers for JE processing
     * @param {number} filteredCsvId - Filtered CSV file ID for attachment
     * @returns {Object} Processing results
     */
    function processJournalEntryGroups(orderData, jeGroups, filteredCsvId) {
        try {
            var journalEntries = [];

            log.debug('Processing Journal Entry Groups', {
                orderNo: orderData.invoiceNumber,
                groupCount: jeGroups.length,
                groups: jeGroups
            });

            // If multiple JE groups, create single JE with multiple lines
            if (jeGroups.length > 1) {
                var jeResult = createSingleJournalEntryWithMultipleLines(
                    orderData,
                    jeGroups,
                    filteredCsvId
                );

                if (jeResult.success) {
                    journalEntries.push(jeResult);
                }
            } else {
                // Single JE group - create individual JE
                var nardaNumber = jeGroups[0];
                var nardaGroup = orderData.groupedLineItems[nardaNumber];

                var jeResult = createJournalEntryFromNardaGroup(
                    orderData,
                    nardaGroup,
                    nardaNumber,
                    filteredCsvId
                );

                if (jeResult.success) {
                    journalEntries.push(jeResult);
                }
            }

            return {
                success: true,
                journalEntries: journalEntries
            };

        } catch (error) {
            log.error('Journal Entry Group Processing Error', {
                error: error.toString(),
                orderNo: orderData.invoiceNumber
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    /**
     * Create single Journal Entry with multiple NARDA lines
     * @param {Object} orderData - Extracted order data
     * @param {Array} jeGroups - Array of NARDA numbers
     * @param {number} filteredCsvId - Filtered CSV file ID
     * @returns {Object} Creation result with governance tracking
     */
    function createSingleJournalEntryWithMultipleLines(orderData, jeGroups, filteredCsvId) {
        // GOVERNANCE: Track starting point
        var governanceBefore = runtime.getCurrentScript().getRemainingUsage();

        try {
            log.debug('Creating Consolidated Journal Entry', {
                orderNo: orderData.invoiceNumber,
                nardaCount: jeGroups.length,
                nardas: jeGroups,
                governanceBefore: governanceBefore
            });

            // Create tranid
            var tranid = orderData.invoiceNumber + ' CM';

            // Check for duplicates
            var duplicateCheck = checkForDuplicateJournalEntry(tranid);
            if (!duplicateCheck.success) {
                log.error('Duplicate JE detected', {
                    tranid: tranid,
                    existing: duplicateCheck.existingEntry
                });
                return {
                    success: false,
                    error: 'Duplicate: ' + tranid,
                    isDuplicate: true,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            // Parse date
            var jeDate = parseCSVDate(orderData.invoiceDate);

            // Calculate total across all NARDA groups
            var grandTotal = 0;
            var expectedTotal = 0;

            for (var i = 0; i < jeGroups.length; i++) {
                var nardaGroup = orderData.groupedLineItems[jeGroups[i]];
                grandTotal += nardaGroup.totalAmount;
            }

            // VALIDATION: Calculate expected total from CSV (sum of all Price × Quantity for these NARDAs)
            // Get the reported total from CSV
            var reportedTotal = Math.abs(
                parseFloat(orderData.totalAmount.replace(/[$,()]/g, ''))
            );

            // For multiple NARDAs, we need to sum only the ones we're processing
            expectedTotal = grandTotal; // This should match what we calculated

            // CRITICAL VALIDATION: Verify calculated total matches expected total
            var totalDifference = Math.abs(grandTotal - expectedTotal);
            if (totalDifference > 0.01) {
                log.error('Journal Entry Total Validation Failed', {
                    orderNo: orderData.invoiceNumber,
                    calculatedTotal: grandTotal,
                    expectedTotal: expectedTotal,
                    difference: totalDifference,
                    nardas: jeGroups
                });

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'Journal Entry total mismatch: Calculated ' +
                        formatCurrency(grandTotal) + ' vs Expected ' +
                        formatCurrency(expectedTotal) + ' (difference: ' +
                        formatCurrency(totalDifference) + ')',
                    skipType: 'JE_TOTAL_MISMATCH',
                    orderNo: orderData.invoiceNumber,
                    nardaNumbers: jeGroups,
                    calculatedTotal: grandTotal,
                    expectedTotal: expectedTotal,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            log.audit('Journal Entry Total Validation Passed', {
                orderNo: orderData.invoiceNumber,
                calculatedTotal: grandTotal,
                expectedTotal: expectedTotal,
                difference: totalDifference
            });

            // Create Journal Entry
            var journalEntry = record.create({
                type: record.Type.JOURNAL_ENTRY,
                isDynamic: true
            });

            journalEntry.setValue({ fieldId: 'tranid', value: tranid });
            journalEntry.setValue({ fieldId: 'trandate', value: jeDate });
            journalEntry.setValue({
                fieldId: 'memo',
                value: 'MARCONE CM' + orderData.invoiceNumber + ' Multiple NARDAs'
            });

            // Debit line - Accounts Payable
            journalEntry.selectNewLine({ sublistId: 'line' });
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
                value: 'MARCONE CM' + orderData.invoiceNumber
            });
            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'entity',
                value: CONFIG.ENTITIES.MARCONE
            });
            journalEntry.commitLine({ sublistId: 'line' });

            // Credit lines - One per NARDA group
            for (var i = 0; i < jeGroups.length; i++) {
                var nardaNumber = jeGroups[i];
                var nardaGroup = orderData.groupedLineItems[nardaNumber];

                // Find credit line entity
                var entityResult = findCreditLineEntity(nardaNumber);
                if (!entityResult.success) {
                    log.error('Entity lookup failed', {
                        nardaNumber: nardaNumber,
                        error: entityResult.error
                    });
                    continue;
                }

                journalEntry.selectNewLine({ sublistId: 'line' });
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
                    value: 'MARCONE CM' + orderData.invoiceNumber + ' ' + nardaNumber
                });
                journalEntry.setCurrentSublistValue({
                    sublistId: 'line',
                    fieldId: 'entity',
                    value: entityResult.entityId
                });
                journalEntry.commitLine({ sublistId: 'line' });
            }

            // Save JE
            var jeId = journalEntry.save();

            // GOVERNANCE: Calculate usage
            var governanceAfter = runtime.getCurrentScript().getRemainingUsage();
            var governanceUsed = governanceBefore - governanceAfter;

            log.audit('Consolidated JE Created', {
                jeId: jeId,
                tranid: tranid,
                nardaCount: jeGroups.length,
                totalAmount: grandTotal,
                validatedAmount: expectedTotal,
                governanceUsed: governanceUsed,
                governanceRemaining: governanceAfter
            });

            // Attach filtered CSV
            attachFileToRecord(jeId, filteredCsvId, record.Type.JOURNAL_ENTRY);

            return {
                success: true,
                journalEntryId: jeId,
                tranid: tranid,
                nardaNumbers: jeGroups,
                totalAmount: grandTotal,
                governanceUsed: governanceUsed
            };

        } catch (error) {
            log.error('Consolidated JE Creation Error', {
                error: error.toString(),
                orderNo: orderData.invoiceNumber,
                governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
            });

            return {
                success: false,
                error: error.toString(),
                governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
            };
        }
    }

    /**
     * Create Journal Entry from single NARDA group
     * @param {Object} orderData - Extracted order data
     * @param {Object} nardaGroup - NARDA group data
     * @param {string} nardaNumber - NARDA number
     * @param {number} filteredCsvId - Filtered CSV file ID
     * @returns {Object} Creation result with governance tracking
     */
    function createJournalEntryFromNardaGroup(orderData, nardaGroup, nardaNumber, filteredCsvId) {
        // GOVERNANCE: Track starting point
        var governanceBefore = runtime.getCurrentScript().getRemainingUsage();

        try {
            log.audit('=== JOURNAL ENTRY CREATION START ===', {
                orderNo: orderData.invoiceNumber,
                nardaNumber: nardaNumber,
                amount: nardaGroup.totalAmount,
                governanceBefore: governanceBefore,
                lineItemCount: nardaGroup.lineItems.length,
                invoiceDate: orderData.invoiceDate
            });

            // STEP 1: Create tranid
            var tranid = orderData.invoiceNumber + ' CM';
            log.debug('Step 1: Tranid Created', {
                tranid: tranid,
                orderNo: orderData.invoiceNumber
            });

            // STEP 2: Check for duplicates
            log.debug('Step 2: Starting Duplicate Check', {
                tranid: tranid
            });

            var duplicateCheck = checkForDuplicateJournalEntry(tranid);

            log.debug('Step 2: Duplicate Check Complete', {
                success: duplicateCheck.success,
                hasDuplicate: !duplicateCheck.success,
                existingEntry: duplicateCheck.existingEntry || null
            });

            if (!duplicateCheck.success) {
                log.error('DUPLICATE JOURNAL ENTRY DETECTED', {
                    tranid: tranid,
                    existingEntry: duplicateCheck.existingEntry,
                    orderNo: orderData.invoiceNumber
                });

                return {
                    success: false,
                    error: 'Duplicate JE exists: ' + tranid,
                    isDuplicate: true,
                    existingJE: duplicateCheck.existingEntry,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            // STEP 3: Calculate and validate totals
            log.debug('Step 3: Starting Total Calculation', {
                nardaNumber: nardaNumber,
                expectedTotal: nardaGroup.totalAmount,
                lineItemCount: nardaGroup.lineItems.length
            });

            var expectedTotal = nardaGroup.totalAmount;
            var calculatedTotal = 0;

            // Re-calculate from line items to verify
            for (var i = 0; i < nardaGroup.lineItems.length; i++) {
                var lineItem = nardaGroup.lineItems[i];
                calculatedTotal += lineItem.extendedPrice;

                log.debug('Step 3: Line Item Calculation', {
                    lineIndex: i,
                    part: lineItem.part,
                    quantity: lineItem.quantity,
                    price: lineItem.price,
                    extendedPrice: lineItem.extendedPrice,
                    runningTotal: calculatedTotal
                });
            }

            // CRITICAL VALIDATION: Verify calculated total matches expected total
            var totalDifference = Math.abs(calculatedTotal - expectedTotal);

            log.debug('Step 3: Total Calculation Complete', {
                calculatedTotal: calculatedTotal,
                expectedTotal: expectedTotal,
                difference: totalDifference,
                isValid: totalDifference < 0.01
            });

            if (totalDifference > 0.01) {
                log.error('JOURNAL ENTRY TOTAL VALIDATION FAILED', {
                    orderNo: orderData.invoiceNumber,
                    nardaNumber: nardaNumber,
                    calculatedTotal: calculatedTotal,
                    expectedTotal: expectedTotal,
                    difference: totalDifference,
                    lineItemCount: nardaGroup.lineItems.length,
                    lineItems: nardaGroup.lineItems
                });

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'Journal Entry total mismatch: Calculated ' +
                        formatCurrency(calculatedTotal) + ' vs Expected ' +
                        formatCurrency(expectedTotal) + ' (difference: ' +
                        formatCurrency(totalDifference) + ')',
                    skipType: 'JE_TOTAL_MISMATCH',
                    orderNo: orderData.invoiceNumber,
                    nardaNumber: nardaNumber,
                    calculatedTotal: calculatedTotal,
                    expectedTotal: expectedTotal,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            log.audit('Step 3: Total Validation PASSED', {
                orderNo: orderData.invoiceNumber,
                nardaNumber: nardaNumber,
                calculatedTotal: calculatedTotal,
                expectedTotal: expectedTotal,
                difference: totalDifference
            });

            // STEP 4: Find credit line entity
            log.debug('Step 4: Starting Entity Lookup', {
                nardaNumber: nardaNumber
            });

            var entityResult = findCreditLineEntity(nardaNumber);

            log.debug('Step 4: Entity Lookup Complete', {
                success: entityResult.success,
                entityId: entityResult.entityId || null,
                error: entityResult.error || null
            });

            if (!entityResult.success) {
                log.error('ENTITY LOOKUP FAILED', {
                    nardaNumber: nardaNumber,
                    error: entityResult.error,
                    orderNo: orderData.invoiceNumber
                });

                return {
                    success: false,
                    error: 'Entity lookup failed for ' + nardaNumber + ': ' + entityResult.error,
                    orderNo: orderData.invoiceNumber,
                    nardaNumber: nardaNumber,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            log.audit('Step 4: Entity Lookup SUCCESS', {
                nardaNumber: nardaNumber,
                entityId: entityResult.entityId
            });

            // STEP 5: Parse date
            log.debug('Step 5: Parsing Date', {
                invoiceDateString: orderData.invoiceDate
            });

            var jeDate = parseCSVDate(orderData.invoiceDate);

            log.debug('Step 5: Date Parsed', {
                originalString: orderData.invoiceDate,
                parsedDate: jeDate,
                dateValid: !isNaN(jeDate.getTime())
            });

            if (isNaN(jeDate.getTime())) {
                log.error('DATE PARSING FAILED', {
                    invoiceDateString: orderData.invoiceDate,
                    parsedDate: jeDate,
                    orderNo: orderData.invoiceNumber
                });

                return {
                    success: false,
                    error: 'Invalid date format: ' + orderData.invoiceDate,
                    orderNo: orderData.invoiceNumber,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            // STEP 6: Create Journal Entry record
            log.audit('Step 6: Creating Journal Entry Record', {
                recordType: record.Type.JOURNAL_ENTRY,
                isDynamic: true
            });

            var journalEntry = record.create({
                type: record.Type.JOURNAL_ENTRY,
                isDynamic: true
            });

            log.debug('Step 6: Record Created - Setting Header Fields');

            var memo = 'MARCONE CM' + orderData.invoiceNumber + ' ' + nardaNumber;

            // Set header fields
            journalEntry.setValue({ fieldId: 'tranid', value: tranid });
            log.debug('Step 6: Set tranid', { tranid: tranid });

            journalEntry.setValue({ fieldId: 'trandate', value: jeDate });
            log.debug('Step 6: Set trandate', { trandate: jeDate });

            journalEntry.setValue({ fieldId: 'memo', value: memo });
            log.debug('Step 6: Set memo', { memo: memo });

            // STEP 7: Add Debit line (Accounts Payable)
            log.debug('Step 7: Adding Debit Line (Accounts Payable)');

            journalEntry.selectNewLine({ sublistId: 'line' });
            log.debug('Step 7: New line selected');

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'account',
                value: CONFIG.ACCOUNTS.ACCOUNTS_PAYABLE
            });
            log.debug('Step 7: Account set', {
                account: CONFIG.ACCOUNTS.ACCOUNTS_PAYABLE,
                accountName: 'Accounts Payable'
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'debit',
                value: expectedTotal
            });
            log.debug('Step 7: Debit amount set', { debit: expectedTotal });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'memo',
                value: memo
            });
            log.debug('Step 7: Line memo set', { memo: memo });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'entity',
                value: CONFIG.ENTITIES.MARCONE
            });
            log.debug('Step 7: Entity set', {
                entity: CONFIG.ENTITIES.MARCONE,
                entityName: 'Marcone'
            });

            journalEntry.commitLine({ sublistId: 'line' });
            log.audit('Step 7: Debit Line Committed', {
                account: CONFIG.ACCOUNTS.ACCOUNTS_PAYABLE,
                debit: expectedTotal,
                entity: CONFIG.ENTITIES.MARCONE
            });

            // STEP 8: Add Credit line (Accounts Receivable)
            log.debug('Step 8: Adding Credit Line (Accounts Receivable)');

            journalEntry.selectNewLine({ sublistId: 'line' });
            log.debug('Step 8: New line selected');

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'account',
                value: CONFIG.ACCOUNTS.ACCOUNTS_RECEIVABLE
            });
            log.debug('Step 8: Account set', {
                account: CONFIG.ACCOUNTS.ACCOUNTS_RECEIVABLE,
                accountName: 'Accounts Receivable'
            });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'credit',
                value: expectedTotal
            });
            log.debug('Step 8: Credit amount set', { credit: expectedTotal });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'memo',
                value: memo
            });
            log.debug('Step 8: Line memo set', { memo: memo });

            journalEntry.setCurrentSublistValue({
                sublistId: 'line',
                fieldId: 'entity',
                value: entityResult.entityId
            });
            log.debug('Step 8: Entity set', {
                entity: entityResult.entityId,
                entitySource: 'Customer from NARDA lookup'
            });

            journalEntry.commitLine({ sublistId: 'line' });
            log.audit('Step 8: Credit Line Committed', {
                account: CONFIG.ACCOUNTS.ACCOUNTS_RECEIVABLE,
                credit: expectedTotal,
                entity: entityResult.entityId
            });

            // STEP 9: Save Journal Entry
            log.audit('Step 9: Saving Journal Entry', {
                tranid: tranid,
                totalAmount: expectedTotal,
                lineCount: 2
            });

            var jeId = journalEntry.save();

            log.audit('Step 9: Journal Entry SAVED SUCCESSFULLY', {
                jeId: jeId,
                tranid: tranid
            });

            // GOVERNANCE: Calculate usage
            var governanceAfter = runtime.getCurrentScript().getRemainingUsage();
            var governanceUsed = governanceBefore - governanceAfter;

            log.audit('=== JOURNAL ENTRY CREATION SUCCESS ===', {
                jeId: jeId,
                tranid: tranid,
                nardaNumber: nardaNumber,
                amount: expectedTotal,
                validatedAmount: calculatedTotal,
                governanceUsed: governanceUsed,
                governanceRemaining: governanceAfter,
                orderNo: orderData.invoiceNumber
            });

            // STEP 10: Attach filtered CSV
            log.debug('Step 10: Attaching CSV File', {
                jeId: jeId,
                csvFileId: filteredCsvId
            });

            attachFileToRecord(jeId, filteredCsvId, record.Type.JOURNAL_ENTRY);

            log.debug('Step 10: CSV Attachment Complete');

            return {
                success: true,
                journalEntryId: jeId,
                tranid: tranid,
                nardaNumber: nardaNumber,
                totalAmount: expectedTotal,
                governanceUsed: governanceUsed
            };

        } catch (error) {
            log.error('=== JOURNAL ENTRY CREATION ERROR ===', {
                error: error.toString(),
                errorName: error.name,
                errorMessage: error.message,
                errorStack: error.stack,
                orderNo: orderData.invoiceNumber,
                nardaNumber: nardaNumber,
                expectedAmount: nardaGroup.totalAmount,
                governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
            });

            return {
                success: false,
                error: error.toString(),
                errorDetails: {
                    name: error.name,
                    message: error.message,
                    stack: error.stack
                },
                orderNo: orderData.invoiceNumber,
                nardaNumber: nardaNumber,
                governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
            };
        }
    }

    /**
     * Check for duplicate Journal Entry by tranid
     * @param {string} tranid - Transaction ID to check
     * @returns {Object} Duplicate check result
     */
    function checkForDuplicateJournalEntry(tranid) {
        try {
            var journalEntrySearch = search.create({
                type: search.Type.JOURNAL_ENTRY,
                filters: [
                    ['type', 'anyof', 'Journal'],
                    'AND',
                    ['tranid', 'is', tranid]
                ],
                columns: ['tranid', 'trandate', 'internalid']
            });

            var existingEntries = [];
            journalEntrySearch.run().each(function (result) {
                existingEntries.push({
                    internalId: result.getValue('internalid'),
                    tranid: result.getValue('tranid'),
                    trandate: result.getValue('trandate')
                });
                return true;
            });

            if (existingEntries.length > 0) {
                return {
                    success: false,
                    existingEntry: existingEntries[0]
                };
            }

            return { success: true };

        } catch (error) {
            log.error('Duplicate Check Error', {
                error: error.toString(),
                tranid: tranid
            });

            return {
                success: false,
                error: error.toString()
            };
        }
    }

    /**
    * Find Credit Line Entity based on NARDA number
    * Searches open invoices by custbody_f4n_job_id first, then falls back to invoice tranid lookup
    * @param {string} nardaNumber - NARDA number (e.g., J17679, INV1666079)
    * @returns {Object} Entity lookup result with entityId and details
    */
    function findCreditLineEntity(nardaNumber) {
        try {
            log.debug('Entity Lookup: Starting', {
                nardaNumber: nardaNumber
            });

            // STEP 1: Search for open invoices with NARDA number in custbody_f4n_job_id
            log.debug('Entity Lookup: Searching by custbody_f4n_job_id (Open Invoices)', {
                nardaNumber: nardaNumber
            });

            var invoiceSearch = search.create({
                type: search.Type.INVOICE,
                filters: [
                    ['type', 'anyof', 'CustInvc'],
                    'AND',
                    ['status', 'anyof', 'CustInvc:A'], // Open invoices only
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

            log.debug('Entity Lookup: custbody_f4n_job_id Search Complete', {
                nardaNumber: nardaNumber,
                totalResults: searchResults.length
            });

            // STEP 2: If no results found, search against the invoice tranid
            if (searchResults.length === 0) {
                log.debug('Entity Lookup: No matches in custbody_f4n_job_id, searching by invoice tranid', {
                    nardaNumber: nardaNumber
                });

                var invoiceSearchByTranid = search.create({
                    type: search.Type.INVOICE,
                    filters: [
                        ['type', 'anyof', 'CustInvc'],
                        'AND',
                        ['status', 'anyof', 'CustInvc:A'], // Open invoices only
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

                log.debug('Entity Lookup: Invoice tranid Search Complete', {
                    nardaNumber: nardaNumber,
                    totalResults: searchResults.length
                });
            }

            // FINAL VALIDATION: Check if any open invoices were found
            if (searchResults.length === 0) {
                log.debug('Entity Lookup: No open invoices found', {
                    nardaNumber: nardaNumber,
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

            log.audit('Entity Lookup: SUCCESS', {
                nardaNumber: nardaNumber,
                entityId: mostRecentInvoice.entityId,
                invoiceTranid: mostRecentInvoice.tranid,
                tranDate: mostRecentInvoice.tranDate,
                totalResults: searchResults.length,
                foundBy: searchResults.length > 1 ? 'custbody_f4n_job_id' : 'invoice_tranid'
            });

            return {
                success: true,
                entityId: mostRecentInvoice.entityId,
                entityIdText: mostRecentInvoice.tranid, // For compatibility
                invoiceTranid: mostRecentInvoice.tranid,
                tranDate: mostRecentInvoice.tranDate,
                searchResultCount: searchResults.length
            };

        } catch (error) {
            log.error('Entity Lookup: Exception', {
                error: error.toString(),
                errorStack: error.stack,
                nardaNumber: nardaNumber
            });

            return {
                success: false,
                error: 'Search error: ' + error.toString(),
                reason: 'SEARCH_ERROR',
                nardaNumber: nardaNumber
            };
        }
    }

    /**
     * Process Vendor Credit groups for an order
     * @param {Object} orderData - Extracted order data
     * @param {Array} vcGroups - Array of NARDA numbers for VC processing
     * @param {number} filteredCsvId - Filtered CSV file ID
     * @returns {Object} Processing results
     */
    function processVendorCreditGroups(orderData, vcGroups, filteredCsvId) {
        try {
            var vendorCredits = [];
            var skipped = [];

            log.debug('Processing Vendor Credit Groups', {
                orderNo: orderData.invoiceNumber,
                groupCount: vcGroups.length,
                groups: vcGroups
            });

            // Consolidate by original bill number
            var consolidatedGroups = consolidateVendorCreditGroups(orderData.groupedLineItems);

            log.debug('Vendor Credit Groups Consolidated', {
                orderNo: orderData.invoiceNumber,
                billNumberCount: Object.keys(consolidatedGroups).length,
                billNumbers: Object.keys(consolidatedGroups)
            });

            // Process each bill number group
            var billNumbers = Object.keys(consolidatedGroups);
            for (var i = 0; i < billNumbers.length; i++) {
                var billNumber = billNumbers[i];
                var billGroup = consolidatedGroups[billNumber];

                log.debug('Processing Bill Number Group', {
                    billNumber: billNumber,
                    lineItemCount: billGroup.lineItems.length,
                    totalAmount: billGroup.totalAmount,
                    nardaTypes: billGroup.nardaNumbers ? billGroup.nardaNumbers.join('+') : 'Unknown'
                });

                // Search for matching VRA
                var vraResults = searchForMatchingVRA(billNumber);

                if (!vraResults.success || vraResults.results.length === 0) {
                    log.debug('No VRA Found', {
                        billNumber: billNumber,
                        orderNo: orderData.invoiceNumber,
                        vraSearchSuccess: vraResults.success,
                        vraResultCount: vraResults.results ? vraResults.results.length : 0
                    });

                    skipped.push({
                        orderNo: orderData.invoiceNumber,
                        billNumber: billNumber,
                        skipReason: 'No VRA found for bill number',
                        skipType: 'NO_VRA',
                        nardaNumbers: billGroup.nardaNumbers || []
                    });
                    continue;
                }

                log.debug('VRA Found - Matching Lines', {
                    billNumber: billNumber,
                    vraCount: vraResults.results.length,
                    pdfLineCount: billGroup.lineItems.length
                });

                // Match PDF lines to VRA lines
                var matchedPairs = matchPDFLinesToVRALines(
                    billGroup.lineItems,
                    vraResults.results,
                    billNumber
                );

                log.debug('Line Matching Complete', {
                    billNumber: billNumber,
                    matchedPairs: matchedPairs.length,
                    pdfLines: billGroup.lineItems.length,
                    vraLines: vraResults.results.length
                });

                if (matchedPairs.length === 0) {
                    log.debug('No Amount Matches Found', {
                        billNumber: billNumber,
                        orderNo: orderData.invoiceNumber,
                        pdfLineCount: billGroup.lineItems.length,
                        vraLineCount: vraResults.results.length
                    });

                    skipped.push({
                        orderNo: orderData.invoiceNumber,
                        billNumber: billNumber,
                        skipReason: 'No matching VRA lines by item name/amount',
                        skipType: 'NO_AMOUNT_MATCH',
                        nardaNumbers: billGroup.nardaNumbers || []
                    });
                    continue;
                }

                // Create vendor credit
                var vcResult = createGroupedVendorCredit(
                    orderData,
                    billGroup,
                    matchedPairs,
                    billNumber,
                    filteredCsvId
                );

                log.debug('Vendor Credit Creation Result', {
                    billNumber: billNumber,
                    success: vcResult.success,
                    isSkipped: vcResult.isSkipped || false,
                    hasVendorCreditId: !!vcResult.vendorCreditId,
                    skipReason: vcResult.skipReason || 'N/A'
                });

                // Handle result based on structure
                if (vcResult.success) {
                    // Check if it was skipped (no available VRA lines)
                    if (vcResult.isSkipped) {
                        log.debug('Vendor Credit Skipped', {
                            billNumber: billNumber,
                            skipReason: vcResult.skipReason,
                            skipType: vcResult.skipType
                        });

                        skipped.push({
                            orderNo: orderData.invoiceNumber,
                            billNumber: billNumber,
                            skipReason: vcResult.skipReason,
                            skipType: vcResult.skipType || 'NO_AVAILABLE_VRA_LINES',
                            nardaNumbers: billGroup.nardaNumbers || [],
                            matchingVRA: vcResult.matchingVRA
                        });
                    } else if (vcResult.vendorCreditId) {
                        // SUCCESS: Vendor Credit was created
                        log.audit('Vendor Credit Created Successfully', {
                            vendorCreditId: vcResult.vendorCreditId,
                            tranid: vcResult.tranid,
                            billNumber: vcResult.billNumber,
                            lineCount: vcResult.lineCount,
                            totalAmount: vcResult.totalAmount
                        });

                        vendorCredits.push({
                            vendorCreditId: vcResult.vendorCreditId,
                            tranid: vcResult.tranid,
                            billNumber: vcResult.billNumber,
                            lineCount: vcResult.lineCount,
                            nardaTypes: vcResult.nardaTypes || billGroup.nardaNumbers || [],
                            totalAmount: vcResult.totalAmount,
                            matchingVRA: vcResult.matchingVRA,
                            governanceUsed: vcResult.governanceUsed || 0
                        });
                    } else {
                        // SUCCESS flag but neither skipped nor created - shouldn't happen
                        log.error('Unexpected Vendor Credit Result Structure', {
                            billNumber: billNumber,
                            vcResult: vcResult
                        });

                        skipped.push({
                            orderNo: orderData.invoiceNumber,
                            billNumber: billNumber,
                            skipReason: 'Unexpected result structure - neither skipped nor created',
                            skipType: 'UNEXPECTED_RESULT',
                            nardaNumbers: billGroup.nardaNumbers || []
                        });
                    }
                } else {
                    // FAILURE: Creation error
                    log.error('Vendor Credit Creation Failed', {
                        billNumber: billNumber,
                        error: vcResult.error,
                        isDuplicate: vcResult.isDuplicate || false
                    });

                    skipped.push({
                        orderNo: orderData.invoiceNumber,
                        billNumber: billNumber,
                        skipReason: 'Vendor Credit creation failed: ' + vcResult.error,
                        skipType: vcResult.isDuplicate ? 'DUPLICATE' : 'CREATION_ERROR',
                        nardaNumbers: billGroup.nardaNumbers || []
                    });
                }
            }

            log.audit('Vendor Credit Group Processing Complete', {
                orderNo: orderData.invoiceNumber,
                totalBillNumbers: billNumbers.length,
                vendorCreditsCreated: vendorCredits.length,
                skippedCount: skipped.length
            });

            return {
                success: true,
                vendorCredits: vendorCredits,
                skipped: skipped
            };

        } catch (error) {
            log.error('Vendor Credit Group Processing Error', {
                error: error.toString(),
                orderNo: orderData.invoiceNumber,
                stack: error.stack
            });

            return {
                success: false,
                error: error.toString(),
                vendorCredits: [],
                skipped: []
            };
        }
    }

    /**
     * Consolidate vendor credit groups by original bill number
     * @param {Object} groupedLineItems - Line items grouped by NARDA
     * @returns {Object} Groups consolidated by bill number
     */
    function consolidateVendorCreditGroups(groupedLineItems) {
        var consolidated = {};

        var nardaNumbers = Object.keys(groupedLineItems);
        for (var i = 0; i < nardaNumbers.length; i++) {
            var nardaNumber = nardaNumbers[i];
            var nardaGroup = groupedLineItems[nardaNumber];

            // Skip non-VC types
            var nardaType = determineNardaType(nardaNumber);
            if (nardaType !== 'VC') continue;

            // Group by bill number
            for (var j = 0; j < nardaGroup.originalBillNumbers.length; j++) {
                var billNumber = nardaGroup.originalBillNumbers[j];

                if (!consolidated[billNumber]) {
                    consolidated[billNumber] = {
                        lineItems: [],
                        totalAmount: 0,
                        nardaNumbers: []
                    };
                }

                // Add line items for this bill number
                for (var k = 0; k < nardaGroup.lineItems.length; k++) {
                    var lineItem = nardaGroup.lineItems[k];
                    if (lineItem.originalBillNumber === billNumber) {
                        consolidated[billNumber].lineItems.push(lineItem);
                        consolidated[billNumber].totalAmount += lineItem.extendedPrice;
                    }
                }

                if (consolidated[billNumber].nardaNumbers.indexOf(nardaNumber) === -1) {
                    consolidated[billNumber].nardaNumbers.push(nardaNumber);
                }
            }
        }

        return consolidated;
    }

    /**
      * Search for matching VRA by original bill number
      * @param {string} originalBillNumber - Bill number to search
      * @returns {Object} Search results
      */
    function searchForMatchingVRA(originalBillNumber) {
        try {
            var vraSearch = search.create({
                type: search.Type.VENDOR_RETURN_AUTHORIZATION,
                filters: [
                    ['type', 'anyof', 'VendAuth'],
                    'AND',
                    ['memo', 'contains', originalBillNumber]
                ],
                columns: [
                    'tranid',
                    'internalid',
                    'memo',
                    'entity',
                    'status',
                    search.createColumn({
                        name: 'itemid',
                        join: 'item'
                    }), // CHANGED: Get item name/number instead of display name
                    'amount',
                    'line'
                ]
            });

            var results = [];
            vraSearch.run().each(function (result) {
                // CHANGED: Get the actual item name/number (part number)
                var itemNameNumber = result.getValue({
                    name: 'itemid',
                    join: 'item'
                });

                results.push({
                    internalId: result.getValue('internalid'),
                    tranid: result.getValue('tranid'),
                    memo: result.getValue('memo'),
                    entity: result.getValue('entity'),
                    status: result.getValue('status'),
                    item: itemNameNumber,  // Now contains the actual part number
                    amount: result.getValue('amount'),
                    lineNumber: result.getValue('line')
                });
                return true;
            });

            log.debug('VRA Search Results', {
                billNumber: originalBillNumber,
                totalResults: results.length,
                items: results.map(function (r) { return r.item; })
            });

            return {
                success: true,
                results: results
            };

        } catch (error) {
            log.error('VRA Search Error', {
                error: error.toString(),
                billNumber: originalBillNumber,
                stack: error.stack
            });

            return {
                success: false,
                error: error.toString(),
                results: []
            };
        }
    }

    /**
     * Match PDF lines to VRA lines by item and line number
     * Bill number is already validated via VRA search memo filter
     * @param {Array} pdfLines - PDF line items (from CSV)
     * @param {Array} vraLines - VRA line items (from search, already filtered by bill number)
     * @param {string} billNumber - Original bill number (for logging only)
     * @returns {Array} Matched pairs with pdfLine and vraLine
     */
    function matchPDFLinesToVRALines(pdfLines, vraLines, billNumber) {
        var matchedPairs = [];
        var usedVRALineKeys = {}; // Track by unique key: internalId + lineNumber

        log.debug('Starting Line Matching', {
            billNumber: billNumber,
            pdfLineCount: pdfLines.length,
            vraLineCount: vraLines.length
        });

        for (var i = 0; i < pdfLines.length; i++) {
            var pdfLine = pdfLines[i];
            var pdfPart = pdfLine.part ? pdfLine.part.trim() : null;
            var pdfAmount = pdfLine.extendedPrice;

            if (!pdfPart) {
                log.debug('PDF Line Missing Part', {
                    lineIndex: i,
                    description: pdfLine.description
                });
                continue;
            }

            log.debug('Matching PDF Line', {
                lineIndex: i,
                part: pdfPart,
                amount: pdfAmount
            });

            // Find matching VRA line by item name and unique line number
            for (var j = 0; j < vraLines.length; j++) {
                var vraLine = vraLines[j];

                // Create unique key for this VRA line
                var vraLineKey = vraLine.internalId + '_' + vraLine.lineNumber;

                // Skip if already used
                if (usedVRALineKeys[vraLineKey]) {
                    continue;
                }

                var vraItem = vraLine.item ? vraLine.item.trim() : null;
                var vraAmount = Math.abs(parseFloat(vraLine.amount));

                log.debug('Comparing VRA Line', {
                    vraLineIndex: j,
                    vraLineNumber: vraLine.lineNumber,
                    vraItem: vraItem,
                    vraAmount: vraAmount,
                    pdfPart: pdfPart,
                    alreadyUsed: !!usedVRALineKeys[vraLineKey]
                });

                // Match by item name only (bill number already validated by search)
                if (vraItem && pdfPart === vraItem) {
                    log.audit('Match Found', {
                        pdfPart: pdfPart,
                        vraItem: vraItem,
                        vraLineNumber: vraLine.lineNumber,
                        pdfAmount: pdfAmount,
                        vraAmount: vraAmount,
                        amountDifference: Math.abs(pdfAmount - vraAmount)
                    });

                    matchedPairs.push({
                        pdfLine: pdfLine,
                        vraLine: vraLine
                    });

                    // Mark this specific VRA line as used
                    usedVRALineKeys[vraLineKey] = true;
                    break;
                }
            }
        }

        log.audit('Line Matching Complete', {
            billNumber: billNumber,
            pdfLines: pdfLines.length,
            vraLines: vraLines.length,
            matches: matchedPairs.length,
            unmatchedPDFLines: pdfLines.length - matchedPairs.length,
            matchedVRALineNumbers: matchedPairs.map(function (p) {
                return p.vraLine.lineNumber;
            })
        });

        return matchedPairs;
    }

    /**
     * Create grouped vendor credit from matched pairs
     * @param {Object} orderData - Order extracted data
     * @param {Object} billGroup - Consolidated bill group data
     * @param {Array} matchedPairs - Matched line pairs
     * @param {string} billNumber - Bill number
     * @param {number} filteredCsvId - Filtered CSV file ID
     * @returns {Object} Creation result with governance tracking
     */
    function createGroupedVendorCredit(orderData, billGroup, matchedPairs, billNumber, filteredCsvId) {
        // GOVERNANCE: Track starting point
        var governanceBefore = runtime.getCurrentScript().getRemainingUsage();

        try {
            log.debug('Creating Grouped Vendor Credit', {
                orderNo: orderData.invoiceNumber,
                billNumber: billNumber,
                matchedPairs: matchedPairs.length,
                nardaTypes: billGroup.nardaNumbers ? billGroup.nardaNumbers.join('+') : 'VC'
            });

            // VALIDATION: Check if we have any matched pairs before proceeding
            if (matchedPairs.length === 0) {
                log.error('No Matched Pairs for Vendor Credit', {
                    orderNo: orderData.invoiceNumber,
                    billNumber: billNumber,
                    nardaTypes: billGroup.nardaNumbers ? billGroup.nardaNumbers.join('+') : 'VC',
                    totalAmount: billGroup.totalAmount
                });

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'VRA found but all lines already used/transformed - no available lines to credit',
                    skipType: 'NO_AVAILABLE_VRA_LINES',
                    billNumber: billNumber,
                    nardaNumber: billGroup.nardaNumbers ? billGroup.nardaNumbers.join('+') : 'VC',
                    orderData: orderData,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            // VALIDATION: Calculate expected total from CSV data
            var expectedTotal = 0;
            for (var i = 0; i < billGroup.lineItems.length; i++) {
                expectedTotal += billGroup.lineItems[i].extendedPrice;
            }

            // Calculate total from matched pairs (what will actually be on the VC)
            var calculatedTotal = 0;
            for (var i = 0; i < matchedPairs.length; i++) {
                calculatedTotal += matchedPairs[i].pdfLine.extendedPrice;
            }

            // CRITICAL VALIDATION: Verify calculated total matches expected total
            var totalDifference = Math.abs(calculatedTotal - expectedTotal);
            if (totalDifference > 0.01) {
                log.error('Vendor Credit Total Validation Failed', {
                    orderNo: orderData.invoiceNumber,
                    billNumber: billNumber,
                    calculatedTotal: calculatedTotal,
                    expectedTotal: expectedTotal,
                    difference: totalDifference,
                    matchedPairs: matchedPairs.length,
                    totalLineItems: billGroup.lineItems.length
                });

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'Vendor Credit total mismatch: Calculated ' +
                        formatCurrency(calculatedTotal) + ' vs Expected ' +
                        formatCurrency(expectedTotal) + ' (difference: ' +
                        formatCurrency(totalDifference) + ') - Not all CSV lines matched to VRA',
                    skipType: 'VC_TOTAL_MISMATCH',
                    billNumber: billNumber,
                    nardaNumber: billGroup.nardaNumbers ? billGroup.nardaNumbers.join('+') : 'VC',
                    orderData: orderData,
                    calculatedTotal: calculatedTotal,
                    expectedTotal: expectedTotal,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            log.audit('Vendor Credit Total Validation Passed', {
                orderNo: orderData.invoiceNumber,
                billNumber: billNumber,
                calculatedTotal: calculatedTotal,
                expectedTotal: expectedTotal,
                difference: totalDifference
            });

            // Get first VRA ID and details
            var vraId = matchedPairs[0].vraLine.internalId;
            var vraTranid = matchedPairs[0].vraLine.tranid;

            // Create tranid
            var tranid = orderData.invoiceNumber;

            // Check for duplicates
            var duplicateCheck = checkForDuplicateVendorCredit(tranid);
            if (!duplicateCheck.success) {
                return {
                    success: false,
                    error: 'Duplicate: ' + tranid,
                    isDuplicate: true,
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            // Transform VRA to Vendor Credit
            var vcDate = parseCSVDate(orderData.invoiceDate);

            var vendorCredit = record.transform({
                fromType: record.Type.VENDOR_RETURN_AUTHORIZATION,
                fromId: vraId,
                toType: record.Type.VENDOR_CREDIT,
                isDynamic: true
            });

            // Build memo with NARDA types
            var nardaTypesList = billGroup.nardaNumbers ? billGroup.nardaNumbers.join('+') : 'VC';
            var vcMemo = 'CSV Import: ' + nardaTypesList + ' Credit - ' + orderData.invoiceNumber +
                ' - Original Bill: ' + billNumber + ' - VRA: ' + vraTranid;

            vendorCredit.setValue({ fieldId: 'tranid', value: tranid });
            vendorCredit.setValue({ fieldId: 'trandate', value: vcDate });
            vendorCredit.setValue({ fieldId: 'memo', value: vcMemo });

            // Build map of VRA line numbers to CSV amounts and quantities
            var vraLineDataMap = {};
            for (var i = 0; i < matchedPairs.length; i++) {
                var csvAmount = matchedPairs[i].pdfLine.extendedPrice;
                var csvQuantity = parseInt(matchedPairs[i].pdfLine.quantity, 10);

                vraLineDataMap[matchedPairs[i].vraLine.lineNumber] = {
                    amount: csvAmount,
                    quantity: csvQuantity,
                    rate: csvQuantity > 0 ? csvAmount / csvQuantity : csvAmount
                };
            }

            log.debug('VRA Line Data Mapping', {
                billNumber: billNumber,
                mappings: vraLineDataMap
            });

            // Process item sublist
            var lineCount = vendorCredit.getLineCount({ sublistId: 'item' });
            var matchedLineNumbers = matchedPairs.map(function (p) {
                return p.vraLine.lineNumber;
            });

            // CRITICAL: Track how many lines remain after removal
            var remainingLineCount = 0;

            // Remove non-matching lines in reverse order
            for (var i = lineCount - 1; i >= 0; i--) {
                var lineKey = vendorCredit.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'line',
                    line: i
                });

                if (matchedLineNumbers.indexOf(lineKey) === -1) {
                    vendorCredit.removeLine({ sublistId: 'item', line: i });
                    log.debug('Removed Non-Matching Line', {
                        lineIndex: i,
                        lineKey: lineKey
                    });
                } else {
                    // This line will remain - update it with CSV values
                    var lineData = vraLineDataMap[lineKey];

                    vendorCredit.selectLine({ sublistId: 'item', line: i });

                    vendorCredit.setCurrentSublistValue({
                        sublistId: 'item',
                        fieldId: 'quantity',
                        value: lineData.quantity
                    });

                    vendorCredit.setCurrentSublistValue({
                        sublistId: 'item',
                        fieldId: 'rate',
                        value: lineData.rate
                    });

                    vendorCredit.setCurrentSublistValue({
                        sublistId: 'item',
                        fieldId: 'amount',
                        value: lineData.amount
                    });

                    vendorCredit.commitLine({ sublistId: 'item' });

                    var itemName = vendorCredit.getSublistValue({
                        sublistId: 'item',
                        fieldId: 'item_display',
                        line: i
                    });

                    log.debug('Updated Line Rate and Amount from VRA to CSV', {
                        lineIndex: i,
                        lineKey: lineKey,
                        itemName: itemName,
                        quantity: lineData.quantity,
                        vraRate: lineData.rate,
                        vraAmount: lineData.amount,
                        csvRate: lineData.rate,
                        csvAmount: lineData.amount,
                        rateCalculation: 'amount (' + lineData.amount + ') / quantity (' + lineData.quantity + ') = ' + lineData.rate
                    });

                    remainingLineCount++;
                }
            }

            // VALIDATION: Check if any lines remain after removal/update
            if (remainingLineCount === 0) {
                log.error('All VRA Lines Removed - Cannot Save Vendor Credit', {
                    orderNo: orderData.invoiceNumber,
                    billNumber: billNumber,
                    originalLineCount: lineCount,
                    matchedLineNumbers: matchedLineNumbers,
                    vraId: vraId
                });

                return {
                    success: true,
                    isSkipped: true,
                    skipReason: 'All VRA lines were already used/transformed - no lines available after matching',
                    skipType: 'NO_AVAILABLE_VRA_LINES',
                    billNumber: billNumber,
                    nardaNumber: nardaTypesList,
                    orderData: orderData,
                    matchingVRA: {
                        internalId: vraId,
                        tranid: vraTranid
                    },
                    governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
                };
            }

            // Save vendor credit
            var vcId = vendorCredit.save();

            // GOVERNANCE: Calculate usage
            var governanceAfter = runtime.getCurrentScript().getRemainingUsage();
            var governanceUsed = governanceBefore - governanceAfter;

            log.audit('Vendor Credit Created', {
                vcId: vcId,
                tranid: tranid,
                billNumber: billNumber,
                lineCount: matchedPairs.length,
                nardaTypes: nardaTypesList,
                totalAmount: calculatedTotal,
                validatedAmount: expectedTotal,
                memo: vcMemo,
                governanceUsed: governanceUsed,
                governanceRemaining: governanceAfter
            });

            // Attach filtered CSV
            if (filteredCsvId) {
                attachFileToRecord(vcId, filteredCsvId, record.Type.VENDOR_CREDIT);
            }

            return {
                success: true,
                vendorCreditId: vcId,
                tranid: tranid,
                billNumber: billNumber,
                lineCount: matchedPairs.length,
                nardaTypes: billGroup.nardaNumbers || [],
                totalAmount: calculatedTotal,
                matchingVRA: {
                    internalId: vraId,
                    tranid: vraTranid
                },
                governanceUsed: governanceUsed
            };

        } catch (error) {
            log.error('Vendor Credit Creation Error', {
                error: error.toString(),
                billNumber: billNumber,
                orderNo: orderData.invoiceNumber,
                stack: error.stack,
                governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
            });

            return {
                success: false,
                error: error.toString(),
                billNumber: billNumber,
                governanceUsed: governanceBefore - runtime.getCurrentScript().getRemainingUsage()
            };
        }
    }

    /**
     * Check for duplicate Vendor Credit by tranid
     * @param {string} tranid - Transaction ID
     * @returns {Object} Duplicate check result
     */
    function checkForDuplicateVendorCredit(tranid) {
        try {
            var vcSearch = search.create({
                type: search.Type.VENDOR_CREDIT,
                filters: [
                    ['type', 'anyof', 'VendCred'],
                    'AND',
                    ['tranid', 'is', tranid]
                ],
                columns: ['tranid', 'internalid']
            });

            var existing = [];
            vcSearch.run().each(function (result) {
                existing.push({
                    internalId: result.getValue('internalid'),
                    tranid: result.getValue('tranid')
                });
                return true;
            });

            if (existing.length > 0) {
                return {
                    success: false,
                    existingEntry: existing[0]
                };
            }

            return { success: true };

        } catch (error) {
            return {
                success: false,
                error: error.toString()
            };
        }
    }

    /**
     * Attach file to record using record.attach()
     * @param {number} recordId - Record internal ID
     * @param {number} fileId - File internal ID
     * @param {string} recordType - Record type constant
     */
    function attachFileToRecord(recordId, fileId, recordType) {
        try {
            record.attach({
                record: {
                    type: 'file',
                    id: fileId
                },
                to: {
                    type: recordType,
                    id: recordId
                }
            });

            log.debug('File Attached', {
                fileId: fileId,
                recordId: recordId,
                recordType: recordType
            });

        } catch (error) {
            log.error('File Attachment Error', {
                error: error.toString(),
                fileId: fileId,
                recordId: recordId
            });
        }
    }

    /**
     * Create filtered CSV file containing only rows for specific OrderNo
     * This CSV will be attached to created transactions
     * @param {Object} parsedData - Full parsed CSV data
     * @param {string} orderNo - OrderNo to filter
     * @returns {number} Created CSV file ID
     */
    function createFilteredCSV(parsedData, orderNo) {
        try {
            log.debug('Creating Filtered CSV', {
                orderNo: orderNo,
                totalRows: parsedData.totalRows
            });

            // Filter rows to this OrderNo
            var filteredRows = [];
            for (var i = 0; i < parsedData.rows.length; i++) {
                if (parsedData.rows[i].OrderNo === orderNo) {
                    filteredRows.push(parsedData.rows[i]);
                }
            }

            log.debug('Rows Filtered', {
                orderNo: orderNo,
                filteredCount: filteredRows.length
            });

            // Build CSV content
            var csvContent = '';

            // Add header row
            csvContent += parsedData.headers.join(',') + '\n';

            // Add filtered data rows
            for (var i = 0; i < filteredRows.length; i++) {
                var row = filteredRows[i];
                var values = [];

                // Build row in same order as headers
                for (var j = 0; j < parsedData.headers.length; j++) {
                    var header = parsedData.headers[j];
                    var value = row[header] || '';

                    // Escape commas and quotes in values
                    if (value.indexOf(',') !== -1 || value.indexOf('"') !== -1) {
                        value = '"' + value.replace(/"/g, '""') + '"';
                    }

                    values.push(value);
                }

                csvContent += values.join(',') + '\n';
            }

            // Create file name
            var fileName = 'Marcone_Warranty_' + orderNo + '.csv';

            // Create and save file
            var csvFile = file.create({
                name: fileName,
                fileType: file.Type.CSV,
                contents: csvContent,
                folder: CONFIG.FOLDERS.CSV_ATTACHMENTS
            });

            var fileId = csvFile.save();

            log.audit('Filtered CSV Created', {
                fileId: fileId,
                fileName: fileName,
                orderNo: orderNo,
                rowCount: filteredRows.length
            });

            return fileId;

        } catch (error) {
            log.error('Filtered CSV Creation Error', {
                error: error.toString(),
                orderNo: orderNo,
                stack: error.stack
            });

            return null;
        }
    }

    /**
     * Save unprocessed OrderNos to new CSV file for next script run
     * @param {Object} parsedData - Full CSV data
     * @param {Array} remainingOrderNos - OrderNos not processed this run
     * @returns {number} New CSV file ID
     */
    function saveUnprocessedCSV(parsedData, remainingOrderNos) {
        try {
            log.debug('Saving Unprocessed CSV', {
                remainingCount: remainingOrderNos.length
            });

            // Filter rows to remaining OrderNos
            var unprocessedRows = [];
            for (var i = 0; i < parsedData.rows.length; i++) {
                var row = parsedData.rows[i];
                if (remainingOrderNos.indexOf(row.OrderNo) !== -1) {
                    unprocessedRows.push(row);
                }
            }

            log.debug('Unprocessed Rows Filtered', {
                totalRows: unprocessedRows.length,
                orderNos: remainingOrderNos.length
            });

            // Build CSV content
            var csvContent = '';

            // Add header row
            csvContent += parsedData.headers.join(',') + '\n';

            // Add unprocessed data rows
            for (var i = 0; i < unprocessedRows.length; i++) {
                var row = unprocessedRows[i];
                var values = [];

                // Build row in same order as headers
                for (var j = 0; j < parsedData.headers.length; j++) {
                    var header = parsedData.headers[j];
                    var value = row[header] || '';

                    // Escape commas and quotes in values
                    if (value.indexOf(',') !== -1 || value.indexOf('"') !== -1) {
                        value = '"' + value.replace(/"/g, '""') + '"';
                    }

                    values.push(value);
                }

                csvContent += values.join(',') + '\n';
            }

            // Create file name with timestamp
            var timestamp = new Date().getTime();
            var fileName = 'Marcone_Warranty_Remaining_' + timestamp + '.csv';

            // Create and save file to source folder
            var csvFile = file.create({
                name: fileName,
                fileType: file.Type.CSV,
                contents: csvContent,
                folder: CONFIG.FOLDERS.CSV_SOURCE
            });

            var fileId = csvFile.save();

            log.audit('Unprocessed CSV Created', {
                fileId: fileId,
                fileName: fileName,
                rowCount: unprocessedRows.length,
                orderNosCount: remainingOrderNos.length
            });

            return fileId;

        } catch (error) {
            log.error('Unprocessed CSV Save Error', {
                error: error.toString(),
                remainingCount: remainingOrderNos.length,
                stack: error.stack
            });

            return null;
        }
    }

    /**
     * Move processed CSV file to completed folder
     * @param {number} fileId - Original CSV file ID
     */
    function moveProcessedCSV(fileId) {
        try {
            log.debug('Moving Processed CSV', { fileId: fileId });

            // Load original file
            var originalFile = file.load({ id: fileId });

            // Get current file details
            var fileName = originalFile.name;
            var fileContents = originalFile.getContents();

            log.debug('Original File Loaded', {
                fileId: fileId,
                fileName: fileName,
                currentFolder: originalFile.folder
            });

            // Create new file in processed folder
            var processedFile = file.create({
                name: fileName,
                fileType: file.Type.CSV,
                contents: fileContents,
                folder: CONFIG.FOLDERS.CSV_PROCESSED
            });

            var newFileId = processedFile.save();

            log.debug('File Copied to Processed Folder', {
                originalFileId: fileId,
                newFileId: newFileId,
                fileName: fileName
            });

            // Delete original file
            try {
                file.delete({ id: fileId });
                log.audit('Original File Deleted', {
                    fileId: fileId,
                    fileName: fileName
                });
            } catch (deleteError) {
                log.error('Original File Deletion Failed', {
                    error: deleteError.toString(),
                    fileId: fileId,
                    fileName: fileName
                });
                // Continue - file was copied successfully
            }

            log.audit('CSV File Moved Successfully', {
                originalId: fileId,
                newId: newFileId,
                fileName: fileName,
                fromFolder: CONFIG.FOLDERS.CSV_SOURCE,
                toFolder: CONFIG.FOLDERS.CSV_PROCESSED
            });

        } catch (error) {
            log.error('File Move Error', {
                error: error.toString(),
                fileId: fileId,
                stack: error.stack
            });
            // Don't throw - this is cleanup, shouldn't stop execution
        }
    }

    /**
     * Create CSV file containing all skipped line items with full original data
     * @param {Array} skippedEntries - Array of skipped transactions
     * @param {Object} parsedData - Full parsed CSV data with all rows
     * @returns {number} Created CSV file ID or null
     */
    function createSkippedItemsCSV(skippedEntries, parsedData) {
        try {
            log.debug('Creating Skipped Items CSV', {
                totalEntries: skippedEntries.length
            });

            // Build CSV content
            var csvContent = '';

            // Add header row - original CSV headers + skip info columns
            var headers = parsedData.headers.slice(); // Copy original headers
            headers.push('Skip Type');
            headers.push('Skip Reason');
            csvContent += headers.join(',') + '\n';

            // Process each skipped entry
            for (var i = 0; i < skippedEntries.length; i++) {
                var entry = skippedEntries[i];
                var orderNo = entry.orderNo;
                var nardaNumber = entry.nardaNumber;
                var billNumber = entry.billNumber;

                // Find all matching rows in original CSV
                for (var j = 0; j < parsedData.rows.length; j++) {
                    var row = parsedData.rows[j];

                    // Check if this row matches the skipped entry
                    var isMatch = false;

                    if (row.OrderNo === orderNo) {
                        // If NARDA number specified, check for match
                        if (nardaNumber) {
                            var rowNarda = row['NARDA Number'] ? row['NARDA Number'].trim() : '';
                            if (rowNarda === nardaNumber) {
                                isMatch = true;
                            }
                        }
                        // If bill number specified, check description
                        else if (billNumber) {
                            var rowBillNumber = extractOriginalBillNumber(row.Description);
                            if (rowBillNumber === billNumber) {
                                isMatch = true;
                            }
                        }
                        // If no specific NARDA/bill number, include all rows for this order
                        else {
                            isMatch = true;
                        }
                    }

                    if (isMatch) {
                        // Build row with original data + skip info
                        var rowValues = [];

                        // Add all original column values
                        for (var k = 0; k < parsedData.headers.length; k++) {
                            var header = parsedData.headers[k];
                            var value = row[header] || '';
                            rowValues.push(escapeCSVValue(value));
                        }

                        // Add skip info columns
                        rowValues.push(escapeCSVValue(entry.skipType || 'UNKNOWN'));
                        rowValues.push(escapeCSVValue(entry.skipReason || ''));

                        csvContent += rowValues.join(',') + '\n';
                    }
                }
            }

            // Create file name with timestamp
            var timestamp = new Date();
            var dateStr = timestamp.getFullYear() +
                ('0' + (timestamp.getMonth() + 1)).slice(-2) +
                ('0' + timestamp.getDate()).slice(-2) + '_' +
                ('0' + timestamp.getHours()).slice(-2) +
                ('0' + timestamp.getMinutes()).slice(-2);

            var fileName = 'Marcone_Warranty_Skipped_Items_' + dateStr + '.csv';

            // Create and save file
            var csvFile = file.create({
                name: fileName,
                fileType: file.Type.CSV,
                contents: csvContent,
                folder: CONFIG.FOLDERS.CSV_PROCESSED
            });

            var fileId = csvFile.save();

            log.audit('Skipped Items CSV Created', {
                fileId: fileId,
                fileName: fileName,
                entryCount: skippedEntries.length
            });

            return fileId;

        } catch (error) {
            log.error('Skipped Items CSV Creation Error', {
                error: error.toString(),
                entryCount: skippedEntries.length,
                stack: error.stack
            });

            return null;
        }
    }

    /**
     * Escape CSV value - handle commas, quotes, and newlines
     * @param {string} value - Value to escape
     * @returns {string} Escaped value
     */
    function escapeCSVValue(value) {
        if (!value) return '';

        var stringValue = String(value);

        // If value contains comma, quote, or newline, wrap in quotes and escape quotes
        if (stringValue.indexOf(',') !== -1 ||
            stringValue.indexOf('"') !== -1 ||
            stringValue.indexOf('\n') !== -1) {
            return '"' + stringValue.replace(/"/g, '""') + '"';
        }

        return stringValue;
    }

    /**
 * Send email with processing results summary
 * @param {Object} stats - Processing statistics
 * @param {number} unprocessedFileId - Unprocessed CSV file ID (if any)
 */
    function sendResultsEmail(stats, unprocessedFileId) {
        try {
            log.debug('Preparing Results Email', {
                processedOrders: stats.processedOrderNos,
                journalEntries: stats.journalEntriesCreated,
                vendorCredits: stats.vendorCreditsCreated,
                skippedEntries: stats.skippedEntries.length
            });

            // Get email recipient from script parameter
            var recipientId = runtime.getCurrentScript().getParameter({
                name: 'custscript_csv_bc_process_email_recip'
            });

            if (!recipientId) {
                log.error('Email Recipient Missing', 'Script parameter custscript_csv_bc_process_email_recip not set');
                return;
            }

            // Build email subject
            var subject = buildEmailSubject(stats);

            // Build email body
            var emailBody = buildEmailBody(stats, unprocessedFileId);

            // Create skipped items CSV file if there are any skipped entries
            var attachments = [];
            var skippedCsvFileId = null;

            if (stats.skippedEntries.length > 0) {
                skippedCsvFileId = createSkippedItemsCSV(stats.skippedEntries, stats.parsedData);

                if (skippedCsvFileId) {
                    var skippedCsvFile = file.load({ id: skippedCsvFileId });
                    attachments.push(skippedCsvFile);

                    log.debug('Skipped Items CSV Attached', {
                        fileId: skippedCsvFileId,
                        fileName: skippedCsvFile.name
                    });
                }
            }

            // Send email with attachments
            var emailConfig = {
                author: 151135,
                recipients: recipientId,
                subject: subject,
                body: emailBody
            };

            if (attachments.length > 0) {
                emailConfig.attachments = attachments;
            }

            email.send(emailConfig);

            log.audit('Results Email Sent', {
                recipient: recipientId,
                author: 151135,
                subject: subject,
                attachmentCount: attachments.length,
                skippedCsvFileId: skippedCsvFileId
            });

        } catch (error) {
            log.error('Email Send Error', {
                error: error.toString(),
                stack: error.stack
            });
        }
    }

    /**
     * Build email subject line based on processing results
     * @param {Object} stats - Processing statistics
     * @returns {string} Email subject
     */
    function buildEmailSubject(stats) {
        var status = 'SUCCESS';

        if (stats.failedEntries.length > 0 || stats.validationFailures > 0) {
            status = 'PARTIAL';
        }

        if (stats.processedOrderNos === 0) {
            status = 'FAILED';
        }

        return 'Marcone Warranty CSV Processing - ' + status +
            ' (' + stats.processedOrderNos + ' of ' + stats.totalOrderNos + ' orders)';
    }

    /**
     * Build complete email body with all processing details
     * @param {Object} stats - Processing statistics
     * @param {number} unprocessedFileId - Unprocessed CSV file ID (if any)
     * @returns {string} Email body text
     */
    function buildEmailBody(stats, unprocessedFileId) {
        var body = '';

        // Header
        body += 'MARCONE PRODUCT WARRANTY CSV PROCESSING RESULTS\n';
        body += '===============================================\n\n';

        // Summary Section
        body += buildSummarySection(stats, unprocessedFileId);

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

        // Failed Entries Section
        if (stats.failedEntries.length > 0) {
            body += '\n\n' + buildFailedEntriesSection(stats.failedEntries);
        }

        // Next Steps Section
        body += '\n\n' + buildNextStepsSection(stats, unprocessedFileId);

        // Footer
        body += '\n\n' + buildEmailFooter();

        return body;
    }

    /**
     * Build summary section of email
     * @param {Object} stats - Processing statistics
     * @param {number} unprocessedFileId - Unprocessed CSV file ID
     * @returns {string} Summary section text
     */
    function buildSummarySection(stats, unprocessedFileId) {
        var summary = 'PROCESSING SUMMARY\n';
        summary += '------------------\n';
        summary += 'Total OrderNos Found:        ' + stats.totalOrderNos + '\n';
        summary += 'OrderNos Processed:          ' + stats.processedOrderNos + '\n';
        summary += 'Journal Entries Created:     ' + stats.journalEntriesCreated + '\n';
        summary += 'Vendor Credits Created:      ' + stats.vendorCreditsCreated + '\n';
        summary += 'Validation Failures:         ' + stats.validationFailures + '\n';
        summary += 'Skipped Transactions:        ' + stats.skippedTransactions + '\n';
        summary += 'Failed Entries:              ' + stats.failedEntries.length + '\n';

        if (unprocessedFileId) {
            summary += '\nUnprocessed File Created:    File ID ' + unprocessedFileId + '\n';
            summary += 'Remaining Orders:            ' +
                (stats.totalOrderNos - stats.processedOrderNos) + '\n';
        }

        if (stats.skippedEntries.length > 0) {
            summary += '\nSkipped Items CSV:           See attachment for detailed line items\n';
        }

        return summary;
    }

    /**
     * Build detailed Journal Entries section with URLs and amounts
     * @param {Array} processedDetails - Array of all created transactions
     * @returns {string} Journal entries section text
     */
    function buildJournalEntriesSection(processedDetails) {
        var section = 'JOURNAL ENTRIES CREATED\n';
        section += '-----------------------\n';

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
            section += '   Transaction ID:  ' + je.tranid + '\n';
            section += '   Internal ID:     ' + je.journalEntryId + '\n';
            section += '   URL:             ' + nsUrl + '/app/accounting/transactions/journal.nl?id=' +
                je.journalEntryId + '\n';

            // ENHANCED: Show NARDA classification details
            if (je.nardaNumbers && je.nardaNumbers.length > 1) {
                section += '   NARDA Type:      Multiple Journal Entry NARDAs (Consolidated)\n';
                section += '   NARDA Numbers:   ' + je.nardaNumbers.join(', ') + '\n';
                section += '   NARDA Count:     ' + je.nardaNumbers.length + '\n';
            } else if (je.nardaNumber) {
                section += '   NARDA Type:      Journal Entry (J# or INV#)\n';
                section += '   NARDA Number:    ' + je.nardaNumber + '\n';
            }

            // ENHANCED: Show dollar amount
            section += '   Total Amount:    ' + formatCurrency(je.totalAmount) + '\n';
        }

        return section;
    }

    /**
     * Build detailed Vendor Credits section with URLs, amounts, and VRA references
     * @param {Array} processedDetails - Array of all created transactions
     * @returns {string} Vendor credits section text
     */
    function buildVendorCreditsSection(processedDetails) {
        var section = 'VENDOR CREDITS CREATED\n';
        section += '----------------------\n';

        // Filter to only Vendor Credits
        var vendorCredits = [];
        for (var i = 0; i < processedDetails.length; i++) {
            if (processedDetails[i].vendorCreditId) {
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
            section += '   Transaction ID:        ' + vc.tranid + '\n';
            section += '   Internal ID:           ' + vc.vendorCreditId + '\n';
            section += '   URL:                   ' + nsUrl + '/app/accounting/transactions/vendcred.nl?id=' +
                vc.vendorCreditId + '\n';

            // ENHANCED: Show NARDA classification
            if (vc.nardaTypes && vc.nardaTypes.length > 0) {
                section += '   NARDA Type:            Vendor Credit (' + vc.nardaTypes.join('+') + ')\n';
                if (vc.nardaTypes.length > 1) {
                    section += '   NARDA Count:           ' + vc.nardaTypes.length + ' types consolidated\n';
                }
            }

            section += '   Original Bill Number:  ' + vc.billNumber + '\n';
            section += '   Lines Matched:         ' + vc.lineCount + '\n';

            // ENHANCED: Show dollar amount
            section += '   Total Amount:          ' + formatCurrency(vc.totalAmount) + '\n';

            // Add VRA reference if available
            if (vc.matchingVRA) {
                section += '   Source VRA ID:         ' + vc.matchingVRA.internalId + '\n';
                section += '   Source VRA #:          ' + vc.matchingVRA.tranid + '\n';
                section += '   VRA URL:               ' + nsUrl + '/app/accounting/transactions/vendauth.nl?id=' +
                    vc.matchingVRA.internalId + '\n';
            }
        }

        return section;
    }

    /**
     * Build skipped transactions section with categorization
     * @param {Array} skippedEntries - Array of skipped transactions
     * @returns {string} Skipped section text
     */
    function buildSkippedTransactionsSection(skippedEntries) {
        var section = 'SKIPPED TRANSACTIONS (Manual Processing Required)\n';
        section += '---------------------------------------------------\n';
        section += '\nA detailed CSV with all line items is attached to this email.\n';

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

            section += '\n' + getSkipTypeDescription(skipType) + ' (' + entries.length + '):\n';

            for (var j = 0; j < entries.length; j++) {
                var entry = entries[j];
                section += '  - OrderNo: ' + entry.orderNo;

                if (entry.nardaNumber) {
                    section += ', NARDA: ' + entry.nardaNumber;
                }

                if (entry.billNumber) {
                    section += ', Original Bill Number: ' + entry.billNumber;
                }

                section += '\n    Reason: ' + entry.skipReason + '\n';
            }
        }

        return section;
    }

    /**
     * Get human-readable description for skip type
     * @param {string} skipType - Skip type code
     * @returns {string} Description
     */
    function getSkipTypeDescription(skipType) {
        var descriptions = {
            'NARDA_SKIP_PATTERN': 'NARDA Pattern Requires Manual Review',
            'NO_VRA': 'No Vendor Return Authorization Found',
            'NO_AMOUNT_MATCH': 'VRA Found But No Matching Line Amounts',
            'NO_AVAILABLE_VRA_LINES': 'VRA Found But All Lines Already Used/Transformed', // NEW
            'UNKNOWN': 'Unknown/Other Issues'
        };

        return descriptions[skipType] || skipType;
    }

    /**
     * Build failed entries section
     * @param {Array} failedEntries - Array of failed processing attempts
     * @returns {string} Failed section text
     */
    function buildFailedEntriesSection(failedEntries) {
        var section = 'FAILED ENTRIES (Require Investigation)\n';
        section += '---------------------------------------\n';

        for (var i = 0; i < failedEntries.length; i++) {
            var entry = failedEntries[i];

            section += '\n' + (i + 1) + '. OrderNo: ' + entry.orderNo + '\n';
            section += '   Error:       ' + entry.error + '\n';

            if (entry.skipReason) {
                section += '   Skip Reason: ' + entry.skipReason + '\n';
            }
        }

        return section;
    }

    /**
     * Build next steps section with action items
     * @param {Object} stats - Processing statistics
     * @param {number} unprocessedFileId - Unprocessed CSV file ID
     * @returns {string} Next steps text
     */
    function buildNextStepsSection(stats, unprocessedFileId) {
        var steps = 'NEXT STEPS\n';
        steps += '----------\n';

        var actionItems = [];

        // Script will auto-run for remaining orders
        if (unprocessedFileId) {
            actionItems.push('Script will automatically process remaining ' +
                (stats.totalOrderNos - stats.processedOrderNos) +
                ' orders in next scheduled run');
        }

        // Manual review for skipped transactions
        if (stats.skippedTransactions > 0) {
            actionItems.push('Review attached CSV for ' + stats.skippedTransactions +
                ' skipped line items and process manually as needed');
            actionItems.push('Common skip reasons: NARDA pattern requires review, ' +
                'no matching VRA found, or VRA line amounts don\'t match');
        }

        // Investigation for failed entries
        if (stats.failedEntries.length > 0) {
            actionItems.push('Investigate ' + stats.failedEntries.length +
                ' failed entries and correct underlying issues');
        }

        // Validation review
        if (stats.validationFailures > 0) {
            actionItems.push('Review ' + stats.validationFailures +
                ' validation failures - verify CSV data accuracy (line totals vs order totals)');
        }

        // Success message
        if (actionItems.length === 0) {
            actionItems.push('All orders processed successfully - no action required');
        }

        for (var i = 0; i < actionItems.length; i++) {
            steps += '\n' + (i + 1) + '. ' + actionItems[i];
        }

        return steps;
    }

    /**
     * Build email footer with script information
     * @returns {string} Footer text
     */
    function buildEmailFooter() {
        var footer = '\n---\n';
        footer += 'Generated by: Marcone Product Warranty CSV Processing Script\n';
        footer += 'Execution Time: ' + new Date().toString() + '\n';
        footer += 'Script ID: ' + runtime.getCurrentScript().id + '\n';
        footer += 'Deployment ID: ' + runtime.getCurrentScript().deploymentId;

        return footer;
    }


    return {
        execute: execute
    };
});