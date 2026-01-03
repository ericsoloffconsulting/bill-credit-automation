/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 * @NModuleScope SameAccount
 * 
 * AP Assist - Transaction Dashboard
 * 
 * Displays comprehensive dashboard of all JSON files across all vendors
 * Shows processed, skipped, and unprocessed files with detailed metrics
 */

define(['N/ui/serverWidget', 'N/file', 'N/search', 'N/log', 'N/url', 'N/redirect', 'N/runtime', 'N/record'], 
function(serverWidget, file, search, log, url, redirect, runtime, record) {

    // ===========================
    // CONFIGURATION
    // ===========================
    
    var CONFIG_RECORD_TYPE = 'customrecord_ap_assist_vend_config';

    // ===========================
    // MAIN SUITELET HANDLER
    // ===========================
    
    function onRequest(context) {
        try {
            if (context.request.method === 'GET') {
                var params = context.request.parameters;
                
                // Check if viewing specific file details
                if (params.fileId) {
                    displayFileDetails(context, params.fileId);
                } else if (params.vendorId && params.folderType) {
                    // Show detailed folder view for a specific vendor
                    displayVendorFolderDetails(context, params.vendorId, params.folderType);
                } else {
                    // Show main dashboard
                    displayDashboard(context);
                }
            } else if (context.request.method === 'POST') {
                // Handle back button - redirect to main dashboard
                var currentScript = runtime.getCurrentScript();
                redirect.toSuitelet({
                    scriptId: currentScript.id,
                    deploymentId: currentScript.deploymentId
                });
            }
        } catch (error) {
            log.error('Suitelet Error', {
                error: error.toString(),
                stack: error.stack
            });
            
            var errorForm = serverWidget.createForm({
                title: 'Error - AP Assist Transaction Dashboard'
            });
            
            var errorField = errorForm.addField({
                id: 'custpage_error',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Error'
            });
            
            errorField.defaultValue = '<div style="color: red; padding: 20px;">' +
                '<h2>Error Loading Dashboard</h2>' +
                '<p>' + error.toString() + '</p>' +
                '</div>';
            
            context.response.writePage(errorForm);
        }
    }

    // ===========================
    // DISPLAY MAIN DASHBOARD
    // ===========================
    
    function displayDashboard(context) {
        var form = serverWidget.createForm({
            title: 'AP Assist - Transaction Dashboard'
        });
        
        // Load all vendor configurations
        var vendorConfigs = loadVendorConfigurations();
        
        log.audit('Dashboard Loaded', {
            vendorCount: vendorConfigs.length
        });
        
        // Build complete dashboard HTML - header + all vendors in one field
        var dashboardHtml = '<div style="width: 100%;">';
        
        // Add dashboard header
        dashboardHtml += '<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; margin-bottom: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">' +
            '<h1 style="margin: 0 0 10px 0; font-size: 2em;">AP Assist Transaction Dashboard</h1>' +
            '<p style="margin: 0; font-size: 1.1em; opacity: 0.9;">Monitor all vendor processing activity in real-time</p>' +
            '<p style="margin: 10px 0 0 0; font-size: 0.9em;"><strong>Active Vendors:</strong> ' + vendorConfigs.length + '</p>' +
            '</div>';
        
        // Add all vendor sections
        for (var i = 0; i < vendorConfigs.length; i++) {
            var config = vendorConfigs[i];
            dashboardHtml += buildVendorDashboardSection(config);
        }
        
        dashboardHtml += '</div>';
        
        // Add single field containing entire dashboard
        var dashboardField = form.addField({
            id: 'custpage_dashboard',
            type: serverWidget.FieldType.INLINEHTML,
            label: ' '
        });
        
        dashboardField.defaultValue = dashboardHtml;
        
        context.response.writePage(form);
    }

    // ===========================
    // LOAD VENDOR CONFIGURATIONS
    // ===========================
    
    function loadVendorConfigurations() {
        var configs = [];
        
        var configSearch = search.create({
            type: CONFIG_RECORD_TYPE,
            filters: [],
            columns: [
                'internalid',
                'custrecord_ap_assist_vendor',
                'custrecord_ap_assist_json_folder_id',
                'custrecord_ap_assist_json_processed_fold',
                'custrecord_ap_assist_json_skipped_fold'
            ]
        });
        
        configSearch.run().each(function(result) {
            var vendorId = result.getValue('custrecord_ap_assist_vendor');
            var vendorName = result.getText('custrecord_ap_assist_vendor');
            
            configs.push({
                id: result.id,
                vendorId: vendorId,
                vendorName: vendorName || 'Unknown Vendor',
                unprocessedFolderId: result.getValue('custrecord_ap_assist_json_folder_id'),
                processedFolderId: result.getValue('custrecord_ap_assist_json_processed_fold'),
                skippedFolderId: result.getValue('custrecord_ap_assist_json_skipped_fold')
            });
            
            return true;
        });
        
        return configs;
    }

    // ===========================
    // BUILD VENDOR DASHBOARD SECTION
    // ===========================
    
    function buildVendorDashboardSection(config) {
        // Get file counts for each folder
        var unprocessedCount = config.unprocessedFolderId ? getJSONFileCount(config.unprocessedFolderId) : 0;
        var processedCount = config.processedFolderId ? getJSONFileCount(config.processedFolderId) : 0;
        var skippedCount = config.skippedFolderId ? getJSONFileCount(config.skippedFolderId) : 0;
        var totalCount = unprocessedCount + processedCount + skippedCount;
        
        // Get current script info for building links
        var currentScript = runtime.getCurrentScript();
        var scriptId = currentScript.id;
        var deploymentId = currentScript.deploymentId;
        
        var html = '<div style="background: #fff; border: 2px solid #e0e0e0; border-radius: 8px; padding: 25px; margin-bottom: 25px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); width: 100%; display: block; clear: both;">';
        
        // Vendor header
        html += '<div style="border-bottom: 2px solid #667eea; padding-bottom: 15px; margin-bottom: 20px;">';
        html += '<h2 style="margin: 0; color: #333; font-size: 1.5em;">' + escapeHtml(config.vendorName) + '</h2>';
        html += '<p style="margin: 5px 0 0 0; color: #666; font-size: 0.9em;">Configuration ID: ' + config.id + ' | Vendor ID: ' + config.vendorId + '</p>';
        html += '</div>';
        
        // Stats grid
        html += '<div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 20px;">';
        
        // Total files card
        html += '<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center;">';
        html += '<div style="font-size: 2.5em; font-weight: bold; margin-bottom: 5px;">' + totalCount + '</div>';
        html += '<div style="font-size: 0.9em; opacity: 0.9;">Total Files</div>';
        html += '</div>';
        
        // Unprocessed files card
        var unprocessedUrl = url.resolveScript({
            scriptId: scriptId,
            deploymentId: deploymentId,
            params: {
                vendorId: config.id,
                folderType: 'unprocessed'
            }
        });
        
        html += '<div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); color: white; padding: 20px; border-radius: 8px; text-align: center;">';
        html += '<div style="font-size: 2.5em; font-weight: bold; margin-bottom: 5px;">' + unprocessedCount + '</div>';
        html += '<div style="font-size: 0.9em; opacity: 0.9; margin-bottom: 10px;">Unprocessed</div>';
        if (unprocessedCount > 0) {
            html += '<a href="' + unprocessedUrl + '" style="color: white; text-decoration: underline; font-size: 0.85em;">View Details</a>';
        }
        html += '</div>';
        
        // Processed files card
        var processedUrl = url.resolveScript({
            scriptId: scriptId,
            deploymentId: deploymentId,
            params: {
                vendorId: config.id,
                folderType: 'processed'
            }
        });
        
        html += '<div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color: white; padding: 20px; border-radius: 8px; text-align: center;">';
        html += '<div style="font-size: 2.5em; font-weight: bold; margin-bottom: 5px;">' + processedCount + '</div>';
        html += '<div style="font-size: 0.9em; opacity: 0.9; margin-bottom: 10px;">Processed</div>';
        if (processedCount > 0) {
            html += '<a href="' + processedUrl + '" style="color: white; text-decoration: underline; font-size: 0.85em;">View Details</a>';
        }
        html += '</div>';
        
        // Skipped files card
        var skippedUrl = url.resolveScript({
            scriptId: scriptId,
            deploymentId: deploymentId,
            params: {
                vendorId: config.id,
                folderType: 'skipped'
            }
        });
        
        html += '<div style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); color: white; padding: 20px; border-radius: 8px; text-align: center;">';
        html += '<div style="font-size: 2.5em; font-weight: bold; margin-bottom: 5px;">' + skippedCount + '</div>';
        html += '<div style="font-size: 0.9em; opacity: 0.9; margin-bottom: 10px;">Skipped</div>';
        if (skippedCount > 0) {
            html += '<a href="' + skippedUrl + '" style="color: white; text-decoration: underline; font-size: 0.85em;">View Details</a>';
        }
        html += '</div>';
        
        html += '</div>';
        
        // Folder IDs reference
        html += '<div style="background: #f5f5f5; padding: 15px; border-radius: 5px; font-size: 0.85em; color: #666;">';
        html += '<strong>Folder IDs:</strong> ';
        html += 'Unprocessed: ' + (config.unprocessedFolderId || 'Not set') + ' | ';
        html += 'Processed: ' + (config.processedFolderId || 'Not set') + ' | ';
        html += 'Skipped: ' + (config.skippedFolderId || 'Not set');
        html += '</div>';
        
        html += '</div>';
        
        return html;
    }

    // ===========================
    // GET JSON FILE COUNT
    // ===========================
    
    function getJSONFileCount(folderId) {
        if (!folderId) {
            return 0;
        }
        
        try {
            var fileSearch = search.create({
                type: 'file',
                filters: [
                    ['folder', 'anyof', folderId],
                    'AND',
                    ['filetype', 'is', 'JSON']
                ],
                columns: ['internalid']
            });
            
            var count = 0;
            fileSearch.run().each(function() {
                count++;
                return true;
            });
            
            return count;
        } catch (error) {
            log.error('Error counting files in folder', {
                folderId: folderId,
                error: error.toString()
            });
            return 0;
        }
    }

    // ===========================
    // DISPLAY VENDOR FOLDER DETAILS
    // ===========================
    
    function displayVendorFolderDetails(context, configId, folderType) {
        // Load vendor configuration
        var configRecord = record.load({
            type: CONFIG_RECORD_TYPE,
            id: configId
        });
        
        var vendorName = configRecord.getText('custrecord_ap_assist_vendor');
        var folderId;
        var folderName;
        
        if (folderType === 'unprocessed') {
            folderId = configRecord.getValue('custrecord_ap_assist_json_folder_id');
            folderName = 'Unprocessed Files';
        } else if (folderType === 'processed') {
            folderId = configRecord.getValue('custrecord_ap_assist_json_processed_fold');
            folderName = 'Processed Files';
        } else if (folderType === 'skipped') {
            folderId = configRecord.getValue('custrecord_ap_assist_json_skipped_fold');
            folderName = 'Skipped Files';
        }
        
        var form = serverWidget.createForm({
            title: 'AP Assist - ' + vendorName + ' - ' + folderName
        });
        
        // Add back button
        form.addSubmitButton({
            label: 'Back to Dashboard'
        });
        
        // Search for JSON files in folder
        var jsonFiles = findJSONFilesInFolder(folderId);
        
        log.audit('Vendor Folder Details', {
            vendor: vendorName,
            folderType: folderType,
            folderId: folderId,
            fileCount: jsonFiles.length
        });
        
        // Add summary field
        var summaryField = form.addField({
            id: 'custpage_summary',
            type: serverWidget.FieldType.INLINEHTML,
            label: 'Summary'
        });
        
        var statusColor;
        if (folderType === 'unprocessed') {
            statusColor = '#f5576c';
        } else if (folderType === 'processed') {
            statusColor = '#00f2fe';
        } else {
            statusColor = '#fee140';
        }
        
        var summaryHtml = '<div style="background: ' + statusColor + '; color: white; padding: 20px; margin-bottom: 20px; border-radius: 8px;">' +
            '<h2 style="margin: 0 0 10px 0;">' + escapeHtml(vendorName) + ' - ' + folderName + '</h2>' +
            '<p style="margin: 0;"><strong>Total Files:</strong> ' + jsonFiles.length + '</p>' +
            '<p style="margin: 5px 0 0 0; font-size: 0.9em;">Folder ID: ' + folderId + '</p>' +
            '</div>';
        
        summaryField.defaultValue = summaryHtml;
        
        // Create sublist for displaying files
        var sublist = form.addSublist({
            id: 'custpage_files_sublist',
            type: serverWidget.SublistType.LIST,
            label: 'Files'
        });
        
        // Add columns
        sublist.addField({
            id: 'custpage_view',
            type: serverWidget.FieldType.TEXT,
            label: 'View'
        });
        
        sublist.addField({
            id: 'custpage_file_id',
            type: serverWidget.FieldType.TEXT,
            label: 'File ID'
        });
        
        sublist.addField({
            id: 'custpage_invoice_number',
            type: serverWidget.FieldType.TEXT,
            label: 'Invoice Number'
        });
        
        sublist.addField({
            id: 'custpage_invoice_date',
            type: serverWidget.FieldType.TEXT,
            label: 'Invoice Date'
        });
        
        sublist.addField({
            id: 'custpage_credit_type',
            type: serverWidget.FieldType.TEXT,
            label: 'Credit Type'
        });
        
        sublist.addField({
            id: 'custpage_po_number',
            type: serverWidget.FieldType.TEXT,
            label: 'PO/VRMA Number'
        });
        
        sublist.addField({
            id: 'custpage_document_total',
            type: serverWidget.FieldType.TEXT,
            label: 'Document Total'
        });
        
        if (folderType === 'skipped') {
            sublist.addField({
                id: 'custpage_skip_reason',
                type: serverWidget.FieldType.TEXT,
                label: 'Skip Reason'
            });
        }
        
        sublist.addField({
            id: 'custpage_processed_date',
            type: serverWidget.FieldType.TEXT,
            label: folderType === 'unprocessed' ? 'Created Date' : 'Processed Date'
        });
        
        sublist.addField({
            id: 'custpage_file_name',
            type: serverWidget.FieldType.TEXT,
            label: 'File Name'
        });
        
        sublist.addField({
            id: 'custpage_line_count',
            type: serverWidget.FieldType.INTEGER,
            label: 'Line Count'
        });
        
        // Get current script info dynamically
        var currentScript = runtime.getCurrentScript();
        var scriptId = currentScript.id;
        var deploymentId = currentScript.deploymentId;
        
        // Process each file and add to sublist
        var line = 0;
        for (var i = 0; i < jsonFiles.length; i++) {
            try {
                var jsonFile = jsonFiles[i];
                var jsonData = parseJSONFile(jsonFile.id);
                
                if (!jsonData) {
                    continue;
                }
                
                // Create view link
                var scriptUrl = url.resolveScript({
                    scriptId: scriptId,
                    deploymentId: deploymentId,
                    params: {
                        fileId: jsonFile.id
                    }
                });
                
                var viewLink = '<a href="' + scriptUrl + '" target="_blank" style="color: #0066cc; text-decoration: underline;">View Details</a>';
                
                sublist.setSublistValue({
                    id: 'custpage_view',
                    line: line,
                    value: viewLink
                });
                
                sublist.setSublistValue({
                    id: 'custpage_file_id',
                    line: line,
                    value: jsonFile.id.toString()
                });
                
                sublist.setSublistValue({
                    id: 'custpage_invoice_number',
                    line: line,
                    value: jsonData.invoiceNumber || ''
                });
                
                sublist.setSublistValue({
                    id: 'custpage_invoice_date',
                    line: line,
                    value: jsonData.invoiceDate || ''
                });
                
                sublist.setSublistValue({
                    id: 'custpage_credit_type',
                    line: line,
                    value: jsonData.creditType || ''
                });
                
                sublist.setSublistValue({
                    id: 'custpage_po_number',
                    line: line,
                    value: jsonData.poNumber || ''
                });
                
                sublist.setSublistValue({
                    id: 'custpage_document_total',
                    line: line,
                    value: jsonData.documentTotal || ''
                });
                
                if (folderType === 'skipped') {
                    var skipReason = '';
                    if (jsonData._processingMetadata && jsonData._processingMetadata.skipReason) {
                        skipReason = jsonData._processingMetadata.skipReason;
                    }
                    
                    sublist.setSublistValue({
                        id: 'custpage_skip_reason',
                        line: line,
                        value: skipReason
                    });
                }
                
                var processedDate = '';
                if (folderType === 'unprocessed') {
                    processedDate = formatDate(jsonFile.created);
                } else if (jsonData._processingMetadata && jsonData._processingMetadata.processedDate) {
                    processedDate = formatDate(jsonData._processingMetadata.processedDate);
                }
                
                sublist.setSublistValue({
                    id: 'custpage_processed_date',
                    line: line,
                    value: processedDate
                });
                
                sublist.setSublistValue({
                    id: 'custpage_file_name',
                    line: line,
                    value: jsonFile.name
                });
                
                var lineCount = jsonData.lineItems ? jsonData.lineItems.length : 0;
                sublist.setSublistValue({
                    id: 'custpage_line_count',
                    line: line,
                    value: lineCount.toString()
                });
                
                line++;
                
            } catch (error) {
                log.error('Error processing file', {
                    fileId: jsonFiles[i].id,
                    fileName: jsonFiles[i].name,
                    error: error.toString()
                });
            }
        }
        
        context.response.writePage(form);
    }

    // ===========================
    // DISPLAY SKIPPED FILES LIST (LEGACY - KEEP FOR COMPATIBILITY)
    // ===========================
    
    function displaySkippedList(context) {
        // Redirect to new dashboard
        displayDashboard(context);
    }

    // ===========================
    // DISPLAY FILE DETAILS
    // ===========================
    
    function displayFileDetails(context, fileId) {
        var form = serverWidget.createForm({
            title: 'Skipped JSON File Details'
        });
        
        // Add back button as submit button
        form.addSubmitButton({
            label: 'Back to List'
        });
        
        try {
            var jsonData = parseJSONFile(fileId);
            var jsonFile = file.load({ id: fileId });
            
            if (!jsonData) {
                throw new Error('Unable to parse JSON file');
            }
            
            // Add file info section
            var fileInfoField = form.addField({
                id: 'custpage_file_info',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'File Information'
            });
            
            var fileInfoHtml = '<div style="background: #f5f5f5; padding: 15px; margin-bottom: 20px; border-radius: 5px;">' +
                '<h2>File Information</h2>' +
                '<p><strong>File Name:</strong> ' + jsonFile.name + '</p>' +
                '<p><strong>File ID:</strong> ' + fileId + '</p>' +
                '<p><strong>File Size:</strong> ' + jsonFile.size + ' bytes</p>' +
                '</div>';
            
            fileInfoField.defaultValue = fileInfoHtml;
            
            // Add credit memo details section
            var detailsField = form.addField({
                id: 'custpage_details',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Credit Memo Details'
            });
            
            var detailsHtml = buildDetailsHTML(jsonData);
            detailsField.defaultValue = detailsHtml;
            
            // Add processing metadata section
            if (jsonData._processingMetadata) {
                var metadataField = form.addField({
                    id: 'custpage_metadata',
                    type: serverWidget.FieldType.INLINEHTML,
                    label: 'Processing Metadata'
                });
                
                var metadataHtml = buildMetadataHTML(jsonData._processingMetadata);
                metadataField.defaultValue = metadataHtml;
            }
            
            // Add retry metadata section if exists
            if (jsonData._retryMetadata) {
                var retryField = form.addField({
                    id: 'custpage_retry',
                    type: serverWidget.FieldType.INLINEHTML,
                    label: 'Retry Metadata'
                });
                
                var retryHtml = buildRetryMetadataHTML(jsonData._retryMetadata);
                retryField.defaultValue = retryHtml;
            }
            
            // Add raw JSON section
            var rawJsonField = form.addField({
                id: 'custpage_raw_json',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Raw JSON Data'
            });
            
            var rawJsonHtml = '<div style="background: #f5f5f5; padding: 15px; border-radius: 5px;">' +
                '<h3>Raw JSON</h3>' +
                '<pre style="background: #fff; padding: 15px; overflow-x: auto; border: 1px solid #ddd;">' +
                escapeHtml(JSON.stringify(jsonData, null, 2)) +
                '</pre>' +
                '</div>';
            
            rawJsonField.defaultValue = rawJsonHtml;
            
        } catch (error) {
            log.error('Error loading file details', {
                fileId: fileId,
                error: error.toString()
            });
            
            var errorField = form.addField({
                id: 'custpage_error',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Error'
            });
            
            errorField.defaultValue = '<div style="color: red; padding: 20px;">' +
                '<h2>Error Loading File</h2>' +
                '<p>' + error.toString() + '</p>' +
                '</div>';
        }
        
        context.response.writePage(form);
    }

    // ===========================
    // HTML BUILDERS
    // ===========================
    
    function buildDetailsHTML(jsonData) {
        var html = '<div style="background: #fff; padding: 20px; border: 1px solid #ddd; border-radius: 5px; margin-bottom: 20px;">';
        html += '<h2>Credit Memo Details</h2>';
        html += '<table style="width: 100%; border-collapse: collapse;">';
        
        // Basic details
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Invoice Number:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (jsonData.invoiceNumber || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Invoice Date:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (jsonData.invoiceDate || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Credit Type:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (jsonData.creditType || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">PO/VRMA Number:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (jsonData.poNumber || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Document Total:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold; font-size: 1.1em;">' + 
                (jsonData.documentTotal || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Delivery Amount:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (jsonData.deliveryAmount || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Is Credit Memo:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (jsonData.isCreditMemo ? 'Yes' : 'No') + '</td></tr>';
        
        if (jsonData.validationError) {
            html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Validation Error:</td>' +
                    '<td style="padding: 8px; border-bottom: 1px solid #eee; color: red;">' + 
                    escapeHtml(jsonData.validationError) + '</td></tr>';
        }
        
        html += '</table>';
        
        // Line items
        if (jsonData.lineItems && jsonData.lineItems.length > 0) {
            html += '<h3 style="margin-top: 20px;">Line Items (' + jsonData.lineItems.length + ')</h3>';
            html += '<table style="width: 100%; border-collapse: collapse; border: 1px solid #ddd;">';
            html += '<thead><tr style="background: #4CAF50; color: white;">' +
                    '<th style="padding: 10px; text-align: left; border: 1px solid #ddd;">NARDA</th>' +
                    '<th style="padding: 10px; text-align: left; border: 1px solid #ddd;">Part Number</th>' +
                    '<th style="padding: 10px; text-align: left; border: 1px solid #ddd;">Total Amount</th>' +
                    '<th style="padding: 10px; text-align: left; border: 1px solid #ddd;">Original Bill #</th>' +
                    '<th style="padding: 10px; text-align: left; border: 1px solid #ddd;">Sales Order #</th>' +
                    '</tr></thead><tbody>';
            
            for (var i = 0; i < jsonData.lineItems.length; i++) {
                var item = jsonData.lineItems[i];
                var rowStyle = (i % 2 === 0) ? 'background: #f9f9f9;' : 'background: #fff;';
                
                html += '<tr style="' + rowStyle + '">' +
                        '<td style="padding: 8px; border: 1px solid #ddd;">' + (item.nardaNumber || '') + '</td>' +
                        '<td style="padding: 8px; border: 1px solid #ddd;">' + (item.partNumber || '') + '</td>' +
                        '<td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">' + (item.totalAmount || '') + '</td>' +
                        '<td style="padding: 8px; border: 1px solid #ddd;">' + (item.originalBillNumber || '') + '</td>' +
                        '<td style="padding: 8px; border: 1px solid #ddd;">' + (item.salesOrderNumber || '') + '</td>' +
                        '</tr>';
            }
            
            html += '</tbody></table>';
        }
        
        html += '</div>';
        return html;
    }
    
    function buildMetadataHTML(metadata) {
        var html = '<div style="background: #fff3cd; padding: 15px; border: 1px solid #ffc107; border-radius: 5px; margin-bottom: 20px;">';
        html += '<h3 style="color: #856404;">Processing Metadata</h3>';
        html += '<table style="width: 100%; border-collapse: collapse;">';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Status:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (metadata.processingStatus || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Skip Reason:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee; color: #d32f2f; font-weight: bold;">' + 
                (metadata.skipReason || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Processed Date:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + formatDate(metadata.processedDate) + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Original File Name:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (metadata.originalFileName || '') + '</td></tr>';
        
        if (metadata.skippedTransactionCount) {
            html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Skipped Transaction Count:</td>' +
                    '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + metadata.skippedTransactionCount + '</td></tr>';
        }
        
        html += '</table></div>';
        return html;
    }
    
    function buildRetryMetadataHTML(retryMetadata) {
        var html = '<div style="background: #e3f2fd; padding: 15px; border: 1px solid #2196F3; border-radius: 5px; margin-bottom: 20px;">';
        html += '<h3 style="color: #1565C0;">Retry Metadata</h3>';
        html += '<table style="width: 100%; border-collapse: collapse;">';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Process Type:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (retryMetadata.processType || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Retry Timestamp:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + formatDate(retryMetadata.retryTimestamp) + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Original PDF File ID:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (retryMetadata.originalPdfFileId || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Original PDF File Name:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (retryMetadata.originalPdfFileName || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Vendor Name:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (retryMetadata.vendorName || '') + '</td></tr>';
        
        html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Claude Model:</td>' +
                '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + (retryMetadata.claudeModel || '') + '</td></tr>';
        
        if (retryMetadata.processingDuration) {
            html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Processing Duration:</td>' +
                    '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + retryMetadata.processingDuration + '</td></tr>';
        }
        
        if (retryMetadata.inputTokens) {
            html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Input Tokens:</td>' +
                    '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + retryMetadata.inputTokens + '</td></tr>';
        }
        
        if (retryMetadata.outputTokens) {
            html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Output Tokens:</td>' +
                    '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + retryMetadata.outputTokens + '</td></tr>';
        }
        
        if (retryMetadata.retryReason) {
            html += '<tr><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">Retry Reason:</td>' +
                    '<td style="padding: 8px; border-bottom: 1px solid #eee;">' + escapeHtml(retryMetadata.retryReason) + '</td></tr>';
        }
        
        html += '</table></div>';
        return html;
    }

    // ===========================
    // HELPER FUNCTIONS
    // ===========================
    
    function findJSONFilesInFolder(folderId) {
        var jsonFiles = [];
        
        var fileSearch = search.create({
            type: 'file',
            filters: [
                ['folder', 'anyof', folderId],
                'AND',
                ['filetype', 'is', 'JSON']
            ],
            columns: [
                'name',
                'created',
                'modified',
                'documentsize'
            ]
        });
        
        fileSearch.run().each(function(result) {
            jsonFiles.push({
                id: result.id,
                name: result.getValue('name'),
                created: result.getValue('created'),
                modified: result.getValue('modified'),
                size: result.getValue('documentsize')
            });
            return true;
        });
        
        return jsonFiles;
    }
    
    function parseJSONFile(fileId) {
        try {
            var jsonFile = file.load({
                id: fileId
            });
            
            var fileContents = jsonFile.getContents();
            var jsonData = JSON.parse(fileContents);
            
            return jsonData;
        } catch (error) {
            log.error('Error parsing JSON file', {
                fileId: fileId,
                error: error.toString()
            });
            return null;
        }
    }
    
    function formatDate(dateString) {
        if (!dateString) {
            return '';
        }
        
        try {
            var date = new Date(dateString);
            return date.toLocaleString('en-US', {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: true
            });
        } catch (error) {
            return dateString;
        }
    }
    
    function escapeHtml(text) {
        if (!text) {
            return '';
        }
        
        var map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        
        return text.toString().replace(/[&<>"']/g, function(m) { return map[m]; });
    }

    // ===========================
    // EXPORTS
    // ===========================
    
    return {
        onRequest: onRequest
    };
});
