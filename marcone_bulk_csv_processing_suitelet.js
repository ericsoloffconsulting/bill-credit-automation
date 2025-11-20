/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 * @NModuleScope SameAccount
 */
define(['N/ui/serverWidget', 'N/file', 'N/runtime', 'N/redirect', 'N/task', 'N/search', 'N/log'],
    function (serverWidget, file, runtime, redirect, task, search, log) {

        var CONFIG = {
            FOLDERS: {
                CSV_SOURCE: 2676075,
                MISSING_AP_AGING_FOLDER: 2676075
            },
            SCHEDULED_SCRIPT: {
                SCRIPT_ID: 'customscript_marcone_bill_credit_csv_pro',
                DEPLOYMENT_ID: 'customdeploy_marcone_bill_credit_csv_pro'
            },
            MISSING_AP_AGING_FILENAME: 'Marcone Missing AP Aging from OK To Pay.csv'
        };

        /**
         * Definition of the Suitelet script trigger point
         * @param {Object} context
         * @param {ServerRequest} context.request - Incoming request
         * @param {ServerResponse} context.response - Suitelet response
         */
        function onRequest(context) {
            try {
                if (context.request.method === 'GET') {
                    // Display form
                    displayForm(context);
                } else {
                    // Process form submission
                    processFormSubmission(context);
                }
            } catch (error) {
                log.error('Suitelet Error', {
                    error: error.toString(),
                    stack: error.stack
                });

                // Show error page
                showErrorPage(context, error);
            }
        }

        /**
 * Display the upload form
 * @param {Object} context - Suitelet context
 */
        function displayForm(context) {
            try {
                log.debug('Displaying Form', 'Creating Marcone Bill Credit Bulk CSV Processing Portal');

                // Create form
                var form = serverWidget.createForm({
                    title: 'Marcone Bill Credit Bulk CSV Processing Portal'
                });

                // Add instructions field group
                var instructionsGroup = form.addFieldGroup({
                    id: 'custpage_instructions_group',
                    label: 'Instructions'
                });

                var instructionsField = form.addField({
                    id: 'custpage_instructions',
                    type: serverWidget.FieldType.INLINEHTML,
                    label: 'Instructions',
                    container: 'custpage_instructions_group'
                });

                var instructionsHtml = '<div style="padding: 10px; background-color: #f0f8ff; border: 1px solid #4682b4; border-radius: 5px; margin-bottom: 15px;">' +
                    '<h3 style="margin-top: 0; color: #4682b4;">Processing Instructions</h3>' +
                    '<ol style="margin: 10px 0; padding-left: 20px;">' +
                    '<li>Upload the Marcone vendor order CSV file</li>' +
                    '<li>Select the email recipient (defaults to current user)</li>' +
                    '<li>Optionally enable filtering by Missing from AP Aging</li>' +
                    '<li>Click Submit to process the file</li>' +
                    '</ol>' +
                    '<p style="margin: 10px 0;"><strong>Note:</strong> The scheduled script will process the file and send results to the specified recipient.</p>' +
                    '</div>';

                instructionsField.defaultValue = instructionsHtml;

                // Add file upload field - DO NOT add to a field group or container
                var fileField = form.addField({
                    id: 'custpage_csv_file',
                    type: serverWidget.FieldType.FILE,
                    label: 'Marcone Vendor Order File'
                });
                fileField.isMandatory = true;
                fileField.help = 'Upload the Marcone vendor order CSV file to process';

                // Add email recipient field (no field group)
                var emailField = form.addField({
                    id: 'custpage_email_recipient',
                    type: serverWidget.FieldType.SELECT,
                    label: 'Email Recipient',
                    source: 'employee'
                });
                emailField.isMandatory = true;
                emailField.defaultValue = runtime.getCurrentUser().id;
                emailField.help = 'Employee to receive processing results email';

                // Add filter checkbox (no field group)
                var filterCheckbox = form.addField({
                    id: 'custpage_filter_by_missing_ap',
                    type: serverWidget.FieldType.CHECKBOX,
                    label: 'Filter By Missing from AP Aging from OK To Pay Automation'
                });
                filterCheckbox.defaultValue = 'F';
                filterCheckbox.help = 'Check to filter by "Marcone Missing AP Aging from OK To Pay.csv" file';

                // Add submit button
                form.addSubmitButton({
                    label: 'Submit & Process'
                });

                // Add cancel button
                form.addButton({
                    id: 'custpage_cancel_button',
                    label: 'Cancel',
                    functionName: 'window.history.back()'
                });

                // Write form to response
                context.response.writePage(form);

                log.debug('Form Displayed', 'Form created successfully');

            } catch (error) {
                log.error('Display Form Error', {
                    error: error.toString(),
                    stack: error.stack
                });
                throw error;
            }
        }

        /**
         * Process form submission
         * @param {Object} context - Suitelet context
         */
        function processFormSubmission(context) {
            try {
                log.audit('Processing Form Submission', 'Starting form processing');

                var request = context.request;

                // Get form field values
                var uploadedFile = request.files.custpage_csv_file;
                var emailRecipient = request.parameters.custpage_email_recipient;
                var filterByMissingAP = request.parameters.custpage_filter_by_missing_ap === 'T';

                log.debug('Form Parameters', {
                    hasFile: !!uploadedFile,
                    emailRecipient: emailRecipient,
                    filterByMissingAP: filterByMissingAP
                });

                // Validate uploaded file
                if (!uploadedFile) {
                    throw new Error('No file uploaded');
                }

                // Save uploaded file to CSV_SOURCE folder
                uploadedFile.folder = CONFIG.FOLDERS.CSV_SOURCE;
                var uploadedFileId = uploadedFile.save();

                log.audit('File Uploaded', {
                    fileId: uploadedFileId,
                    fileName: uploadedFile.name,
                    folder: CONFIG.FOLDERS.CSV_SOURCE
                });

                // Prepare script parameters
                var scriptParams = {
                    custscript_csv_file_id: uploadedFileId,
                    custscript_csv_bc_process_email_recip: emailRecipient
                };

                // If filter checkbox is checked, find Missing AP Aging file
                if (filterByMissingAP) {
                    var missingAPFileId = findMissingAPAgingFile();

                    if (missingAPFileId) {
                        scriptParams.custscript_missing_ap_aging = missingAPFileId;
                        log.debug('Missing AP Aging File Found', {
                            fileId: missingAPFileId
                        });
                    } else {
                        log.error('Missing AP Aging File Not Found', {
                            folder: CONFIG.FOLDERS.MISSING_AP_AGING_FOLDER,
                            fileName: CONFIG.MISSING_AP_AGING_FILENAME
                        });
                        throw new Error('Filter by Missing AP Aging is enabled but file "' +
                            CONFIG.MISSING_AP_AGING_FILENAME + '" not found in folder ' +
                            CONFIG.FOLDERS.MISSING_AP_AGING_FOLDER);
                    }
                }

                // Schedule the script
                var scheduledTask = task.create({
                    taskType: task.TaskType.SCHEDULED_SCRIPT,
                    scriptId: CONFIG.SCHEDULED_SCRIPT.SCRIPT_ID,
                    deploymentId: CONFIG.SCHEDULED_SCRIPT.DEPLOYMENT_ID,
                    params: scriptParams
                });

                var taskId = scheduledTask.submit();

                log.audit('Scheduled Script Submitted', {
                    taskId: taskId,
                    scriptId: CONFIG.SCHEDULED_SCRIPT.SCRIPT_ID,
                    deploymentId: CONFIG.SCHEDULED_SCRIPT.DEPLOYMENT_ID,
                    parameters: scriptParams
                });

                // Show success page
                showSuccessPage(context, {
                    taskId: taskId,
                    fileName: uploadedFile.name,
                    emailRecipient: emailRecipient,
                    filterByMissingAP: filterByMissingAP
                });

            } catch (error) {
                log.error('Form Submission Error', {
                    error: error.toString(),
                    stack: error.stack
                });
                throw error;
            }
        }

        /**
  * Find Missing AP Aging file in folder
  * @returns {number|null} File ID or null if not found
  */
        function findMissingAPAgingFile() {
            try {
                log.debug('Searching for Missing AP Aging File', {
                    folder: CONFIG.FOLDERS.MISSING_AP_AGING_FOLDER,
                    fileName: CONFIG.MISSING_AP_AGING_FILENAME
                });

                var fileSearch = search.create({
                    type: 'file',
                    filters: [
                        ['name', 'is', CONFIG.MISSING_AP_AGING_FILENAME],
                        'AND',
                        ['folder', 'anyof', CONFIG.FOLDERS.MISSING_AP_AGING_FOLDER]
                    ],
                    columns: ['internalid', 'name', 'created']
                });

                var searchResults = fileSearch.run().getRange({
                    start: 0,
                    end: 1
                });

                if (searchResults && searchResults.length > 0) {
                    var fileId = searchResults[0].getValue('internalid');
                    var fileName = searchResults[0].getValue('name');
                    var created = searchResults[0].getValue('created');

                    log.debug('Missing AP Aging File Found', {
                        fileId: fileId,
                        fileName: fileName,
                        created: created
                    });

                    return parseInt(fileId);
                }

                log.debug('Missing AP Aging File Not Found', {
                    folder: CONFIG.FOLDERS.MISSING_AP_AGING_FOLDER,
                    fileName: CONFIG.MISSING_AP_AGING_FILENAME
                });

                return null;

            } catch (error) {
                log.error('File Search Error', {
                    error: error.toString(),
                    stack: error.stack
                });
                return null;
            }
        }

        /**
         * Show success page
         * @param {Object} context - Suitelet context
         * @param {Object} details - Processing details
         */
        function showSuccessPage(context, details) {
            try {
                var form = serverWidget.createForm({
                    title: 'Processing Submitted Successfully'
                });

                var successField = form.addField({
                    id: 'custpage_success_message',
                    type: serverWidget.FieldType.INLINEHTML,
                    label: 'Success'
                });

                var successHtml = '<div style="padding: 20px; background-color: #d4edda; border: 2px solid #28a745; border-radius: 5px; margin: 20px 0;">' +
                    '<h2 style="color: #155724; margin-top: 0;">✓ Processing Submitted Successfully</h2>' +
                    '<div style="margin: 15px 0; padding: 15px; background-color: white; border-radius: 3px;">' +
                    '<p><strong>Task ID:</strong> ' + details.taskId + '</p>' +
                    '<p><strong>File Name:</strong> ' + details.fileName + '</p>' +
                    '<p><strong>Email Recipient:</strong> Employee ID ' + details.emailRecipient + '</p>' +
                    '<p><strong>Filter by Missing AP Aging:</strong> ' + (details.filterByMissingAP ? 'Yes' : 'No') + '</p>' +
                    '</div>' +
                    '<h3 style="color: #155724;">Next Steps:</h3>' +
                    '<ol style="color: #155724;">' +
                    '<li>The scheduled script will process the CSV file</li>' +
                    '<li>Results will be emailed to the specified recipient</li>' +
                    '<li>Check the Scheduled Script Status page for progress</li>' +
                    '</ol>' +
                    '<p style="margin-top: 20px;"><a href="/app/common/scripting/scriptstatus.nl?daterange=TODAY&primarykey=' +
                    details.taskId + '" target="_blank" style="color: #007bff; text-decoration: underline;">View Script Status</a></p>' +
                    '</div>';

                successField.defaultValue = successHtml;

                form.addButton({
                    id: 'custpage_new_upload',
                    label: 'Upload Another File',
                    functionName: 'window.location.reload()'
                });

                context.response.writePage(form);

            } catch (error) {
                log.error('Success Page Error', {
                    error: error.toString(),
                    stack: error.stack
                });
                throw error;
            }
        }

        /**
         * Show error page
         * @param {Object} context - Suitelet context
         * @param {Error} error - Error object
         */
        function showErrorPage(context, error) {
            try {
                var form = serverWidget.createForm({
                    title: 'Processing Error'
                });

                var errorField = form.addField({
                    id: 'custpage_error_message',
                    type: serverWidget.FieldType.INLINEHTML,
                    label: 'Error'
                });

                var errorHtml = '<div style="padding: 20px; background-color: #f8d7da; border: 2px solid #dc3545; border-radius: 5px; margin: 20px 0;">' +
                    '<h2 style="color: #721c24; margin-top: 0;">✗ Processing Error</h2>' +
                    '<div style="margin: 15px 0; padding: 15px; background-color: white; border-radius: 3px;">' +
                    '<p><strong>Error Message:</strong></p>' +
                    '<p style="color: #721c24; font-family: monospace;">' + error.toString() + '</p>' +
                    '</div>' +
                    '<h3 style="color: #721c24;">What to do:</h3>' +
                    '<ol style="color: #721c24;">' +
                    '<li>Verify that the CSV file is in the correct format</li>' +
                    '<li>Check that all required fields are filled in</li>' +
                    '<li>If the problem persists, contact your administrator</li>' +
                    '</ol>' +
                    '</div>';

                errorField.defaultValue = errorHtml;

                form.addButton({
                    id: 'custpage_try_again',
                    label: 'Try Again',
                    functionName: 'window.history.back()'
                });

                context.response.writePage(form);

            } catch (displayError) {
                log.error('Error Page Display Error', {
                    originalError: error.toString(),
                    displayError: displayError.toString()
                });

                // Fallback to simple text response
                context.response.write('An error occurred: ' + error.toString() + '\n\n' +
                    'Please contact your administrator.');
            }
        }

        return {
            onRequest: onRequest
        };
    });