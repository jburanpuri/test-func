const sql = require('mssql');
const jsforce = require('jsforce');
require('dotenv').config();

// Azure Function entry point
module.exports = async function (context, req) {
    context.log('Lead Conversion Azure Function started.');

    // Salesforce connection setup
    const sfConn = new jsforce.Connection({ loginUrl: process.env.SF_LOGIN_URL });

    // SQL Server configuration
    const sqlConfig = {
        user: process.env.SQL_USER,
        password: process.env.SQL_PASSWORD,
        server: process.env.SQL_SERVER,
        database: process.env.SQL_DATABASE,
        options: { encrypt: true }
    };

    try {
        // Connect to SQL Server
        await sql.connect(sqlConfig);
        context.log('Connected to SQL Server.');

        // Fetch leads pending conversion
        const pendingLeadsResult = await sql.query`
      SELECT SF_LeadId, SecureSite_ClientId__c, Created_Date 
      FROM JOBS.SF_Leads_Pending_Conversion
    `;

        const pendingLeads = pendingLeadsResult.recordset;

        if (pendingLeads.length === 0) {
            context.log('No leads pending conversion.');
            return;
        }

        // Authenticate with Salesforce
        await sfConn.login(process.env.SF_USERNAME, process.env.SF_PASSWORD);
        context.log('Authenticated with Salesforce.');

        for (const lead of pendingLeads) {
            try {
                context.log(`Converting lead: ${lead.SF_LeadId}`);

                // Call Salesforce API to convert Lead
                const conversionResult = await sfConn.soap.convertLead({
                    leadId: lead.SF_LeadId,
                    convertedStatus: 'Closed - Converted'  // Adjust this based on the SF setup
                });

                if (conversionResult.success) {
                    context.log(`Lead converted successfully: AccountId: ${conversionResult.accountId}, ContactId: ${conversionResult.contactId}`);

                    // Delete converted lead from pending table
                    await sql.query`
            DELETE FROM JOBS.SF_Leads_Pending_Conversion 
            WHERE SF_LeadId = ${lead.SF_LeadId}
          `;

                    // TODO: Proceed with creating Opportunity (DCI-7)
                } else {
                    throw new Error(conversionResult.errors.join('; '));
                }
            } catch (err) {
                context.log.error(`Error converting lead ${lead.SF_LeadId}: ${err.message}`);

                // Insert error details into error table
                await sql.query`
          INSERT INTO JOBS.SF_Leads_Conversion_Errors (SF_LeadId, SecureSite_ClientId__c, Created_Date, Error_Message)
          VALUES (${lead.SF_LeadId}, ${lead.SecureSite_ClientId__c}, ${lead.Created_Date}, ${err.message})
        `;
            }
        }

    } catch (globalErr) {
        context.log.error(`General error in function: ${globalErr.message}`);
    } finally {
        sql.close();
        context.log('Lead Conversion Azure Function completed.');
    }
};
