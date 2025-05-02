const { Connection, SoapApi } = require('jsforce');
const sql = require('mssql');

/* ───────────── 1.  Configuration ───────────── */

const CONFIG = Object.freeze({
    SF_LOGIN_URL: process.env.SF_LOGIN_URL,
    SF_USERNAME: process.env.SF_USERNAME,
    SF_PASSWORD: process.env.SF_PASSWORD,
    SF_TOKEN: process.env.SF_TOKEN,
    DB_CONN_STR: process.env.DB_CONN_STR
});

// Fail fast if any env-var is missing
const missing = Object.entries(CONFIG)
    .filter(([, value]) => !value)
    .map(([key]) => key);
if (missing.length) {
    throw new Error(`Missing environment variables: ${missing.join(', ')}`);
}

// Re-use one SQL pool across invocations (Azure Functions best practice)
const sqlPool = sql.connect(CONFIG.DB_CONN_STR);

/* ───────────── 2.  Helper functions ───────────── */

/** Insert a row into JOBS.SF_Leads_Conversion_Errors */
async function logError(pool, lead, message) {
    await pool.request()
        .input('leadId', sql.Char(18), lead.SF_LeadId)
        .input('client', sql.VarChar(50), lead.SecureSite_ClientId__c)
        .input('created', sql.DateTime2, lead.Created_Date)
        .input('errDate', sql.DateTime2, new Date())
        .input('msg', sql.NVarChar(sql.MAX), message)
        .query(`
      INSERT INTO JOBS.SF_Leads_Conversion_Errors
             (SF_LeadId, SecureSite_ClientId__c,
              Created_Date, Error_Date, Error_Message)
      VALUES (@leadId, @client, @created, @errDate, @msg)
    `);
}

/* ───────────── 3.  Azure Function entry point ───────────── */

module.exports = async function (context) {

    // 3.1  Connect to SQL and Salesforce
    const pool = await sqlPool;

    const sf = new Connection({ loginUrl: CONFIG.SF_LOGIN_URL });
    await sf.login(
        CONFIG.SF_USERNAME,
        CONFIG.SF_PASSWORD + CONFIG.SF_TOKEN          // username-password-token pattern
    );
    const soap = new SoapApi(sf);

    // 3.2  Read the queue
    const { recordset: leads } = await pool.request().query(`
    SELECT SF_LeadId, SecureSite_ClientId__c, Created_Date
    FROM   JOBS.SF_Leads_Pending_Conversion
  `);

    if (leads.length === 0) {
        context.log('No pending leads.');
        await sf.logout();
        return;
    }

    // 3.3  Find the org-specific converted status once
    const [{ MasterLabel: convertedStatus = 'Closed - Converted' }] =
        await sf.sobject('LeadStatus')
            .find({ IsConverted: true }, 'MasterLabel')
            .limit(1);

    // 3.4  Convert each lead 
    for (const lead of leads) {
        try {
            const [result] = await soap.convertLead([{
                leadId: lead.SF_LeadId,
                convertedStatus,
                doNotCreateOpportunity: false
            }]);

            if (result.success) {
                // Success: write IDs to log, then dequeue
                context.log(
                    `Converted Lead ${lead.SF_LeadId} → ` +
                    `Account ${result.accountId}, Contact ${result.contactId}, ` +
                    `Opportunity ${result.opportunityId || 'none'}`
                );

                await pool.request()
                    .input('leadId', sql.Char(18), lead.SF_LeadId)
                    .query(`
            DELETE FROM JOBS.SF_Leads_Pending_Conversion
            WHERE  SF_LeadId = @leadId
          `);

            } else {
                await logError(pool, lead, result.errors.map(e => e.message).join('; '));
            }

        } catch (err) {
            await logError(pool, lead, err.message);
        }
    }

    await sf.logout();
};
