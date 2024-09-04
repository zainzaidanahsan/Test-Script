const fs = require('fs');
const mariadb = require('mariadb');
const path = require('path');
const { execSync } = require('child_process');

async function main() {
    console.log('Script started');

    let conn;
    try {
        const pool = mariadb.createPool({
            host: 'localhost',
            port: 3306,
            user: 'pmifsm',
            password: 'pmifsm',
            connectionLimit: 5,
            database: 'pmifsm'
        });

        conn = await pool.getConnection();
        const snowArchival = new SnowArchival(conn, '/mt/ebs/result', 1000);

        await snowArchival.start();
        console.log('Script finished');
    } catch (err) {
        console.log(err);
    } finally {
        if (conn) await conn.end();
    }
}

class SnowArchival {
    conn;
    resultDir;
    batchSize;

    excludedRitms = [
        '2437ffc5478b295047f2c071e36d43df',
        '43cd2bd5478f695047f2c071e36d43e0',
        'b89ae391478f695047f2c071e36d436d',
        'd630e4d51b0ba550b3f5a6c3b24bcbe0',
        'ff4f94951b0ba550b3f5a6c3b24bcb76',
    ];

    constructor(conn, resultDir, batchSize) {
        this.conn = conn;
        this.resultDir = resultDir;
        this.batchSize = batchSize;
    }

    async start() {
        let startIdx = 0;

        while (true) {
            let tasks = await this.getTasks(startIdx, this.batchSize);
            if (tasks.length === 0) break;

            if (startIdx === 0) {
                tasks = tasks.filter(t => !this.excludedRitms.includes(t.sys_id));
            }

            startIdx += this.batchSize;

            const groupPath = this.getGroupPath(tasks);
            console.log(groupPath, startIdx);

            for (const task of tasks) {
                try {
                    const taskPath = this.getTaskPath(groupPath, task);
                    execSync(`mkdir -p "${taskPath}"`);
                    await this.extractCsv(task, taskPath);
                    await this.extractAttachments(task, taskPath);
                } catch (err) {
                    console.error(`sys_id: ${task.sys_id}, task_number: ${task.number}, err:`, err);
                }
            }
        }
    }

    async extractCsv(task, taskPath) {
        const journals = await this.conn.query(`select * from sys_journal_field where element in ('work_notes', 'comments') and element_id = '${task.sys_id}' order by sys_created_on;`);
        const commentsAndWorkNotes = journals.map(this.constructJournal).join('\n');
    
        const assignedTo = await this.getAssignedTo(task);
        const catItemName = await this.getCatItemName(task);
        const reference = await this.getReference(task);
        const companyCode = await this.getCompanyCode(task);
    
        const contexts = await this.conn.query(`select name, stage from wf_context where id = '${task.sys_id}'`);
    
        let stageName = '';
        if (contexts && contexts.length > 0) {
            const context = contexts[0];
            const stages = await this.conn.query(`select name from wf_stage where sys_id = '${context.stage}'`);
            stageName = stages[0]?.name;
        }
    
        const closedAtDate = new Date(task.closed_at);

        const variables = await this.conn.query(`
            SELECT opt.value 
            FROM sc_item_option_mtom mtom
            JOIN sc_item_option opt ON mtom.sc_item_option = opt.sys_id
            WHERE mtom.request_item = '${task.sys_id}'
        `);

        // Variabel untuk menyimpan hasil pencarian
        let requestSubject = '';
        let explainRequest = '';

        // Loop untuk memeriksa setiap elemen berdasarkan kondisi yang diberikan
        if (variables && variables.length > 0) {
            for (let i = 0; i < variables.length; i++) {
                const variableValue = variables[i]?.value || '';

                // Logika untuk "request_subject"
                if (!requestSubject) {
                    if (
                        /^(FW:|RE:|PD:)/i.test(variableValue) &&  // Pastikan mengandung "FW:", "RE:", atau "PD:"
                        variableValue.length > 10 &&             // Ambil yang lebih dari 10 karakter
                        !/Email Ingestion/i.test(variableValue) &&  // Hindari "Email Ingestion"
                        !/[@]/.test(variableValue) &&            // Hindari karakter "@"
                        !(variableValue.length >= 25 && variableValue.length <= 40 && /^[a-zA-Z0-9]+$/.test(variableValue)) // Hindari string alfanumerik dengan panjang 25-40 karakter
                    ) {
                        requestSubject = variableValue;
                    }
                }

                // Logika untuk "explain_request"
                if (!explainRequest) {
                    if (
                        variableValue.length > 100 &&             // Ambil yang lebih dari 100 karakter
                        /(Dear|Please)/i.test(variableValue)      // Ambil yang mengandung "Dear" atau "Please"
                    ) {
                        explainRequest = variableValue;
                    }
                }

                // Berhenti jika kedua field sudah ditemukan
                if (requestSubject && explainRequest) {
                    break;
                }
            }
        }

        const data = {
            'Number': task.number,
            'Opened': task.opened_at,
            'Company Code': companyCode,
            'Region': task.a_str_27,
            'Priority': task.priority,
            'Source': task.a_str_22,
            'Item': catItemName,
            'Short Description': task.short_description,
            'Resolution Note': task.a_str_10,
            'Resolved': this.formatDateBeta(closedAtDate),
            'Closed': this.formatDateBeta(closedAtDate),
            'Stage': stageName,
            'State': task.State,
            'PMI Generic Mailbox': task.a_str_23,
            'Email TO Recipients': task.a_str_25,
            'Email CC Recipients': task.a_str_24,
            'External User\'s Email': task.a_str_7,
            'Sys Email Address': task.sys_created_by,
            'Contact Type': task.contact_type,
            'Assigned To': assignedTo,
            'Resolved By': assignedTo,
            'Contact Person': task.a_str_28,
            'Approval': task.approval,
            'Approval Attachment': '',
            'Approval Request': task.a_str_11,
            'Approval Set': task.approval_set,
            'Reassignment Count': task.reassignment_count,
            'Related Ticket': reference,
            'Reopening Count': '',
            'Comments And Work Notes': commentsAndWorkNotes,
            'Request': task.task_effective_number,
            'Sys Watch List': task.a_str_24,
            'Request Subject': requestSubject,  
            'Explain Request': explainRequest    
        };
    
        const header = Object.keys(data).join(',');
        const values = Object.values(data).map(value => `"${this.escapeCsvValue(value)}"`).join(',');
    
        // Write CSV string to file
        const filepath = `${taskPath}/${task.number}.csv`;
        fs.writeFileSync('data.csv', `${header}\n${values}`);
        execSync(`mv data.csv "${filepath}"`);
    }

    async getVendorTypeName(task) {
        const vendorType = await this.conn.query(`SELECT name FROM vendor_type WHERE sys_id = '${task.vendor_type}'`);
        return vendorType[0]?.name || '';
    }

    async getCompanyCode(task) {
        const company = await this.conn.query(`select u_company_code from core_company where sys_id = '${task.company}'`);
        return company[0]?.u_company_code || '';
    }
    
    async getRequestType(task) {
        const requestType = await this.conn.query(`SELECT request_type FROM outbound_request_usage_metrics WHERE sys_id = '${task.sys_id}'`);
        return requestType[0]?.request_type || '';
    }

    escapeCsvValue(value) {
        if (typeof value === 'string') {
            return value.replace(/"/g, '""'); // Escape double quotes
        }
        return value;
    }

    async getAssignedTo(task) {
        const user = await this.conn.query(`select name from sys_user where sys_id = '${task.assigned_to}'`);
        return user[0]?.name || '';
    }

    async getCatItemName(task) {
        const catItem = await this.conn.query(`select name from sc_cat_item where sys_id = '${task.cat_item}'`);
        return catItem[0]?.name || '';
    }

    async getReference(task) {
        const ritm = await this.conn.query(`select number from sc_req_item where sys_id = '${task.sys_id}'`);
        return ritm[0]?.number || '';
    }

    formatDateBeta(date) {
        const d = new Date(date);
        const month = `${d.getMonth() + 1}`.padStart(2, '0');
        const day = `${d.getDate()}`.padStart(2, '0');
        const year = d.getFullYear();
        return `${year}-${month}-${day}`;
    }

    getGroupPath(tasks) {
        if (tasks.length === 0) return '';
        const dateStr = tasks[0].opened_at.split(' ')[0];
        return `${this.resultDir}/${dateStr}`;
    }

    getTaskPath(groupPath, task) {
        const dirPath = `${groupPath}/${task.number}`;
        return dirPath;
    }
}

main().then(r => console.log('done')).catch(e => console.log(e));
