const fs = require('fs');
const mariadb = require('mariadb');
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

    includedRitms = [
        'RITM1187823',
        'RITM0010503',
        'RITM0376153',
        'RITM0556811',
        'RITM1023899',
        'RITM0017426',
        'RITM1187691',
        'RITM0376145',
        'RITM0989659',
        'RITM0831264',
        'RITM1187787',
        'RITM1187698',
        'RITM0376155',
        'RITM1188570',
        'RITM1188451',
        'RITM0010483',
        'RITM1068622',
        'RITM0937637',
        'RITM0937756',
        'RITM0019738'
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
                    execSync(`mkdir -p ${taskPath}`);
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
        const requestNumber = await this.getRequestNumber(task); // Menambahkan pelacakan REQ number
    
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
        
        const variable1 = variables[2]?.value || '';
        const variable2 = variables[10]?.value || '';

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
            'State': task.a_ref_1,
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
            'Request': requestNumber, // Menggunakan requestNumber yang telah dilacak
            'Sys Watch List': task.a_str_24,
            'Request Subject': variable1,  
            'Explain Request': variable2    
        };
    
        const header = Object.keys(data).join(',');
        const values = Object.values(data).map(value => `"${this.escapeCsvValue(value)}"`).join(',');
    
        // Write CSV string to file
        const filepath = `${taskPath}/${task.number}.csv`;
        fs.writeFileSync('data.csv', `${header}\n${values}`);
        execSync(`mv data.csv ${filepath}`);
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
        const user = await this.conn.query(`select name from sys_user where sys_id = '${task.a_ref_10}'`);
        return user[0]?.name || '';
    }

    async getCatItemName(task) {
        const cat = await this.conn.query(`select name from sc_cat_item where sys_id = '${task.a_ref_1}'`);
        return cat[0]?.name || '';
    }
    

    async getReference(task) {
        const refTask = await this.conn.query(`select number from task where sys_id = '${task.a_ref_9}'`);
        return refTask[0]?.number || '';
    }

    async getRequestNumber(task) { 
        const request = await this.conn.query(`SELECT task_effective_number FROM task WHERE sys_id = '${task.request}'`);
        return request[0]?.number || '';
    }

    constructJournal(j) {
        return `${j.sys_created_by}\n${j.sys_created_on}\n${j.value}`;
    }

    async getTasks(offset, limit) {
        const ritmList = this.includedRitms.map(ritm => `'${ritm}'`).join(',');
        return this.conn.query(`select * from task where sys_class_name = 'sc_req_item' and number in (${ritmList}) order by number desc limit ${limit} offset ${offset};`);
    }

    async getTask(taskNumber) {
        return this.conn.query(`select * from task where number = '${taskNumber}';`);
    }

    async extractAttachments(task, taskPath) {
        const chunks = await this.getChunks(task.sys_id);

        this.groupChunksIntoAttachments(chunks).forEach(a =>
            this.extractAttachment(a, taskPath)
        );
    }

    groupChunksIntoAttachments(chunks) {
        const attachmentsMap = {};

        chunks.forEach(c => {
            if (attachmentsMap[c.sys_id]) {
                attachmentsMap[c.sys_id].push(c);
            } else {
                attachmentsMap[c.sys_id] = [c];
            }
        });

        return Object.values(attachmentsMap);
    }

    async getChunks(taskId) {
        return this.conn.query(`select * from sys_attachment_doc where sys_attachment in (select sys_id from sys_attachment where table_sys_id = '${taskId}') order by position`);
    }

    extractAttachment(attachmentChunks, taskPath) {
        const firstChunk = attachmentChunks[0];
        const filename = firstChunk.file_name.replace(/_/g, '-');
        const outputFilePath = `${taskPath}/${filename}`;

        const writeStream = fs.createWriteStream(outputFilePath);
        attachmentChunks.forEach(c => writeStream.write(Buffer.from(c.content, 'base64')));
        writeStream.end();
    }

    getGroupPath(tasks) {
        const groupId = tasks[0].number.replace(/^.*?(\d+).*$/, '$1');
        return `${this.resultDir}/${groupId}`;
    }

    getTaskPath(groupPath, task) {
        return `${groupPath}/${task.number}`;
    }

    formatDateBeta(d) {
        const pad = (num) => num.toString().padStart(2, '0');
        return `${pad(d.getMonth() + 1)}/${pad(d.getDate())}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    }
}

main();
