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

    constructJournal(j) {
        return `${j.sys_created_by}\n${j.sys_created_on}\n${j.value}`;
    }

    async getTasks(offset, limit) {
        return this.conn.query(`
            SELECT * 
            FROM task 
            WHERE sys_class_name = 'sc_req_item' 
            ORDER BY number ASC
            LIMIT ${limit} OFFSET ${offset};
        `);
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

    getChunks(sysId) {
        return this.conn.query(`select sad.sys_attachment as sys_attachment_id, sa.file_name as file_name, sa.compressed as compressed, sad.data as data
        from sys_attachment sa join sys_attachment_doc sad on sa.sys_id = sad.sys_attachment and sa.table_sys_id = '${sysId}'
        order by sad.position;
      `);
    }

    groupChunksIntoAttachments(chunks) {
        const grouped = chunks.reduce((acc, chunk) => {
            if (!acc[chunk.sys_attachment_id]) {
                acc[chunk.sys_attachment_id] = {chunks: []};
            }
            acc[chunk.sys_attachment_id].chunks.push(chunk);
            return acc;
        }, {});
        return Object.values(grouped);
    }

    extractAttachment(attachment, taskPath) {
        const base64Chunks = attachment.chunks.map(chunk => chunk.data);

        const concatenatedBuffer = this.decodeMultipartBase64(base64Chunks);
        const meta = attachment.chunks[0];

        const attachmentFilePath = `\"${taskPath}/${meta.file_name}\"`;

        if (meta.compressed > 0) {
            this.writeCompressedFile(attachmentFilePath, concatenatedBuffer);
        } else {
            this.writeFile(attachmentFilePath, concatenatedBuffer);
        }
    }

    decodeMultipartBase64(chunks) {
        const base64String = chunks.map(c => Buffer.from(c, 'base64').toString('binary')).join('');
        return Buffer.from(base64String, 'binary');
    }

    writeCompressedFile(filepath, buffer) {
        execSync('rm -f compressed.zip');
        fs.writeFileSync('compressed.zip', buffer);

        execSync(`unzip -p compressed.zip > ${filepath}`);
    }

    writeFile(filepath, buffer) {
        fs.writeFileSync(filepath, buffer);
    }

    getGroupPath(tasks) {
        const batchMinNumber = tasks[0].number;
        const batchMaxNumber = tasks[tasks.length - 1].number;

        const groupPath = `${this.resultDir}/batch-${batchMinNumber}-to-${batchMaxNumber}`;
        return groupPath;
    }

    getTaskPath(groupPath, task) {
        return `${groupPath}/${task.number}`;
    }

    formatDateBeta(date) {
        const year = date.getFullYear();
        const month = `${date.getMonth() + 1}`.padStart(2, '0');
        const day = `${date.getDate()}`.padStart(2, '0');
        const hours = `${date.getHours()}`.padStart(2, '0');
        const minutes = `${date.getMinutes()}`.padStart(2, '0');
        return `${year}-${month}-${day} ${hours}:${minutes}`;
    }
}

main();
