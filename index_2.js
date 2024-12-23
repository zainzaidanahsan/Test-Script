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
        const stageS = await this.getStageTask(task)
        const priorityLabel = task.priority === 4 ? 'Normal' : task.priority === 5 ? 'Urgent' : task.priority;
        const stateLabel = task.state === 1 ? 'Open' : task.state === 3 ? 'Closed Completed' : task.state === 4 ? 'Closed Incompleted' : task.state; 

    
        const contexts = await this.conn.query(`select name, stage from wf_context where id = '${task.sys_id}'`);
        const slaStage = await this.conn.query(`
            SELECT stage 
            FROM task_sla 
            WHERE sys_id = '${task.sys_id}';
        `);
    
        let stageName = '';
        if (contexts && contexts.length > 0) {
            const context = contexts[0];
            const stages = await this.conn.query(`select name from wf_stage where sys_id = '${context.stage}'`);
            stageName = stages[0]?.name;
        }
        function isValidDate(date) {
            return date instanceof Date && !isNaN(date.getTime());
        }
    
        const closedAtDate = new Date(task.closed_at)
        const resolvedAtDate = new Date(task.a_dtm_2);
        const openedAtDate = new Date(task.opened_at);


        const variables = await this.conn.query(`
            SELECT opt.value 
            FROM sc_item_option_mtom mtom
            JOIN sc_item_option opt ON mtom.sc_item_option = opt.sys_id
            WHERE mtom.request_item = '${task.sys_id}'
        `);

        // Variabel untuk menyimpan hasil pencarian
        let requestSubject = '';
        let explainRequest = '';
        let regionVariable = '';
        let sourceVariable = '';

        // Loop untuk memeriksa setiap elemen berdasarkan kondisi yang diberikan
        if (variables && variables.length > 0) {
            for (let i = 0; i < variables.length; i++) {
                const variableValue = variables[i]?.value || '';


                // Logika untuk "request_subject"
                if (!requestSubject) {
                    if (
                        /^(FW:|RE:|PD:|AW:|AP:)/i.test(variableValue) ||  // Pastikan mengandung "FW:", "RE:", atau "PD:"
                        /^(b5dc152e1b3ce810930821b4bd4bcba7)/i.test(variableValue) &&
                        variableValue.length > 10 &&             // Ambil yang lebih dari 10 karakter
                        !/Email Ingestion/i.test(variableValue)  // Hindari "Email Ingestion"
                    ) {
                        requestSubject = variableValue;
                    }
                }

                // Logika untuk "explain_request"
                if (!explainRequest) {
                    if (
                        variableValue.length > 50 ||             // Ambil yang lebih dari 100 karakter
                        /(Dear|Please)/i.test(variableValue) ||    // Ambil yang mengandung "Dear" atau "Please"
                        /(2fb5302a1b3c205061c38739cd4bcbf0)/i.test(variableValue)      // Ambil yang mengandung "Dear" atau "Please"
                    ) {
                        if (requestSubject !== variableValue) {
                            explainRequest = variableValue;
                        }
                    }
                }
                if(!sourceVariable){
                    if(
                        // /(4bb40dad1bb46810930821b4bd4bcb9a | 09540bfd1b34a810930821b4bd4bcb54)/i.test(variableValue) ||
                        // /(4bb40dad1bb46810930821b4bd4bcb9a)/i.test(variableValue)
                        variableValue.length < 10 &&
                        /\bInternal\b/i.test(variableValue) ||        // Cari yang mengandung "Internal"
                        /\bExternal\b/i.test(variableValue)
                        // /^(Internal|External)$/.test(variableValue) &&
                        // variableValue.length < 9  // Cari nilai dengan panjang 2 hingga 4 karakter
                        
                    ){
                        sourceVariable = variableValue;
                        
                    }
                }

                // if(!regionVariable){
                //     if(
                //         // /(378343fd1b34a810930821b4bd4bcbce | b811faa81bf02c1061c38739cd4bcbb6)/i.test(variableValue) ||
                //         // /(b811faa81bf02c1061c38739cd4bcbb6)/i.test(variableValue)
                //     ){
                //         regionVariable = variableValue
                //     }
                // }

                if (!regionVariable) {
                    if (
                        /^(EMEA|LA|APAC|EE)$/.test(variableValue)  // Cari nilai dengan panjang 2 hingga 4 karakter
                    ) {
                        regionVariable = variableValue;
                    }
                }
                


                // Berhenti jika kedua field sudah ditemukan
                if (requestSubject && explainRequest && regionVariable) {
                    break;
                }
            }
        }

        // Jika tidak ditemukan, tambahkan pesan debug untuk memeriksa query
        if (!requestSubject && !explainRequest) {
            console.log('No matching variables found for Request Subject or Explain Request.');
        }

        const dbDumpData = await this.conn.query(`
            SELECT number, stage, u_closed_time, assigned_to, reopening_count, u_external_user_s_email, request
            FROM dbdump
            WHERE number = '${task.number}'
        `);

        const dbRow = dbDumpData[0];
        const uClosedDate = dbRow.u_closed_time ? new Date(dbRow.u_closed_time).toISOString().split('T')[0] : 'Null';
// Region old come from regionVariable || task.a_str_27
        
        const data = {
            'Number': task.number,
            'Opened': openedAtDate,
            'Company Code': companyCode,
            'Region': dbRow.u_ritm_region || regionVariable,
            'Priority': priorityLabel,
            'Source': dbRow.u_ritm_source || sourceVariable || task.a_str_26,
            'Item': catItemName,
            'Short Description': task.short_description,
            'Resolution Note': task.a_str_10,
            'Resolved': uClosedDate,
            'Closed': closedAtDate,
            'Stage': dbRow.stage || task.a_str_1,
            'State': stateLabel,
            'PMI Generic Mailbox': task.a_str_23,
            'Email TO Recipients': task.a_str_25,
            'Email CC Recipients': task.a_str_24,
            'External User\'s Email': dbRow.u_external_user_s_email || task.a_str_24 ||'N/A',
            'Sys Email Address': task.sys_created_by,
            'Contact Type': task.contact_type,
            'Assigned To': dbRow.assigned_to || 'N/A',
            'Resolved By': assignedTo,
            'Contact Person': task.a_str_28,
            'Approval': task.approval,
            'Approval Request': task.a_str_11,
            'Approval Set': task.approval_set,
            'Reassignment Count': task.reassignment_count,
            'Related Ticket': reference,
            'Reopening Count': dbRow.reopening_count || 0,
            'Comments And Work Notes': commentsAndWorkNotes,
            'Request': dbRow.request,
            'Sys Watch List': task.a_str_24,
            'Request Subject': task.short_description,  
            'Explain Request': explainRequest    
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

    async getStageTask(task){
        const stageTask = await this.conn.query(`select stage from task_sla where sys_id = '${task.sysId}'`);
        return stageTask[0]?.stage || '';
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
        const ritmList = this.includedRitms.map(ritm => `'${ritm}'`).join(',');
        return this.conn.query(`select * from task where sys_class_name = 'sc_req_item' and number in (${ritmList}) order by number desc limit ${limit} offset ${offset};`);
    }
    // async getTasks(offset, limit) {
        
    //     return this.conn.query(`
    //         SELECT * 
    //         FROM task 
    //         WHERE sys_class_name = 'sc_req_item' 
    //         ORDER BY number DESC
    //         LIMIT ${limit} OFFSET ${offset};
    //     `);
    // }
    

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

        const dirPath = path.dirname(attachmentFilePath);
        if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
        }

        if (meta.compressed > 0) {
            this.writeCompressedFile(attachmentFilePath, concatenatedBuffer);
        } else {
            this.writeFile(attachmentFilePath, concatenatedBuffer);
        }
    }

    decodeMultipartBase64(base64Chunks) {
        const binaryChunks = base64Chunks.map(chunk => Buffer.from(chunk, 'base64'));
        return Buffer.concat(binaryChunks);
    }

    writeCompressedFile(filepath, buf) {
        try { execSync('rm tmp', { stdio: [] })} catch (e) {};
        fs.writeFileSync('tmp.gz', buf);
        execSync(`gzip -d tmp.gz && mv tmp ${filepath}`);
    }

    writeFile(filepath, buf) {
        fs.writeFileSync(filepath, buf);
    }

    getTaskPath(groupPath, task) {
        return `${groupPath}/${task.number}_${this.formatDateWithTime(task.sys_created_on)}`;
    }

    getGroupPath(tasks) {
        const startTask = tasks[0];
        const endTask = tasks[tasks.length - 1];
        return `${this.resultDir}/${startTask.number}-${endTask.number}_${this.formatDate(startTask.sys_created_on)}_${this.formatDate(endTask.sys_created_on)}`;
    }

    formatDateBeta(date) {
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = date.getFullYear();
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${day}/${month}/${year} ${hours}:${minutes}`;
    }

    formatDate(date) {
        const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
        const day = String(date.getDate()).padStart(2, '0');
        const month = months[date.getMonth()];
        const year = date.getFullYear();
        return `${day}${month}${year}`;
    }

    formatDateWithTime(date) {
        const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
        const day = String(date.getDate()).padStart(2, '0');
        const month = months[date.getMonth()];
        const year = date.getFullYear();
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${day}${month}${year}_${hours}${minutes}`;
    }
}

main();
