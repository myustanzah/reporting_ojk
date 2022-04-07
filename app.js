//IMPORT PACKAGE YANG DI BUTUHKAN

const { Pool } = require('pg');
const express = require('express');
const app = express();
const port = 3001;
const ObjectsToCsv = require('objects-to-csv');
const fs = require('fs');
const os = require('os')
// const { Storage } = require('@google-cloud/storage');
// const cron = require('node-cron');
const tmp = require('tmp');
const tmpobj = tmp.dirSync();
// const cors = require('cors');
// const path = require('path');



// setup configurasi DB untuk connect
const pool = new Pool({
    user: 'pd_readonly',
    host: '35.247.171.75',
    database: 'pohondana_db_production',
    password: 'pd_rEdUser',
    port: 5432,
})

// Testing connections jika configurasi berhasil
pool.query('SELECT NOW()', (err, res) => {
    if(err){
        console.log(err, res);
    } else {
        console.log(`BERHASIL CONNECT`);
    }
})


// set configurasi variable yang di butuhkan
const config = {
    format: 'en-US',
    zone: { timeZone: 'Asia/Jakarta' },
    key: 'b2f3f054790b111a0d1c37153e80186acc68cf4d',
    // tempFolder: os.tmpdir(),
    tempFolder: '/tmp',
    bucket: 's-cactus',
    bucketFolder: 'reporting-testing',
    idPenyelenggara: '810073'
}

// const storage = new Storage({ 
//     keyFilename: "google-cloud-keys.json" ,
//     projectId: 'staging-256810'
// });

// storage.getBuckets().then(x => console.log(x));



// Dibawah ini untuk menggunakan background cronjob. dokumentasi lihat di => https://www.niagahoster.co.id/blog/cron-job/ 

// cron.schedule('0 23 * * *', function() {
//     console.log('running a task every minute');
//  });


// Create Rest API untuk trigger agar fucntions jalan
app.get("/", async (req, res)=>{

	//Clean tmp folder agar ada space untuk rewrite file

    console.log(config.tempFolder)
    //Generate File
    await generateCsv().catch(err => console.log(err));
    console.log("File berhasil di generate")

    await new Promise((e)=>{setTimeout(e, 1000)});
    
    //ReWriteCSV to replace comma
    await reWritefile(GetFilename(config.idPenyelenggara));
    console.log("File berhasil di tulis ulang")
    
    // uploadFile(`./report-file/${GetFilename(idPenyelenggara)}.csv`, 's-cactus')
    // getBucketMetadata();
    uploadFile().catch(console.error);
    console.log("File Succes di upload");

    res.send({"Status": "Send report OK", "method": "pool"});

});


function reWritefile(timesNamefile){
    let data = fs.readFileSync(`${config.tempFolder}/${timesNamefile}.csv`, "utf-8").split('\n').splice(1);
    let comma = '|';
    let newData = data.map((e)=>{
        return e.split(',').join(comma);
    })
    return fs.writeFileSync(`./report-file/${timesNamefile}.csv`, newData.join('\n'));
}

function createNameOfDate(){
    const fromDate = new Date();
    fromDate.setDate(fromDate.getDate() - 1);
    const nameFile = fromDate.toLocaleDateString('en-GB', config.zone).split('/').reverse().join('');
    return nameFile
}

async function generateCsv(){

    const account = await pool.query(getQuery().query);
    let dataAccountManipulate = account.rows.map((e)=>{

        if (e["status_pinjaman"] == "L") {
			e["sisa_pinjaman_berjalan"] = "0"
		}

        let dbd = String(e["dpd_terakhir"])
        if (dbd > 90) {
			e["kualitas_pinjaman"] = "3"
		} else if (dbd > 30 ){
			e["kualitas_pinjaman"] = "2"
		} else {
			e["kualitas_pinjaman"] = "1"
		}

        e["tgl_pelaporan_data"] = createNameOfDate()
        e["penyelesaian_w_oleh"] = 2
        e["syariah"] = 0

        if(e["installment_payment_type"] === "SALARY"){
            e["tipe_pinjaman"] = 2
        } else {
            e["tipe_pinjaman"] = 1
        }

        if((e["tipe_pinjaman"] === 1 || e["tipe_pinjaman"] === 2) && e["tipe_loan"] === 'PAYLATER'){
            e["sub_tipe_pinjaman"] = 12
        } else if (e["tipe_pinjaman"] === 1 && e["tipe_loan"] === 'NON-PAYLATER'){
            e["sub_tipe_pinjaman"] = 19
        } else {
            e["sub_tipe_pinjaman"] = 20
        }

        let objData = {
           "id_penyelenggara": e["id_penyelenggara"],
		   "borrower_id" : e["borrower_id"],
		   "jenis_pengguna" : e["jenis_pengguna"],
		   "nama_borrower" : e["nama_borrower"],
		   "no_identitas" : e["no_identitas"],
		   "no_npwp" : e["no_npwp"],
		   "id_pinjaman" : e["id_pinjaman"],
		   "tgl_perjanjian_borrower" : e["tgl_perjanjian_borrower"],
		   "tgl_penyaluran_dana" : e["tgl_penyaluran_dana"],
		   "nilai_pendanaan" : e["nilai_pendanaan"],
		   "tgl_pelaporan_data" : e["tgl_pelaporan_data"],
		   "sisa_pinjaman_berjalan" : Math.floor(e["sisa_pinjaman_berjalan"]),
		   "tgl_jatuh_tempo" : e["tgl_jatuh_tempo"],
		   "kualitas_pinjaman" : e["kualitas_pinjaman"],
		   "dpd_terakhir" : e["dpd_terakhir"],
		   "dpd_max" : e["dpd_max"],
		   "status_pinjaman" : e["status_pinjaman"],
           "penyelesaian_w_oleh": e["penyelesaian_w_oleh"],
           "syariah": e["syariah"],
           "tipe_pinjaman": e["tipe_pinjaman"],
           "sub_tipe_pinjaman": e["sub_tipe_pinjaman"],
           "tipe_loan": e["tipe_loan"]
        }

        return objData
    })

    const csv = new ObjectsToCsv(dataAccountManipulate); // Create CSV file
    
    // Upload CSV file to Path
    await csv.toDisk(`${config.tempFolder}/${GetFilename(config.idPenyelenggara)}.csv`, {allColumns:false});
    
}


async function uploadFile() {
//   await storage.bucket(config.bucket).upload(`./report-file/${GetFilename(config.idPenyelenggara)}.csv`, {
//     destination: `/${config.bucketFolder}/test.csv`,
//   });

    await tmpobj.removeCallback();
    console.log("Dir: ", tmpobj.name);

//   console.log(`${filePath} uploaded to ${bucketName}`);
}

function GetFilename(idPenyelenggara){
    return String(idPenyelenggara) + createNameOfDate() + "SIK01";
}

function getQuery(){
    const dateReport = new Date();
    dateReport.setDate(dateReport.getDate() - 1);
    const dateNow = dateReport.toISOString(config.format, config.zone).replace("T", " ").replace("Z", "")

    let query = `
    select
     account.company_id,
	'810073' as id_penyelenggara,
	account.account_id as borrower_id,
    case account.jenis_pengguna
        when 0 then '1' /* mapping from afpi id for individual */
        when 1 then '2' /* mapping from afpi id for badan hukum */
    end as jenis_pengguna,
    account.name as nama_borrower,
    /* nik if individu or npwp if badan hukum*/
    case account.jenis_pengguna
        when 0 then account.nik
        when 1 then account.npwp
    end as no_identitas,
    account.npwp as no_npwp,
    loan.id as id_pinjaman,
    to_char(loan.transfered_date::date, 'yyyymmdd') as tgl_perjanjian_borrower,
    to_char(loan.transfered_date::date, 'yyyymmdd') as tgl_penyaluran_dana,
    loan.approved_amount as nilai_pendanaan,
  /* assume if there aren't any installment paid, remain debt is approved_amount */
    COALESCE((select
        installment.borrower_remaining_debt
        from installment
        where (installment.loan_id = loan.id 
        and installment.status = 0)
        order by installment.id
        asc limit 1
    ), loan.approved_amount) as sisa_pinjaman_berjalan,
    /* due date of last loan installment */
    (select
        to_char(installment.due_date::date, 'yyyymmdd')
        from installment
        where installment.loan_id = loan.id and
        installment.status = 0
        order by installment.month
        asc limit 1
    ) as tgl_jatuh_tempo,
  /*	dbd => Payment overdue duration in day
        check if all installment is paid by checking payment_date and status of last loan instalment
        if last installment status = 2 and payment_date null, loan is manually adjusted so dbd = 0
        if last installment status = 2 and payment_date is not null, user have paid normally so dbd = payment_date - due_date
        if last installment status = 0 and payment_date null, user have not paid the all installment so dbd = now() - due_date of current active installment
  */
        COALESCE(
            (select
                case
                    when installment.status = 2 and installment.payment_date is null then 0
                    when installment.status = 2 and installment.payment_date is not null then GREATEST(DATE_PART('day', installment.payment_date - installment.due_date), 0)
                    when installment.payment_date is null and installment.status = 0 then null
                end
                from installment
            where installment.loan_id = loan.id
            order by month desc limit 1),
            (select
            GREATEST(DATE_PART('day', '${dateNow}'::date - installment.due_date), 0)
            from installment
            where (installment.loan_id = loan.id
            and status = 0)
            order by month asc limit 1)
            ) as dpd_terakhir,
        (select
            max(
            CASE
                WHEN installment.payment_date is null and installment.status = 2 THEN 0
                WHEN installment.payment_date is null and installment.status = 0 THEN GREATEST(DATE_PART('day', '${dateNow}}'::date - installment.due_date), 0)
                WHEN installment.payment_date is not null and installment.status = 2 THEN GREATEST(DATE_PART('day', installment.payment_date - installment.due_date), 0)
                ELSE null
            END
            )
            from installment
            where installment.loan_id = loan.id
        ) as dpd_max,
        CASE loan.status
            WHEN 8 THEN 'O'
            WHEN 9 THEN 'L'
        END as status_pinjaman,
        account.installment_payment_type,
        CASE WHEN 
        account.company_id in (962, 967, 1041) 
        THEN 'PAYLATER' 
        ELSE 'NON-PAYLATER'
        END as tipe_loan 
        from loan
        left join account on loan.borrower_id = account.id
        where loan.transfered_date is not null and loan.transfered_date::date <= '${dateNow}' and loan.status in(8,9)
        order by account.name asc;`
        return {"query": query, "time": dateNow};
        
    };
    
    app.listen(port, async(req, res)=>{
        console.log("App listen on port", port)
    });
    // and account.company_id not in (301,191,901,541,401,1081) and loan.id not in (156264, 156267, 156266, 156265, 156263, 156262, 156260, 156259,        156257)