import clickhouse from "../../ms-base/src/db/clickhouse/clickhouse";
import assert from "assert";

import boxplot from '@sgratzl/boxplots';
import {v4 as uuid} from 'uuid';
import moment from "moment";
import {getAppRootDir, syncAppendToFile} from "../../ms-base/src/utility-methods/file";

const enum DataType {
    LINEAR = 'linear',
    RAND = 'random',
}

type MeasureCodec = {
    codecName: string,
    min: number,
    max: number,
    outlierCount: number,
    mean: number,
    median: number,
    variance: number,
    iqr: number,
    q1: number,
    q3: number,
    whiskerHigh: number,
    whiskerLow: number,
    count: number,
    miss: number,
}

const range = (from, to, step): any[] =>
    Array.from({length: Math.floor((to - from) / step) + 1}, (v, i) => from + i * step)

const table_name = `t_perf`;

const exec = async (
    data_type: DataType,
    type: string,
    // TODO: codecs merger
    ) => {
    const codecs = [
        'T64, LZ4',
        'Delta, ZSTD(1)',
        'Delta, ZSTD(3)',
        'Delta, LZ4',
        'Delta, LZ4HC',
        'Delta, LZ4HC(3)',
        'Delta, LZ4HC(6)',
        'Delta, LZ4HC(9)',
        'DoubleDelta, ZSTD(1)',
        'DoubleDelta, ZSTD(3)',
        'DoubleDelta, LZ4HC',
        'DoubleDelta, LZ4HC(6)',
        'DoubleDelta, LZ4HC(9)',
        'DoubleDelta, LZ4'
    ]

    let select: MeasureCodec[] = [];
    let insert: MeasureCodec[] = [];

    const root_dir = getAppRootDir();
    const today = moment().format('YYYY-MM-DD')

    for (const codec of codecs) {
        await clickhouse.query(`DROP TABLE IF EXISTS ${table_name};`).toPromise();

        const create_table = `
                CREATE TABLE ${table_name} (
                    n Int32,
        
                    column ${type} default n CODEC(${codec})
        
                ) Engine = MergeTree
                PARTITION BY tuple() ORDER BY tuple();
            `;

        let resp = await clickhouse.query(create_table).toPromise();
        assert(resp['r'] === 1, `not ok. table ${create_table} not created.`)

        let Epochjs = require('epochjs'),
            epochjs1 = new Epochjs(),
            epochjs2 = new Epochjs();

        let total_select_time: number[] = [];
        let total_insert_time: number[] = [];

        let jsonObject: any = {};

        const to = 1000;
        const rows = 10_000_000;

        for(let i of range(1, to, 1)){
            epochjs1.start();

            const insert_q =
                (data_type === DataType.LINEAR)
                    ? `insert into ${table_name} (n) select number from numbers(${rows}) settings max_block_size=1000000;`
                    : `insert into ${table_name} (n) select round(number * rand() / 4294967295, 2) as number from numbers(10000000) settings max_block_size=1000000`

            resp = await clickhouse.query(insert_q).toPromise();
            assert(resp['r'] === 1, `not ok. insert to table ${table_name} failed.`)

            total_insert_time.push(epochjs1.secElapsed());

            epochjs2.start();

            await clickhouse.query(`SELECT column FROM ${table_name}`).toPromise();

            total_select_time.push(epochjs2.secElapsed());

            await clickhouse.query(`TRUNCATE TABLE ${table_name}`).toPromise();
        }

        jsonObject[`bp_res_sel_${codec}`] = boxplot(total_select_time);
        jsonObject[`bp_res_ins_${codec}`] = boxplot(total_insert_time);

        const jo = jsonObject[`bp_res_sel_${codec}`];
        const jo_ins = jsonObject[`bp_res_ins_${codec}`];

        select.push({
            codecName: codec,
            min: jo.min.toFixed(3),
            max: jo.max.toFixed(3),
            outlierCount: jo.outlier.length,
            mean: jo.mean.toFixed(4),
            median: jo.median.toFixed(4),
            variance: jo.variance.toFixed(6),
            iqr: jo.iqr.toFixed(3),
            q1: jo.q1.toFixed(3),
            q3: jo.q3.toFixed(3),
            whiskerHigh: jo.whiskerHigh.toFixed(3),
            whiskerLow: jo.whiskerLow.toFixed(3),
            count: jo.count,
            miss: jo.missing,
        })
        insert.push({
            codecName: codec,
            min: jo_ins.min.toFixed(3),
            max: jo_ins.max.toFixed(3),
            outlierCount: jo_ins.outlier.length,
            mean: jo_ins.mean.toFixed(4),
            median: jo_ins.median.toFixed(4),
            variance: jo_ins.variance.toFixed(6),
            iqr: jo_ins.iqr.toFixed(3),
            q1: jo_ins.q1.toFixed(3),
            q3: jo_ins.q3.toFixed(3),
            whiskerHigh: jo_ins.whiskerHigh.toFixed(3),
            whiskerLow: jo_ins.whiskerLow.toFixed(3),
            count: jo_ins.count,
            miss: jo_ins.missing,
        })
    }

    const select_sorted = select.sort( (a:MeasureCodec,b:MeasureCodec) => a.median - b.median )
    const insert_sorted = insert.sort( (a:MeasureCodec,b:MeasureCodec) => a.median - b.median )

    //console.table(select_sorted);
    //console.table(insert_sorted);

    let asTable = require ('as-table').configure({ delimiter: ' | ', right: true })
    const t_select_sorted=asTable(select_sorted)
    const t_insert_sorted=asTable(insert_sorted)

    await syncAppendToFile(root_dir + `\\${today}-select-query.log`, t_select_sorted);
    await syncAppendToFile(root_dir + `\\${today}-insert-query.log`, t_insert_sorted);

    await clickhouse.query(`DROP TABLE IF EXISTS ${table_name};`).toPromise();

}

exec(DataType.LINEAR, 'DateTime')
   .then(() => { console.log('finish') })
   .catch(() => { clickhouse.query(`DROP TABLE IF EXISTS ${table_name};`).toPromise(); })