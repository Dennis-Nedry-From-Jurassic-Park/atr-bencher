import {syncAppendToFile} from "../../ms-base/src/utility-methods/file";
import moment from "moment";

const exec = async () => {
    const today = moment().format('YYYY-MM-DD')
    await syncAppendToFile(`./${today}-select-query.log`, "2222");

}
exec();