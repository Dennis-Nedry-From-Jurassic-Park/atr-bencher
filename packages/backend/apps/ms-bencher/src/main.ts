const path = require('path')

const getRootDir = () => path.parse(process.cwd()).root

const exec = async () => {
    const appRoot = require('app-root-path');

    const {INIT_CWD} = process.env; // process.env.INIT_CWD
    //const paths = require(`${INIT_CWD}/config/paths`);
    //console.log('rootDir='+rootDir)
    console.log('rootDir='+INIT_CWD)


    ///^([^\\/]*[\\/]).*/.test(process.cwd())
    //var root = RegExp.$1;

    //console.log(/^([^\\/]*[\\/]).*/.exec(process.cwd())[1]);
}
exec();
