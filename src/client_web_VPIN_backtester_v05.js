

const ROUND_DIGITS = 1000000;               //小数計算の際の倍数
const START_INTERVAL = 9000;                //計算開始までのインターバル
const USE_DB = 'btcfx_mini_vpin_2';         //使用するDB

const Influx = require("influx");

//influxDBにデータをINSERTするクラス
//2つの配列を用意し、逐次流れてくる計測データをpushする処理とDBにINSERTする処理を非同期で行っている
class MyInflux2{
    constructor(dbName,writeLength,mySchema){
        for(let v in mySchema){
            for(let w in mySchema[v].fields){
                switch(mySchema[v].fields[w]){
                    case 'integer' : 
                        mySchema[v].fields[w] = Influx.FieldType.INTEGER;
                        break;
                    case 'float' : 
                        mySchema[v].fields[w] = Influx.FieldType.FLOAT;
                        break;
                    case 'string' :
                        mySchema[v].fields[w] = Influx.FieldType.STRING;
                        break;
                }
            }
        }
        this.influx = new Influx.InfluxDB({
            host: 'localhost',
            database : dbName,
            schema: mySchema
        });
        this.ipData = [{},{}];
        for(let v in mySchema){
            for(let i=0;i<this.ipData.length;i++){
                this.ipData[i][mySchema[v].measurement] = {
                    data:new Array(),
                    writeLength : writeLength[v],
                    laptime : 0,
                    name : mySchema[v].measurement
                }
            }
            console.log(`mySchema[${v}].measurement is ${mySchema[v].measurement}`);
            console.log(JSON.stringify(this.ipData[0][mySchema[v].measurement]));
        }
        this.inWrite = false;
        this.ipWriteIndex = 1;
        this.ipPushIndex = 0;

        this.checkPing();   
    }
    checkPing(){
        this.influx.ping(5000).then(hosts => {
            hosts.forEach(host => {
              if (host.online) {
                console.log(`${host.url.host} responded in ${host.rtt}ms running ${host.version}. DBに繋がったよ！`)
              } else {
                console.log(`${host.url.host} is offline :( おかしいなDBにつながらない・・・`)
              }
            })
        });
    }
    async writeData(){
        this.inWrite = true;
        for(let p in this.ipData[this.ipWriteIndex]){
            const d = this.ipData[this.ipWriteIndex][p];
            if(d.data.length > d.writeLength){
                let index = 0;
                while (index < d.data.length){
                    const wd = d.data.slice(index,index+d.writeLength);
                    await this.influx.writePoints(wd).catch((err)=>{
                        console.log(`influx write data faild. Message:${err}`);
                    });
                    
                    index += d.writeLength;
                }
            }else{
                await this.influx.writePoints(d.data).catch((err)=>{
                    console.log(`influx write data faild. Message:${err}`);
                });;
            }
            this.ipData[this.ipWriteIndex][p].data.length = 0;
        }
        this.inWrite = false;
    }
    pushData(msName,myData){
        this.ipData[this.ipPushIndex][msName].data.push(myData);
        //this.writeData();
    }
    flipIndex(){
        this.ipPushIndex = (this.ipPushIndex === 1) ? 0 : 1;
        this.ipWriteIndex = (this.ipWriteIndex === 1) ? 0 : 1;
    }
}


const myInflux = new MyInflux2(USE_DB,[5000,5000],[
    {
        measurement: 'invalance',
        fields: {
          vb : 'float',
          vs : 'float',
          invalance : 'float'
        },
        tags: [
          'bar','vpin_unit','type','isDiff'
        ]
    },
    {
        measurement: 'vpin',
        fields: {
          vpin : 'float',
          cdf_vpin : 'float'
        },
        tags: [
          'bar','vpin_unit','type','isDiff'
        ]
    }
]);

//約定履歴持ってくるためのインスタンス
const execInflux = new Influx.InfluxDB({
    host: 'localhost',
    database : 'btcfx_mini_executions'
});
execInflux.ping(5000).then(hosts => {
    hosts.forEach(host => {
      if (host.online) {
        console.log(`${host.url.host} responded in ${host.rtt}ms running ${host.version}. DBに繋がったよ！`)
      } else {
        console.log(`${host.url.host} is offline :( おかしいなDBにつながらない・・・`)
      }
    })
});



//約定履歴
class TickExecution{
    constructor(data){
        this.vol = data.size;
        this.price = data.price;
        this.id = data.id;
        this.stamp = (new Date(data.time)).getTime();           //date型
        this.nanoStamp = data.time.getNanoTime();               //nano部 timestampの 文字列
    }
}

//約定履歴のパケット
class ExecPacket{
    constructor(data,id){
        this.list = new Array();
        this.volSum = 0;

        //dataはTickCountの形にしなければならない
        for(let i=0;i<data.length;i++){
            const d = new TickExecution(data[i]);
            this.list.push(d);
            this.volSum += this.list[this.list.length-1].vol * ROUND_DIGITS;
        }
        this.volSum = this.volSum / ROUND_DIGITS;
        this.nextDataIndexNanoStamp = this.list[this.list.length-1].nanoStamp;
        this.nextDataIndexTime = this.list[this.list.length-1].stamp;
        this.packetId = id;
    }
}

//worker達に計算の命令をしたり、計算の進捗管理をしたり、計算結果をDBに渡したりするクラス
class CalcManager{
    constructor(workerCount,lb){
        
        //1Packet何個の約定履歴を持つか
        this.packetSize = 2500; 

        this.nextPacket = null;

        //過去の取引量合計を算出する時間[ms]
        this.retainVolSumTerm = 1000 * 60 * 60 * 24;

        this.dividedBy24h = 72;
        this.lastPacketId = 0;

        //24時間取引量（出来高）
        this.volSumIn24h = 0;

        //過去24h分の約定履歴を保持しておく配列
        this.volSums = new Array();
        this.volSumIsFull = false;

        this.workerCount = workerCount;
        this.inLoad = false;
        this.isDoneWorkers = new Array(workerCount);
        this.isDoneWorkers.fill(false);
        this.nextDataIndexTime = '';    //nanoTimeString
        this.propotion = 0;
        this.volLabel = lb;
    }
    async init(){
        //まずは先頭（タイムスタンプ古い方）からretainPacketSize分取ってくる
        const head = await execInflux.query(`SELECT * FROM "execution" WHERE time > '2017-01-01 0:00:30.285' LIMIT ${this.packetSize}`);

        this.nextPacket = new ExecPacket(head,this.lastPacketId,this.workerCount);

        this.volSums.push({vol:this.nextPacket.volSum,stamp:this.nextPacket.nextDataIndexTime});

        this.fromDate = (new Date(head[0].time)).getTime();
        this.toDate = (new Date(this.fromDate + (1000*60*60*24*76))).getTime();
        this.nowDate = (new Date(head[1].time)).getTime();

        return new Promise((resolve,reject)=>resolve());
    }
    async loadPacket(){
        this.inLoad = true;
        console.log(`id : ${this.lastPacketId}, stamp : ${this.nextPacket.nextDataIndexNanoStamp}`);
        const newData = await execInflux.query(`SELECT * FROM "execution" WHERE time > ${this.nextPacket.nextDataIndexNanoStamp} LIMIT ${this.packetSize}`);
        this.lastPacketId++;
        this.nextPacket = new ExecPacket(newData,this.lastPacketId,this.workerCount);
        this.updateVolSum(this.nextPacket);
        this.nowDate = this.nextPacket.list[0].stamp;
        this.inLoad = false;
        return new Promise((resolve,reject)=>resolve());
    }
    checkIsDoneWork(id){
        this.isDoneWorkers[id] = true;
        return this.isDoneWorkers.reduce(((a,c) => a && c));
    }
    resetIsDoneWork(){
        this.isDoneWorkers.fill(false);
    }
    getPropotion(){
        /*
            進捗パーセントの計算
            取得が昇順か降順かによって処理変わる
            　過去→現在の順
        */
        this.propotion = (Math.round(((this.nowDate - this.fromDate)/(this.toDate - this.fromDate))*10000)/100);
        return this.propotion;
    }
    get24hVolSum(){
        return (this.volSumIsFull) ? Math.round((this.volSumIn24h / ROUND_DIGITS)/this.dividedBy24h) : 0;
    }
    updateVolSum(packet){
        this.volSums.push({vol:packet.volSum,stamp:packet.nextDataIndexTime});
        this.volSumIn24h += packet.volSum * ROUND_DIGITS;
        while(1){
            if(this.volSums[this.volSums.length-1].stamp - this.volSums[0].stamp > this.retainVolSumTerm){
                const v = this.volSums.shift();
                this.volSumIn24h -= v.vol * ROUND_DIGITS;  
                this.volSumIsFull = true;  
            }else{ 
                break;
            }
        }
        this.volLabel.innerHTML = `24h Transaction volume ... ${Math.round(this.volSumIn24h/ROUND_DIGITS)}`;
    }
}

class DummyMethod{
    constructor(bar,vpinUnit){
        this.bar = bar;
        this.vpinUnit = vpinUnit;
        this.side = 'NP';
    }
}

//main
(async()=>{
    /*
         ・1秒あたりの取引量平均
         ・volume barは2〜100の2枚刻み (50)
         ・volume bucketは200〜2200の50刻み
         ・VPIN計測単位は固定
         ・ 
    */
    /*
        1:volumeBar
        2:volumeBarの倍数 bucket
        3:VPINユニット

    */
    //worker達初期化
    const cores = 8 - 1;
    let workers = [];
    const mainLoopTime = 1200;

    const progressLabel = document.getElementById('progress');
    const volLabel = document.getElementById('vol24');
   
    const manager = new CalcManager(cores,volLabel);
    await manager.init();

    //volume barは3~9で1刻み
    const barDelta = 3;
    const barTick = 1;
    const barCount = 7;

    //vpin計測単位は2から40で2刻み
    const vpUnitDelta = 2;
    const vpUnitTick = 2;
    const vpUnitCount = 20;

    //volume backetは24時間取引高/divided24h(72)をVPIN計測単位で割った値

    const worktimeLabel = new Array(cores);
    const workIsDoneLabel = new Array(cores);
    const inWriteLabel = document.getElementById('in_write');
    for(let i=0;i<cores;i++){
        worktimeLabel[i] = document.getElementById(`worker_${i}`);
        workIsDoneLabel[i] = document.getElementById(`worker_isDone_${i}`);
    }
    
    
    //メソッドの初期化はworkerに投げる
    const mmPairs = [];
    
    //DummyMethodとMarketの初期化 mmPairsは1次元配列,
    let mmCount = 0;
    const cont = new Array(cores);
    for(let i=0;i<cores;i++) cont[i] = new Array();
    
    for(let i = 0;i<barCount;i=(i+1)|0){
        const bar = (i * barTick) + barDelta;
        for(let k=0;k<vpUnitCount;k=(k+1)|0){
            const vpUnit = (k*vpUnitTick)+vpUnitDelta;
            mmPairs.push({
                method : new DummyMethod(bar,vpUnit),
                id : mmCount
            });
            cont[i%cores].push({
                bar : mmPairs[mmPairs.length-1].method.bar,
                vpinUnit : mmPairs[mmPairs.length-1].method.vpinUnit,
                id : mmPairs[mmPairs.length-1].id
            });
            mmCount = (mmCount+1)|0;
        }
    }

    for(let i=0;i<cores;i++){
        const nworker = new Worker('bundle_worker.js');
        const workerEventListener = async (message) =>{
            switch(message.data.work){
                case 'getExecutions':
                    //データくれの仕事
                    for(let i=0;i<message.data.invData.length;i++){
                        myInflux.pushData('invalance',{
                            measurement: 'invalance',
                            tags: {
                                bar: mmPairs[message.data.invData[i].id].method.bar,
                                vpin_unit: mmPairs[message.data.invData[i].id].method.vpinUnit,
                                type: 'local',
                                isDiff:message.data.invData[i].isDiff
                            },
                            fields: {
                                vb: message.data.invData[i].vb,
                                vs: message.data.invData[i].vs,
                                invalance: message.data.invData[i].invalance
                            },
                            timestamp: message.data.invData[i].stamp
                        });
                    }
                    for(let i=0;i<message.data.vpinData.length;i++){
                        myInflux.pushData('vpin',{
                            measurement: 'vpin',
                            tags: {
                                bar: mmPairs[message.data.vpinData[i].id].method.bar,
                                vpin_unit: mmPairs[message.data.vpinData[i].id].method.vpinUnit,
                                type: 'local',
                                isDiff:message.data.invData[i].isDiff
                            },
                            fields: {
                                vpin: message.data.vpinData[i].VPIN,
                                cdf_vpin: message.data.vpinData[i].cdfVPIN,
                            },
                            timestamp: message.data.vpinData[i].stamp
                        });
                    }
                    workIsDoneLabel[message.data.id].innerHTML = 'Done!';

                    //すべてのworkerが仕事終わったら次のデータをすべてのworkerに渡す
                    if(manager.checkIsDoneWork(message.data.id)){
                        while(myInflux.inWrite){
                            await _asyncSleep(50);
                            inWriteLabel.innerHTML = 'in write...';
                        }
                        while(manager.inLoad){
                            await _asyncSleep(50);
                            inWriteLabel.innerHTML = 'in load...';
                        }
                        inWriteLabel.innerHTML = '';
                        
                        const prg = manager.getPropotion();
                        progressLabel.innerHTML = `${prg}%`;

                        myInflux.flipIndex();
                        myInflux.writeData(true);
                        if(prg >= 100){
                            workers.forEach((v)=> v.worker.terminate());
                            progressLabel.innerHTML = `${prg}% Done!`;
                        }
                        
                        manager.isDoneWorkers.fill(false);
                        for(let i=0;i<workIsDoneLabel.length;i++){
                            workIsDoneLabel[i].innerHTML = 'in calc';
                        }
                        for(let i=0;i<workers.length;i++){
                            workers[i].worker.postMessage({
                                work : 'sendExecutions',
                                execList : manager.nextPacket.list,
                                volSumIn24h : manager.get24hVolSum()
                            });
                        }
                        manager.loadPacket();    
                    }
                   
                    break;
                //end of getExecutions
                case 'firstGetExecutions' :
                    nworker.postMessage({
                        work : 'sendExecutions',
                        execList : manager.nextPacket.list
                    });
                    break;
                //end of sendWorkTime
                case 'sendWorkTime' : 
                    worktimeLabel[message.data.id].innerHTML = `${message.data.workTime} [msec]`;
                    break;　
                //end of sendWorkTime
                case 'sendIpMethodParameters' :
                    for(let i=0;i<message.data.invData.length;i++){
                        myInflux.pushData('invalance',{
                            measurement: 'invalance',
                            tags: {
                                bar: mmPairs[message.data.invData[i].id].method.bar,
                                vpin_unit: mmPairs[message.data.invData[i].id].method.vpinUnit,
                                isDiff:message.data.invData[i].isDiff,
                                type: 'local'
                            },
                            fields: {
                                vb: message.data.invData[i].vb,
                                vs: message.data.invData[i].vs,
                                invalance: message.data.invData[i].invalance
                            },
                            timestamp: message.data.invData[i].stamp
                        });
                    }
                    for(let i=0;i<message.data.vpinData.length;i++){
                        myInflux.pushData('vpin',{
                            measurement: 'vpin',
                            tags: {
                                bar: mmPairs[message.data.vpinData[i].id].method.bar,
                                vpin_unit: mmPairs[message.data.vpinData[i].id].method.vpinUnit,
                                isDiff:message.data.invData[i].isDiff,
                                type: 'local'
                            },
                            fields: {
                                vpin: message.data.vpinData[i].VPIN,
                                cdf_vpin: message.data.vpinData[i].cdfVPIN,
                            },
                            timestamp: message.data.vpinData[i].stamp
                        });
                    }
                    break;
                //end of IpMethodParameters
            }        
        }
        nworker.addEventListener('message',workerEventListener);
        nworker.addEventListener('error', (error) => {
            console.log(`worker error : ${error}`);
        });
        workers.push({worker:nworker,id:i});

        workers[i].worker.postMessage({
            work : 'init',
            container : cont[i],
            loopTime : mainLoopTime,
            ready : START_INTERVAL,
            workerId : i
        });
    }
})();

const _asyncSleep = (msec)=>{
    return new Promise(resolve => setTimeout(resolve, msec));
}












