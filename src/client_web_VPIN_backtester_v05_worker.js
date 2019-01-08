
const BigNumber = require("bignumber.js");

const ROUND_DIGITS = 1000000;
let loopTime = 1200;


class invalanceIpContainer{
    constructor(vb,vs,invalance,id,isDiff,stamp=0){
        this.id = id;
        this.stamp = stamp;
        this.vb = vb;
        this.vs = vs;
        this.invalance = invalance;
        this.isDiff = isDiff;
    }
}

class vpinIpContainer{
    constructor(vpin,cdfvpin,id,isDiff,stamp=0){
        this.id = id;
        this.stamp = stamp;
        this.VPIN = vpin;
        this.cdfVPIN = cdfvpin;
        this.isDiff = isDiff;
    }
}


//cdf計算する際の定数
const P  =  0.2316419;
const b1 =  0.31938153;
const b2 = -0.356563782;
const b3 =  1.781477937;
const b4 = -1.821255978;
const b5 =  1.330274429;



class VolumeBar{
    constructor(bar){
        this.pVolumeBar = bar;
        this.remainVolSum = 0;
        this.lastExecStamp = null;
        this.retainBarPrices = new Array();
        this.retainBarPricesTerm = 1000 * 60 * 60 * 24 * 14;
        this.barPricesIsFull = false;

        //標準偏差をオンラインアルゴリズムで計算する際の変数達
        this.priceSum = 0;
        this.pricePowSum = new BigNumber(0);
        this.beforeBarPrice = 0;
        this.priceDiffSum = 0;
        this.priceDiffPowSum = 0;
        this.sd = 0;
        this.sdDiff = 0;
    }
    //pVolumeBar単位でBarにまとめる
    getVolumeBarData(data){
        this.lastExecStamp = data[data.length-1].stamp;
        let volSum = this.remainVolSum;
        let carryFoward = 0;
        const n = this.pVolumeBar;
        const price = new Array();
        for(let i=0;i<data.length;i++){
            if(carryFoward/ROUND_DIGITS >= n){
                volSum += carryFoward;
                i--;
            }else{
                volSum += data[i].vol * ROUND_DIGITS + carryFoward;
            }
            carryFoward = 0;
            if(volSum/ROUND_DIGITS >= n){
                price.push({price:data[i].price,stamp:data[i].stamp});                
                carryFoward = volSum - (n * ROUND_DIGITS);
                volSum = 0;
            }
        }
        //余り
        if(volSum != 0){
            this.remainVolSum = volSum;
        }
        if(price.length != 0) this.updateSD(price);
        return [price.map(v => v.price),this.lastExecStamp,this.barPricesIsFull];
    }

    //σΔpをオンラインアルゴリズムで計算する
    updateSD(price){
        for(let i=0;i<price.length;i++){
            const diff = (this.retainBarPrices.length > 1) ? this.retainBarPrices[this.retainBarPrices.length -1].price - price[i].price : 0;
            this.retainBarPrices.push(price[i]);
            this.priceSum += price[i].price;
            this.pricePowSum = this.pricePowSum.plus(price[i].price * price[i].price);
            this.priceDiffSum += diff;
            this.priceDiffPowSum += diff * diff;
        }
        while(1){
            if(this.retainBarPrices[this.retainBarPrices.length-1].stamp - this.retainBarPrices[0].stamp > this.retainBarPricesTerm){
                const v = this.retainBarPrices.shift();
                const diff = v.price - this.retainBarPrices[0].price;
                this.priceSum -= v.price;
                this.pricePowSum = this.pricePowSum.minus(v.price * v.price);
                this.priceDiffSum -= diff;
                this.priceDiffPowSum -= diff * diff;
                this.barPricesIsFull = true;
            }else{ 
                break;
            }
        }
        if(this.barPricesIsFull){
            const n = this.retainBarPrices.length;
            this.sd = Math.sqrt((this.pricePowSum.div(n).toNumber() - ((this.priceSum/n)**2)) * (n/(n-1)));
            this.sdDiff = Math.sqrt(((this.priceDiffPowSum/(n-1)) - ((this.priceDiffSum/(n-1))**2)) * ((n-1)/(n-2)));
            //V(x) = E(x*x) - E(x) * E(x)
        }
    }
}


class MyVPINMethod{
    constructor(pVolumeBar,pVPINMesureUnit,isDiff,id){
        
        this.pVolumeBar = pVolumeBar;
        this.pVolumeBucket = 1;

        this.pVPINMesureUnit = pVPINMesureUnit;

        this.isDiff = isDiff;
        this.volumeBars = new Array();
        this.volumeBuckets = new Array();
        this.invalances = new Array();
        this.VPINs = new Array();
        this.sortedVPINs = new Array();
        this.latestVPIN = 0;
        this.latestCdfVPIN = 0;
        this.remainVbPrices = new Array();
        this.remainInvalances = new Array();

        //cdfvpinを計算する際のVPINサンプル期間[ms]
        this.retainVPINTerm = 1000 * 60 * 60 * 24 * 30;

        this.lastExecStamp = null;
        //this.asignInvalances = new Array();
        //this.asignInvalanceSize = 5;
        this.firstExecStamp = null;
        

        this.stateCdfVPINisFull = false;
        this.stateClose = false;
        this.stateForcedTermination = false;    //強制終了モード

        this.volSumIn24h = 0;

        this.id = id;
        
    }
    main(resultPrices,stamp,volSumIn24h,sd){
        this.lastExecStamp = stamp;
        this.volSumIn24h = volSumIn24h;
        this.pVolumeBucket = Math.round((this.volSumIn24h / this.pVPINMesureUnit)/this.pVolumeBar);
        if(this.pVolumeBucket < 1) this.pVolumeBucket = 1;
        const vbPrices = this.remainVbPrices.slice();
        if(resultPrices.length != 0){
            Array.prototype.push.apply(vbPrices,resultPrices);
        }else{
            return [[],[],[]];
        }
        
        const invalances = this.remainInvalances.slice();

        const [resultInvalances,invContainers] = this.calcInvalance(vbPrices,sd);
        if(resultInvalances.length != 0){
            Array.prototype.push.apply(invalances,resultInvalances);
        }else{
            return [[],[],[]];
        }
        /*
        if(this.id === 1){
            console.log(`invalances:${invalances}`);
        }
        */
        const vpinContainers = this.setVPIN(invalances);
    
        return [invContainers,vpinContainers];
    }
    

    calcInvalance(prices,sd){
        const v = this.pVolumeBar;
        const mV = this.pVolumeBucket;
        const invalances = new Array();
        let i=0;
    
        const containers = [];

        while(i<prices.length){
            const bucketPrices = prices.slice(i,i+(mV+1));
            this.remainVbPrices.length = 0;
            if(bucketPrices.length == mV+1){
                let vb = 0;
                let vs = 0;
                
                for(let j=0;j<bucketPrices.length-1;j++){
                    vb += (sd != 0) ? v * this.cdf((bucketPrices[j+1]-bucketPrices[j])/sd) : v/2;
                }
                
                vs = v*mV - vb;

                invalances.push(Math.abs(vb-vs));
                containers.push(new invalanceIpContainer(vb,vs,Math.abs(vb-vs),this.id,this.isDiff));
                /*
                this.asignInvalances.push(vb-vs);
                if(this.asignInvalances.length > this.asignInvalanceSize) this.asignInvalances.shift();
                */
            }else{
                Array.prototype.push.apply(this.remainVbPrices,bucketPrices);
                break;
            }
            i=i+mV;
        }
        for(let i=0;i<containers.length;i++){
            const t = new Date(this.lastExecStamp - (Math.floor(loopTime/containers.length)*(containers.length-1-i)));
            containers[i].stamp = t;
        }
        return [invalances,containers];
    }
    setVPIN(invalances){
        const n = this.pVPINMesureUnit;
        const v = this.pVolumeBar;
        const mV = this.pVolumeBucket;
       
        
        let i=0;
        const containers = [];

        while(i<invalances.length){
            const slicedInvalances = invalances.slice(i,i=i+n);
         
            this.remainInvalances.length = 0;
            

            if(slicedInvalances.length == n){
                const vpin = slicedInvalances.reduce((p,c)=>p+c)/(n*v*mV);
                const value = {VPIN:vpin,stamp:this.lastExecStamp+i};

                this.VPINs.push(value);
                const res = this.setCdfVpin(value);
                const cdfvpin = (this.stateCdfVPINisFull) ? res : 0;
              
                containers.push(new vpinIpContainer(vpin,cdfvpin,this.id,this.isDiff));
               
                this.latestVPIN = vpin;
                this.latestCdfVPIN = cdfvpin;
            }else{
                Array.prototype.push.apply(this.remainInvalances,slicedInvalances);
                break;
            }
        }
        if(this.VPINs.length > 2){

            //古くなったVPINを取り除く処理
            while(1){
                if(this.VPINs[this.VPINs.length-1].stamp - this.VPINs[0].stamp > this.retainVPINTerm){
                    const v = this.VPINs.shift();
                    const index = this.sortedVPINs.indexOf(v);
                    if(index != -1) this.sortedVPINs.splice(index,1);  
                    this.stateCdfVPINisFull = true;  
                }else{ 
                    break;
                }
            }
        }
        for(let i=0;i<containers.length;i++){
            containers[i].stamp = new Date(this.lastExecStamp - (Math.floor(loopTime/containers.length)*(containers.length-1-i)));
        }
        return containers;
    }
    //ソート済みVPINの配列の添字を配列の長さで割ったものがcdfVpinとなる
    setCdfVpin(v){
        let low  = 0;
        let high = this.sortedVPINs.length - 1;
        let i = 0;
        if(high === -1){
            this.sortedVPINs.push(v)
            return 0;
        }
        while(low <= high){
            i = Math.floor((low + high) / 2);
            if(this.sortedVPINs[i].VPIN > v.VPIN){
                high = i - 1;
            }else{
                low = i + 1;
            }
        }
        const setIndex = (this.sortedVPINs[i].VPIN < v.VPIN) ? i+1 : i;
        this.sortedVPINs.splice(setIndex,0,v);

        return setIndex / this.sortedVPINs.length;
    }
    stdev(a){
        let tempLtpAve = 0;
        for(let i=0;i<a.length;i=(i+1)|0) tempLtpAve = tempLtpAve + a[i] * ROUND_DIGITS;
        //整数
        const tLtpAve = Math.round(tempLtpAve/a.length);
        //Σ(xi-xave)^2の計算//n-1で割ってルート.
        let sigma = 0  
        for(let i=0;i<a.length;i=(i+1)|0) sigma = sigma + ((a[i] * ROUND_DIGITS - tLtpAve) ** 2);
        //整数
        sigma = Math.round(Math.sqrt(sigma/(a.length-1)));
        return sigma/ROUND_DIGITS;
    }
    cdf(x){
        // constants
        /*
        const p  =  0.2316419;
        const b1 =  0.31938153;
        const b2 = -0.356563782;
        const b3 =  1.781477937;
        const b4 = -1.821255978;
        const b5 =  1.330274429;
        グローバル変数へ
        */
    
        const t = 1 / (1 + P * Math.abs(x));
        const Z = Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
        const y = 1 - Z * ((((b5 * t + b4) * t + b3) * t + b2) * t + b1) * t;
    
        return (x > 0) ? y : 1 - y;
    }
    
}


//main
{

    let methods = [];
    let workerId = 0;
    const bars = new Array();
    
    self.addEventListener('message',async (message)=>{
        switch(message.data.work){
            case 'init':
                
                const d = message.data.container;
                for(let i=0;i<d.length;i++){
                    if((bars.map(v=>v.pVolumeBar)).indexOf(d[i].bar) == -1) bars.push(new VolumeBar(d[i].bar));
                    methods.push(new MyVPINMethod(d[i].bar,d[i].vpinUnit,true,d[i].id));
                    methods.push(new MyVPINMethod(d[i].bar,d[i].vpinUnit,false,d[i].id));
                }


                loopTime = message.data.loopTime;
                workerId = message.data.workerId;
               

                await _asyncSleep(message.data.ready);

                //getExecutionsのメッセージを送る
                self.postMessage({
                    work : 'firstGetExecutions',
                    id : workerId,
                    first : true
                });
                break;

            //end of init
            case 'sendExecutions':
                
                /*
                class TickExecution{
                    constructor(data){
                        this.vol = data.size;
                        this.price = data.price;
                        this.id = data.id;
                        this.stamp = new Date(data.time);           //date型
                        this.nanoStamp = data.time.getNanoTime();   //nano部 timestampの 文字列
                        this.futurePrices = new Array();
                    }
                }
                */
                
                //execListをloop間隔で分割して 1200[ms]

                const invData = [];
                const vpinData = [];

                const now = Date.now();
                const execList = message.data.execList;
                const slicedExecList = new Array();

                let indexTime = execList[0].stamp;

                //約定履歴を実稼働時のループ時間（1200[ms]）で区切って計算していく
                for(let i=0;i<execList.length;i++){
                    slicedExecList.push(execList[i]);
                    if(execList[i].stamp - indexTime > loopTime || i == execList.length-1){
                        for(let k=0;k<bars.length;k++){
                            const [prices,stamp,isFull] = bars[k].getVolumeBarData(slicedExecList);

                            //volume barが2週間分貯まったら（bar間の標準偏差の計算のため)
                            //オーダーフローインバランス,vpinを計算していく
                            if(isFull){
                                for(let j=0;j<methods.length;j++){
                                    if(bars[k].pVolumeBar === methods[j].pVolumeBar){
                                        const [invCt,vpinCt] = (methods[j].isDiff) ? methods[j].main(prices,stamp,message.data.volSumIn24h,bars[k].sdDiff) : methods[j].main(prices,stamp,message.data.volSumIn24h,bars[k].sd);
                                        if(invCt.length != 0) Array.prototype.push.apply(invData,invCt);
                                        if(vpinCt.length != 0) Array.prototype.push.apply(vpinData,vpinCt);
                                    }
                                }
                            }
                            indexTime = execList[i].stamp;
                            slicedExecList.length = 0; 
                        }
                    }
                }
                
                const workTime = Date.now()-now;
                self.postMessage({
                    work:'sendWorkTime',
                    id : workerId,
                    workTime : workTime 
                });
                self.postMessage({
                    work:'getExecutions',
                    invData : invData,
                    vpinData : vpinData,
                    id : workerId
                });
               
                break;
            //end of board
        }
    })
}

const _asyncSleep = (msec)=>{
    return new Promise(resolve => setTimeout(resolve, msec));
}









