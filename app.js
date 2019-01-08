/* 1. expressモジュールをロードし、インスタンス化してappに代入。*/
const express = require("express");
const BigNumber = require('bignumber.js');
const request = require("request");
const crypto = require("crypto"); //node.js標準

let app = express();





class bfAPI{
    constructor(){
    }
    get key(){
        return 'Dummy';
        //return BF_ACCESS_KEY;
    }
    get secret(){
        return 'Dummy';
        //return BF_SECRET_KEY;
    }
    get endPointUrl(){
        return 'https://api.bitflyer.jp'
    }
    getBoardState(pc='FX_BTC_JPY',myCallback){
        const options ={
            url:`${this.endPointUrl}/v1/getboardstate?product_code=${pc}`,
            method : 'GET',
            headers:{
                "Content-Type" : "application/json"
            },
            json:true
        }
        request(options,(error,res,body)=>{
            if(error){
                console.log(`error is ${error}@getBoardState`);
                body = {message:'error recieved.'};
                myCallback(error,res,body,100);
                return 0;
            }
            if(res.statusCode != 200) this._outputError(res);
            myCallback(error,res,body,res.statusCode);
        });
    }
    getBoard(pc='FX_BTC_JPY',myCallback){
        const options ={
            url:`${this.endPointUrl}/v1/getboard?product_code=${pc}`,
            method : 'GET',
            headers:{
                "Content-Type" : "application/json"
            },
            json:true
        }
        //console.log(`options is ${JSON.stringify(options)}`);
        //request(options,myCallback(error,res,body));
        //console.log(JSON.stringify(myCallback));
        request(options,(error,res,body)=>{
            if(error){
                console.log(`error is ${error}@getBoard`);
                body = {message:'error recieved.'};
                myCallback(error,res,body,100);
                return 0;
            }
            if(res.statusCode != 200) this._outputError(res);
            myCallback(error,res,body,res.statusCode);
        });
    }
    getPositions(myCallback){
        const path = '/v1/me/getpositions';
        const sign = this._createSign(path,'GET');
        const options ={
            url: '' + this.endPointUrl + path + '?product_code=FX_BTC_JPY',
            method : 'GET',
            headers: {
                'ACCESS-KEY': this.key,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-SIGN': sign,
                'Content-Type': 'application/json'
            },
            json:true
        }
        request(options,(error,res,body)=>{
            if(error){
                console.log(`error is ${error}@getPosition`);
                body = {message:'error recieved.'};
                myCallback(error,res,body,100);
                return 0;
            }
            if(res.statusCode != 200) this._outputError(res);
            myCallback(error,res,body,res.statusCode);
        });
    }
    getCollateral(myCallback){
        const path = '/v1/me/getcollateral';
        const sign = this._createSign(path,'GET');
        const options ={
            url: '' + this.endPointUrl + path,
            method : 'GET',
            headers: {
                'ACCESS-KEY': this.key,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-SIGN': sign,
                'Content-Type': 'application/json'
            },
            json:true
        }
        request(options,(error,res,body)=>{
            if(error){
                console.log(`error is ${error}@getCollateral`);
                body = {message:'error recieved.'};
                myCallback(error,res,body,100);
                return 0;
            }
            if(res.statusCode != 200) this._outputError(res);
            myCallback(error,res,body,res.statusCode);
        });
    }
    getExecutions(pc='FX_BTC_JPY',id,myCallback){
        const path = '/v1/me/getcollateral';
        const sign = this._createSign(path,'GET');
        const options ={
            url: '' + this.endPointUrl + path + '?product_code=' + pc + '&child_order_acceptance_id=' + id,
            method : 'GET',
            headers: {
                'ACCESS-KEY': this.key,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-SIGN': sign,
                'Content-Type': 'application/json'
            },
            json:true
        }
        request(options,(error,res,body)=>{
            if(error){
                console.log(`error is ${error}@getExecution`);
                body = {message:'error recieved.'};
                myCallback(error,res,body,100);
                return 0;
            }
            if(res.statusCode != 200) this._outputError(res);
            myCallback(error,res,body,res.statusCode);
        });
    }
    sendChildOrder(_side,_size,pc='FX_BTC_JPY',myCallback){
        const path = '/v1/me/getcollateral';
        const body = JSON.stringify({                 
            product_code: pc,
            child_order_type: 'MARKET',
            side: _side,
            size: _size
        });    
        const sign = this._createSign(path,'GET',body);
        const options ={
            url: '' + this.endPointUrl + path + '?product_code=' + pc,
            method : 'GET',
            headers: {
                'ACCESS-KEY': this.key,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-SIGN': sign,
                'Content-Type': 'application/json'
            },
            json:true
        }
        request(options,(error,res,body)=>{
            if(error){
                console.log(`error is ${error}@sendChildOrder`);
                body = {message:'error recieved.'};
                myCallback(error,res,body,100);
                return 0;
            }
            if(res.statusCode != 200) this._outputError(res);
            myCallback(error,res,body,res.statusCode);
        });
    }
    _createSign(path,method,body=''){
        const timestamp = Date.now().toString();
        const text = timestamp + method + path + body; //getはbody空
        const sign = crypto.createHmac('sha256', this.secret).update(text).digest('hex'); //あっち
        return sign;
    }
    _outputError(res){
        console.log('/////////////////////////////////////////////////////////////////////\n');
        console.log(`statusCode ${res.statusCode}@${JSON.stringify(res.request.uri.path)}`);
        console.log(`message ${res.statusMessage}`);
        console.log('\n/////////////////////////////////////////////////////////////////////');
    }
}




const bf =new bfAPI();


//以下appの設定　ルーティング

app.set('port', (process.env.PORT || 8080));

//静的ファイルを提供するところ index.ejsのsrcはpublic内を参照する
app.use(express.static(__dirname + '/public'));


// CORSを許可する
app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});


/* 3. 以後、アプリケーション固有の処理 */

/*
/api/bf/public/board
/api/bf/public/boardstate
/api/bf/private/getcollateral
/api/bf/private/getexecutions
/api/bf/private/getpositions
/api/bf/private/sendchildorder?side=SELL&size=10
*/
//bfから板情報を取得(これもミドルウエア)
app.get("/api/bf/public/board", function(req, res, next){    
    bf.getBoard('FX_BTC_JPY',function(error,response,body,code){
        body.code = code;
        res.send(body);
    });
});

app.get("/api/bf/public/boardstate", function(req, res, next){
    bf.getBoardState('FX_BTC_JPY',function(error,response,body,code){
        body.code = code;
        res.send(body);
    });
});
app.get("/api/bf/private/getcollateral", function(req, res, next){
    bf.getCollateral(function(error,response,body,code){
        body.code = code;
        res.send(body);
    });
});

app.get("/api/bf/private/getexecutions",function(req, res, next){
    bf.getExecutions('FX_BTC_JPY',req.query.id,function(error,response,body){
        body.code = code;
        res.send(body);
    });
});
app.get("/api/bf/private/getpositions", function(req, res, next){
    bf.getPositions(function(error,response,body,code){
        body.code = code; 
        res.send(body);
    });
});
app.get("/api/bf/private/sendchildorder", function(req, res, next){
    let size = req.query.size;
    size = size / ROUND_DIGITS_MIN;
    bf.sendChildOrder(req.query.side,size.toNumber(),'FX_BTC_JPY',function(error,response,body,code){
        body.code = code;
        res.send(body);
    });
});


app.get("/api/bf/web/sendchildorder", function(req, res, next){
    let size = req.query.size;
    size = size / ROUND_DIGITS_MIN;
    bfWeb.pushOrder(req.query.side,size,req.query.stamp,function(body){
        res.send(body);
    });
});

app.set('view engine', 'ejs');

// "/"へのGETリクエストでindex.ejsを表示する。拡張子（.ejs）は省略されていることに注意。
app.get("/", function(req, res, next){
    res.render("index_vpin", {});
});

app.use(function(err, req, res, next) {
    console.log(err.message);
});

app.listen(app.get('port'), function() {
    console.log("Node app is running at localhost:" + app.get('port'))
});
