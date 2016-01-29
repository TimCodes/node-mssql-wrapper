var config = require('../config/mainConfig')();
var logger = config.logger;
var sql = require('mssql');
var connection =  new sql.Connection(config.db.sql);
var Promise = require("bluebird");



//logger.log('info',"engage hyper speed");

exports.getAll = function(table){
    return executePreparedStatement("select * from " + table);
};

exports.getById = function getItemById(tableName, id) {
	var param = [{col: 'id', operator: "=", value: id}];
    var sqlStr = "select * from " + tableName;
    return executePreparedStatement(buildWherePrepared(sqlStr, param));
};

exports.getAllWhere = function getItemById(tableName, params) {
    var sqlStr = "select * from " + tableName;
	return  executePreparedStatement(buildWherePrepared(sqlStr, params));
};

exports.insert = function inserItem(tableName, cols, vals) {
    return executePreparedStatement(buildPreparedInsert(tableName, cols, vals));
}


exports.updateById = function updateItemById(tableName, cols, vals, id) {
    var where = [{col: 'id', operator: "=", value: id}];
    return executeQueryStatment(buildPreparedUpdate(tableName, cols, vals, where));
};

exports.updateWhere = function updateItemWhere(tableName, cols, vals, where) {
	return executeQueryStatment(buildPreparedUpdate(tableName,cols, vals, where));
};

exports.deleteById = function delteItemById(tableName, id) {
	var where = [{col: 'id', operator: "=", value: id}];
    return executeQueryStatment(buildPreparedDelete(tableName, where));
};

exports.deleteWhere = function deleteItemWhere(tableName, where) {
	return executeQueryStatment(buildPreparedDelete(tableName, where));
};


//temp for testing purposes refactor this and protect against sql injection 

exports.insertall = function(table, cols, vals){
	
	console.log("insert into " + table +  " (" + cols + ") values" + vals)
	 
	return executeQueryStatment("insert into " + table +  " (" + cols + ") values " + vals, new sql.Connection(config.db.sql));
	//return executeQueryStatment("insert into flowreadings" + " (" + 'time, recordid, ghcorr, ghcorravg, cfs, cfs_avg, battvolt, battvoltmax, batvoltmin, inttemp, watertemp, stationid' + 
	//	      ") values (" + "'12/04/2011 12:00:00 AM','1','-0.03','-0.042','NULL','NULL','13.5','13.56','13.45','16.69','9.77', 'test2'" + " ) ", new sql.Connection(config.db.sql));

}

exports.getColSum = function calcColSum(tableName, sumCol, groupCol) {
   return  executePreparedStatement(buildAggrgatePrepared(tableName, "SUM", sumCol, groupCol));  
}

exports.getColSumWhere = function calcColSumwhere(tableName, sumCol, groupCol, where) {
    return  executePreparedStatement(buildAggrgatePrepared(tableName, "SUM", sumCol, groupCol, where));  
}

exports.getColAvg = function calcColAvg(tableName, sumCol, groupCol) {
     return  executePreparedStatement(buildAggrgatePrepared(tableName, "AVG", sumCol, groupCol));  
}

exports.getColMax= function calcColMax(tableName, sumCol, groupCol) {
    var sqlStr = "select " + groupCol + ", Max(" + sumCol + " ) as Max from " + tableName + " group by " + groupCol;
    console.log(sqlStr)
    return  executeQueryStatment(sqlStr);  
}

exports.getColMin = function calcColMin(tableName, sumCol, groupCol) {
    var sqlStr = "select " + groupCol + ", Min(" + sumCol + " ) as Min from " + tableName + " group by " + groupCol;
    console.log(sqlStr)
    return  executeQueryStatment(sqlStr);  
}



exports.openConn = function(){
	
	return connection.connect();
};

exports.closeConn = function(){
	
	return connection.close();
};

exports.isConnOpen = function () {
    return isConnOpen();
}

exports.insert= function(table, cols, vals){
    
   
	 var request = new sql.Request(connection);
	 var query = "insert into " + table +  " (" + cols + ")  values (" +  vals + ")";
    
	 request.query(query).then(function (data) {
		 // return data here instead?
		 // this way no need to wrap con in promise
		
		
	 })
	 .catch(function(err) {
	
		logger.error(err);
	
		
	})
}


function executePreparedStatement(queryObj) {
    var params = queryObj.params;
    var query  = queryObj.sql;
    
 return  getConn()
         .then(function (conn) {
            var ps = new sql.PreparedStatement(conn);
            var values = {};
            
            //build paramers and value object from array
           
            
            if(params){
                 params.forEach(function (param) {
                 ps.input(param.name, param.type);
                 values[param.name] = param.value;
               });
            }
            
            // return promse from exeuted statement
            return  ps.prepare(query)
            .then(function executePrepare() {
               console.log("query", query) 
               return ps.execute(values);
            })
            .then(function execurePrepare(recordset) {
               console.log("recordset", recordset)
                return recordset;
            })
            .catch(function executePrepareError(err) {
                
                console.log(err);
            })

    
   })
   
   
}
// may need to refactor to open new connection each time 
// this is called 
// maybe no need to wrap whole thing in promise
function executeQueryStatment(query) {
	
	
  return getConn().then(function (conn) {
         
      var request = new sql.Request(conn);
	  return request.query(query).
      then(function (data) {
		
		 return data; 
	
	 })
	 .catch(function(err) {
		var error = {
            type: 'db',
            error: err
        };
       logger.error(JSON.stringify(error));
     })
 });
	 


};

function buildAggrgatePrepared(tableName, aggrType, sumCol, groupCol, where) {
    var sqlStr = "select " + groupCol + "," + aggrType + "(" + sumCol + ") as " + aggrType + " from " + tableName;
    var query = {};
    if (where) {
         query = buildWherePrepared(sqlStr, where);
         query.sql +=  " group by " + groupCol
    } else {
        
        sqlStr +=  " group by " + groupCol
        query.sql = sqlStr;
    }
     
     return query;
    
}

 function buildPreparedInsert(tableName, cols, vals){
       var sqlString    = "insert into " + tableName + "(";
       var paramList    = buildPreparedParamArr(vals); 
      
       cols.forEach(function buildColInsert(col, idx, arr) {
           
           if (idx < (arr.length - 1)) {
               sqlString +=  col + ", ";
           }else{
               sqlString +=  col + " ) ";
           }
       });
       
       sqlString += " values ("
       vals.forEach(function (val, idx, arr) {
     
            if (idx < (arr.length - 1)) {
                sqlString += " @" + paramList[idx].name + ", ";     
            }else{
                sqlString += " @" + paramList[idx].name + ")";    
            }
       }); 
      
      
       return {sql: sqlString, params: paramList};        
}


function buildPreparedUpdate(tableName, cols, vals, where){
       var sqlString    = "update " + tableName + " set ";
       var paramList    = buildPreparedParamArr(vals); 
       var preparedWhere;
      
       cols.forEach(function buildColInsert(col, idx, arr) {
           
           if (idx < (arr.length - 1)) {
               sqlString +=  col + " = @" + paramList[idx].name + ", ";
           }else{
               sqlString +=  col + " = @" + paramList[idx].name;
           }
       });
       
      
       preparedWhere = buildWherePrepared(sqlString, where);
       sqlString     = preparedWhere.sql;
       paramList     = paramList.concat(preparedWhere.params, []) 
      
      
       return {sql: sqlString, params: paramList};        
}

function buildPreparedDelete(tableName, where){
       var sqlString     = "delete from " + tableName;
       var preparedWhere = buildWherePrepared(sqlString, where);
       var paramList     =  preparedWhere.params
           sqlString     =  preparedWhere.sql;
      
       return {sql: sqlString, params: paramList};        
}


function buildWherePrepared(prefix, vals) {
   
    var sqlString    = prefix + " where ";
    var paramList;
    var paramVals     = []; 
    
    vals.forEach(function (param) {
        
        paramVals.push(param.value)
    });
    
    paramList =  buildPreparedParamArr(paramVals);
    console.log("params", paramList);
    vals.forEach(function (val, idx) {
     
        if (idx < 1) {
            sqlString += val.col + " " +  val.operator + " @" + paramList[idx].name;     
        }else{
            sqlString += " AND " + "  "  +  val.col + val.operator + " @" +  paramList[idx].name; 
        }
    }) 
   
    return {sql: sqlString, params: paramList};
}                
   
     
 function isConnOpen() {
    return connection.connected;
 }
 
 function getConn(){
    logger.info("get datbase connection");
    if (isConnOpen()) {
      
       logger.info("connection already open using current connection")
       return Promise.resolve(connection); 
       
    } else {
     
     logger.info("no connection open opening new one ")  
     return  connection.connect().then(function newConnection(connObj) {
            
           return connObj;
        
        }).catch(function errOnConn (err) {
            
            var error = {
                type: 'db',
                error: err
            };
           
            logger.error(JSON.stringify(error));  
            
            
        })
       
    }
    
    
 }

 function buildPreparedParamArr(vals, options ) {
      
      var options = options || {};
      var stringType   =  options.stringType ||  sql.VarChar(50);
      var intType      = options.intType  || sql.Int;
      var floatType    = options.floatType  || sql.Decimal(18, 4);
      var startAlphlen = options.startAlph || 2;
      
      var paramArr = [];
      vals.forEach(function(el, idx, arr){
          
          var paramName =  getRandomParamName(idx + startAlphlen) 
          
          if (typeof el === "string") {
             paramArr.push(buildPreparedParam(paramName,  el, stringType));
            
          } else if(typeof el === "number" ) {
              
              if (isInteger(el)) {
                paramArr.push(buildPreparedParam(paramName, el, intType))  
               } else {
                paramArr.push(buildPreparedParam(paramName, el, floatType))   
               }
          }
      })
      
      return paramArr;
 }

function buildPreparedParam(name, val, type){
      
      return {name: name, value : val, type: type}
      
}

function isInteger(x) {
        return (typeof x === 'number') && (x % 1 === 0);
}


function getRandomParamName(strLength){
    var returnString = ""; 
    var ranNum1;
    var ranNum2;
    for(var i = 0; i < strLength; i++){
        ranNum1 = Math.floor(Math.random() * 10);
        ranNum2 = Math.floor(Math.random() * 20); 
        
        if (i < 1) {
            returnString += String.fromCharCode(Math.floor(Math.random() * ( 90 - 65)) + 65); 
        }else  if(ranNum1 % 2 ){
            
            if (ranNum2 % 2) {
               returnString += String.fromCharCode(Math.floor(Math.random() * ( 122 - 97)) + 97);  
            } else {
                returnString += String.fromCharCode(Math.floor(Math.random() * ( 90 - 65)) + 65); 
            }
            
        }else{
            returnString +=  Math.floor(Math.random() * 10);
        } 
    }
    return returnString;
}

