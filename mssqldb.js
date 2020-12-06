var $config = require('./config');

var mssql= require('mssql');
var SqlString = require('../utils/SqlString');

var pool = new mssql.ConnectionPool($config.authdb,function (error) {
    if(error){
        console.log(error)
    }
});


function query(sql) {
    // console.log(sql)
    return new Promise((resolve, reject) => {
        pool.query(sql, function(err, rows) {
            if(err){
                console.log(err)
                reject(err);
            }
            resolve(rows);
        });
    })
}

function queryWithV(sql,values) {
    return new Promise((resolve, reject) => {
        sql = SqlString.format(sql,values);
        // console.log(sql)
        pool.query(sql, function(err, rows) {
            if(err){
                reject(err);
            }
            resolve(rows);
        });
    })
}

function transquery(command) {
    return new Promise((resolve, reject) => {
        try {
            const transaction = new mssql.Transaction(pool);
            transaction.begin(err => {
                if (err) {
                    reject(err);
                }
                let rolledBack = false
                transaction.on('rollback', aborted => {
                    rolledBack = true
                })
                new mssql.Request(transaction).query(command, (err, result) => {
                    if (err) {
                        if (!rolledBack) {
                            transaction.rollback(err => {
                                if (err) {
                                    reject(err);
                                }
                            })
                        }
                    } else {
                        transaction.commit(err => {
                            if (err) {
                                reject(err);
                            }
                            resolve(result);
                        })
                    }
                })
            })
        }catch (e) {
            console.log(e)
        }
    })
}

async function execSqls(request, sqls){
    var promise =[]
    for (var i = 0; i < sqls.length; i++) {
        promise[i] = await new Promise((resolve, reject) =>{
            try{
                request.query(sqls[i],(err, result) => {
                    if (err) {
                        reject(err)
                    } else{
                        resolve(result)
                    }
                })
            }catch (e) {
                reject(e)
            }

        })
    }
    return promise
}

async function batchTransquery(sqls) {
    return await new Promise((resolve,reject) => {
        const transaction = pool.transaction();
        // await transaction.begin()
        transaction.begin(async err => {
            if (err) {
                reject(err);
            }
            const request = transaction.request()
            try{
                var promise = await execSqls(request, sqls)
                Promise.all(promise).then(function (results) {
                    transaction.commit(err => {
                        if (err) {
                            reject(err);
                        }
                        resolve(results);
                    })
                }).catch(err=>{
                    transaction.rollback(err => {
                        if (err) {
                            reject(err);
                        }
                    })
                    reject(err)
                })
            }catch (e) {
                transaction.rollback(err => {
                    if (err) {
                        reject(err);
                    }
                })
                reject(e)
            }

        })
    })
}

function pageQuery(sql,page,orderstr) {
    var pageNumber = page.pageNumber
    var pageSize = page.pageSize
    var start=(pageNumber-1)*pageSize
    return new Promise((resolve, reject) => {
        var psql = "select top "+pageSize+ " * from (select row_number() \n" +
            "over(order by "+orderstr+") as rownumber,* from ("+sql+") t) tt  where rownumber>"+start;
        // console.log(psql)
        var csql = "select count(*) as totals from  ( "+sql+") t"
        var returnData = {}
        query(psql).then(function (data) {
            returnData.data = data.recordset
            return query(csql)
        }).then(function (data) {
            returnData.totals = data.recordset[0].totals
            resolve(returnData)
        }).catch(function (err) {
            reject(err)
        })
    })
}

// function transquery(sql) {
//     return new Promise((resolve, reject) => {
//         pool.getConnection(function(err, connection) {
//             connection.beginTransaction(function(err) {
//                 if (err) {
//                     console.log(err)
//                     reject(err)
//                 }
//                 connection.query(sql, function (error, results) {
//                     if (error) {
//                         return connection.rollback(function() {
//                             console.log(error)
//                             reject(error)
//                             // throw error;
//                         });
//                     }
//                     connection.commit(function(err) {
//                         if (err) {
//                             return connection.rollback(function() {
//                                 console.log(err)
//                                 reject(err)
//                                 // throw err;
//                             });
//                         }
//                         resolve(results)
//                     });
//                 });
//             });
//         });
//     })
// }

// function queryArgs(sql, args, callback) {
//     pool.getConnection(function(err, connection) {
//         connection.query(sql, args, function(err, rows) {
//             callback(err, rows);
//             connection.release();
//         });
//     });
// }


// console.log(SqlString.format('select 1 from aa where id=? and name=?',['aaa',111]))

module.exports = {
    query: query,
    transquery:transquery,
    pageQuery: pageQuery,
    queryWithV: queryWithV,
    batchTransquery: batchTransquery
}

