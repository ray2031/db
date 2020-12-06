var $config = require('db/config');

var mysql= require('mysql');

var pool = mysql.createPool($config);


function query(sql) {
    return new Promise((resolve, reject) => {
        pool.getConnection(function(err, connection) {
            connection.query(sql, function(err, rows) {
                if(err){
                    reject(err);
                }
                resolve(rows);
                connection.release();
            });
        });
    })
}

function queryWithV(sql,values) {
    return new Promise((resolve, reject) => {
        pool.getConnection(function(err, connection) {
            connection.query(sql,values, function(err, rows) {
                if(err){
                    reject(err);
                }
                resolve(rows);
                connection.release();
            });
        });
    })
}

function transquery(sql) {
    return new Promise((resolve, reject) => {
        pool.getConnection(function(err, connection) {
            connection.beginTransaction(function(err) {
                if (err) {
                    console.log(err)
                    reject(err)
                }
                connection.query(sql, function (error, results) {
                    if (error) {
                        return connection.rollback(function() {
                            connection.release()
                            console.log(error)
                            reject(error)
                            // throw error;
                        });
                    }
                    connection.commit(function(err) {
                        if (err) {
                            return connection.rollback(function() {
                                connection.release()
                                console.log(err)
                                reject(err)
                                // throw err;
                            });
                        }
                        connection.release()
                        resolve(results)
                    });
                });
            });
        });
    })
}

function batchTransquery(sqls) {
    return new Promise((resolve, reject) => {
        pool.getConnection(function(err, connection) {
            connection.beginTransaction(function(err) {
                if (err) {
                    console.log(err)
                    reject(err)
                }
                var promise = []
                for(var i=0; i<sqls.length; i++){
                    promise[i] = new Promise((resolve, reject) =>{
                        connection.query(sqls[i], function(err, rows) {
                            if(err){
                                reject(err);
                            }
                            resolve(rows);
                        });
                    })
                }
                Promise.all(promise).then(function(results){
                    connection.commit(function(err) {
                        if (err) {
                            return connection.rollback(function() {
                                connection.release()
                                console.log(err)
                                reject(err)
                            });
                        }
                        connection.release()
                        resolve(results)
                    });
                }).catch(function(err){
                    connection.rollback(function(error) {
                        connection.release()
                        reject(error)
                    });
                })
            });
        });
    })
}

function pageQuery(sql,page) {
    var pageNumber = page.pageNumber
    var pageSize = page.pageSize
    var start=(pageNumber-1)*pageSize
    return new Promise((resolve, reject) => {
        var psql = "select * from ( "+sql+") t  limit "+start+ ","+pageSize
        var csql = "select count(*) as totals from  ( "+sql+") t"
        var returnData = {}
        query(psql).then(function (data) {
            returnData.data = data
            return query(csql)
        }).then(function (data) {
            returnData.totals = data[0].totals
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

module.exports = {
    query: query,
    transquery:transquery,
    pageQuery: pageQuery,
    queryWithV: queryWithV,
    batchTransquery: batchTransquery
}

