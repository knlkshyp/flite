const express = require('express'),
        bodyParser = require('body-parser'),
        oracledb = require('oracledb'),
        csv = require('fast-csv'),
        fs = require('file-system');

const app = express();
app.use(bodyParser.urlencoded({extended: true}));

let dbconfig = fs.readFileSync('dbconfig.json', 'utf8');
console.log(dbconfig[0].user);

app.get('/', (req, res) => {
    app.use(express.static('public'));
    res.sendFile(__dirname + '/public/home.html');
});

app.post('/extraction', (req, res) => {

    let SelectAll = Boolean(req.body.SelectAll);
    let Clients = Boolean(req.body.Clients);
    let Contracts = Boolean(req.body.Contracts);
    let Events = Boolean(req.body.Events);
    let Fees = Boolean(req.body.Fees);
    let Schedules = Boolean(req.body.Schedules);
    let ExchangeRates = Boolean(req.body.ExchangeRates);
    let CollateralLinkages = Boolean(req.body.CollateralLinkages);

    if (SelectAll === true) {
        console.log('SelectAll True');

        oracledb.getConnection(
            {
                user          : dbconfig.user,
                password      : dbconfig.password,
                connectString : dbconfig.connectString
            },
            function (err, con) {
                if (err) {
                    console.log(err);
                } else {
                    con.execute("insert into FLITE_EVENT(ID_EXPORT,ID_CONTRACT,DATA_DATE,EVENT_DATE,VALUE_DATE,EVENT_NAME,PRINCIPAL_FLOW,INTEREST_FLOW,FEE_FLOW) values ('2018101011','0408289001377',TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),'NEW_CONTRACT',-435.68,-19948.82,0)", (err, result) => {
                        con.commit((err) => {
                            if (err) {
                                console.log(err);
                            } else  {
                                    console.log('Insertion in Event table successful.');
                                    con.execute("select * from FLITE_EVENT", (err, result) => {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            let data = result.rows;
                                            let ws = fs.createWriteStream('/flite_events.csv');
                                            csv.write(data).pipe(ws);
                                        }
                                    });
                                }
                        });
                    });

                    con.execute("insert into FLITE_FEE(ID_EXPORT,ID_CONTRACT,ID_FEE,DATA_DATE,PAYMENT_DATE,FEE_FLOW,ID_CURRENCY,IS_FEE_MOD,FEE_TYPE) values ('2018101012','1376580020907','PROC_CHG_RATE',TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),2345,'ZMW','0','PROD')", (err, result) => {
                        con.commit((err) => {
                            if (err) {
                                console.log(err);
                            } else  {
                                    console.log('Insertion in Fees table successful.');
                                    con.execute("select * from FLITE_FEE", (err, result) => {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            let data = result.rows;
                                            let ws = fs.createWriteStream('/flite_fees.csv');
                                            csv.write(data).pipe(ws);
                                        }
                                    });
                                }
                        });
                    });

                   con.execute("insert into FLITE_CLIENT(ID_EXPORT,ID_CLIENT,DATA_DATE,GROUP_NAME,SECTOR,COUNTRY) values ('2018101004','5206694',TO_DATE('2018/09/30','YYYY-MM-DD'),'Retail','','ZM')", (err, result) => {
                        con.commit((err) => {
                            if (err) {
                                console.log(err);
                            } else  {
                                    console.log('Insertion in Clients table successful.');
                                    con.execute("select * from FLITE_CLIENT", (err, result) => {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            let data = result.rows;
                                            let ws = fs.createWriteStream('/flite_clients.csv');
                                            csv.write(data).pipe(ws);
                                        }
                                    });
                                }
                        });
                    }); 

                    con.execute("insert into FLITE_SCHEDULE(ID_EXPORT,ID_CONTRACT,ID_SCHEDULE,DATA_DATE,PAYMENT_TYPE,INTEREST_FLOW,PRINCIPAL_FLOW,FEE_FLOW,PAYMENT_DATE) values ('2018101011','0002005004210','00020050042102458392',TO_DATE('2018/09/30','YYYY-MM-DD'),'PI',452.8,583.2,0,TO_DATE('2018/09/30','YYYY-MM-DD'))", (err, result) => {
                        con.commit((err) => {
                            if (err) {
                                console.log(err);
                            } else  {
                                    console.log('Insertion in Schedules table successful.');
                                    con.execute("select * from FLITE_SCHEDULE", (err, result) => {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            let data = result.rows;
                                            let ws = fs.createWriteStream('/flite_schedules.csv');
                                            csv.write(data).pipe(ws);
                                        }
                                    });
                                }
                        });
                    });

                    con.execute("insert into FLITE_FXRATE(ID_EXPORT,ID_CURRENCY,DATA_DATE,FX_RATE) values('2018101004','DKK',TO_DATE('2018/09/30','YYYY-MM-DD'),1.915)", (err, result) => {
                        con.commit((err) => {
                            if (err) {
                                console.log(err);
                            } else  {
                                    console.log('Insertion in Exchange Rates table successful.');
                                    con.execute("select * from FLITE_FXRATE", (err, result) => {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            let data = result.rows;
                                            let ws = fs.createWriteStream('/flite_fxrates.csv');
                                            csv.write(data).pipe(ws);
                                        }
                                    });
                                }
                        });
                    });

                    con.execute("insert into FLITE_COLLATERAL_LINKAGE(ID_EXPORT,ID_CONTRACT,ID_COLLATERAL,DATA_DATE,ID_COLLATERAL_TYPE,ID_CURRENCY,COLLATERAL_CATEGORY,ALLOC_VALUE) values ('2018101012','018GTYG180570004','57306',TO_DATE('2018/09/30','YYYY-MM-DD'),'LANDED_PROPERTY','ZMW','RESIDENTIAL PROPERTY',1006800)", (err, result) => {
                        con.commit((err) => {
                            if (err) {
                                console.log(err);
                            } else  {
                                    console.log('Insertion in Collateral Linkages table successful.');
                                    con.execute("select * from FLITE_COLLATERAL_LINKAGE", (err, result) => {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            let data = result.rows;
                                            let ws = fs.createWriteStream('/flite_collaterallinkages.csv');
                                            csv.write(data).pipe(ws);
                                        }
                                    });
                                }
                        });
                    });

                    con.execute("insert into FLITE_CONTRACT(ID_EXPORT,ID_CONTRACT,DATA_DATE,DATA_VALUE_DATE,ID_CURRENCY,ID_CLIENT,ID_PRODUCT,PRINCIPAL_UNDUE,PRINCIPAL_OVERDUE,INTEREST_UNDUE,INTEREST_OVERDUE,INTEREST_PENALTY,OTHER_OUTSTANDING,OFFBALANCE_UNCOND,OFFBALANCE_COND,ORIGINATION_DATE,IS_ACTIVE,IS_DEFAULT,INTEREST_RATE,INTEREST_REVENUE,MATURITY_DATE,ID_DAY_COUNT_CONVENTION,DAYS_PAST_DUE,IFRS_CATEGORY,GCA_ORIG,IS_WRITE_OFF,ACC_WRITE_OFF_AMT,FX_RATE_ORIG,ID_CLIENT_GROUP,IS_RESTRUCTURED,IS_SICR,MANUAL_STAGE) values ('2018101012','0367144029222',TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),'ZMW','0367144','0670',39118.35,286,27.6,833.97,0,0,0,0,TO_DATE('2018/09/30','YYYY-MM-DD'),'1','0',25.75,10839.84,TO_DATE('2018/09/30','YYYY-MM-DD'),'ACT/365',0,'Amortized Cost',40265.92,'0',0,1,'Retail','0','0','NORM')", (err, result) => {
                        con.commit((err) => {
                            if (err) {
                                console.log(err);
                            } else  {
                                    console.log('Insertion in Contracts table successful.');
                                    con.execute("select * from FLITE_CONTRACT", (err, result) => {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            let data = result.rows;
                                            let ws = fs.createWriteStream('/flite_contracts.csv');
                                            csv.write(data).pipe(ws);
                                        }
                                    });
                                }
                        });
                    });

                }
            }
        );



    } else {

        if (Events === true) {
            console.log('Events True');

            oracledb.getConnection(
                {
                    user          : dbconfig.user,
                    password      : dbconfig.password,
                    connectString : dbconfig.connectString
                },
                function (err, con) {
                    if (err) {
                        console.log(err);
                    } else {
                        con.execute("insert into FLITE_EVENT(ID_EXPORT,ID_CONTRACT,DATA_DATE,EVENT_DATE,VALUE_DATE,EVENT_NAME,PRINCIPAL_FLOW,INTEREST_FLOW,FEE_FLOW) values ('2018101011','0408289001377',TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),'NEW_CONTRACT',-435.68,-19948.82,0)", (err, result) => {
                            con.commit((err) => {
                                if (err) {
                                    console.log(err);
                                } else  {
                                        console.log('Insertion in Event table successful.');
                                        con.execute("select * from FLITE_EVENT", (err, result) => {
                                            if (err) {
                                                console.log(err);
                                            } else {
                                                let data = result.rows;
                                                let ws = fs.createWriteStream('/flite_events.csv');
                                                csv.write(data).pipe(ws);
                                            }
                                        });
                                    }
                            });
                        });
                    }
                }
            );
        }

        if (Fees === true) {
            console.log('Fees True');

            oracledb.getConnection(
                {
                    user          : dbconfig.user,
                    password      : dbconfig.password,
                    connectString : dbconfig.connectString
                },
                function (err, con) {
                    if (err) {
                        console.log(err);
                    } else {
                        con.execute("insert into FLITE_FEE(ID_EXPORT,ID_CONTRACT,ID_FEE,DATA_DATE,PAYMENT_DATE,FEE_FLOW,ID_CURRENCY,IS_FEE_MOD,FEE_TYPE) values ('2018101012','1376580020907','PROC_CHG_RATE',TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),2345,'ZMW','0','PROD')", (err, result) => {
                            con.commit((err) => {
                                if (err) {
                                    console.log(err);
                                } else  {
                                        console.log('Insertion in Fees table successful.');
                                        con.execute("select * from FLITE_FEE", (err, result) => {
                                            if (err) {
                                                console.log(err);
                                            } else {
                                                let data = result.rows;
                                                let ws = fs.createWriteStream('/flite_fees.csv');
                                                csv.write(data).pipe(ws);
                                            }
                                        });
                                    }
                            });
                        });
                    }
                }
            );
        }

        if (Clients === true) {
            console.log('Clients True');

            oracledb.getConnection(
                {
                    user          : dbconfig.user,
                    password      : dbconfig.password,
                    connectString : dbconfig.connectString
                },
                function (err, con) {
                    if (err) {
                        console.log(err);
                    } else {
                        con.execute("insert into FLITE_CLIENT(ID_EXPORT,ID_CLIENT,DATA_DATE,GROUP_NAME,SECTOR,COUNTRY) values ('2018101004','5206694',TO_DATE('2018/09/30','YYYY-MM-DD'),'Retail','','ZM')", (err, result) => {
                            con.commit((err) => {
                                if (err) {
                                    console.log(err);
                                } else  {
                                        console.log('Insertion in Clients table successful.');
                                        con.execute("select * from FLITE_CLIENT", (err, result) => {
                                            if (err) {
                                                console.log(err);
                                            } else {
                                                let data = result.rows;
                                                let ws = fs.createWriteStream('/flite_clients.csv');
                                                csv.write(data).pipe(ws);
                                            }
                                        });
                                    }
                            });
                        });
                    }
                }
            );
        }

        if (Contracts === true) {
            console.log('Contracts True');

            oracledb.getConnection(
                {
                    user          : dbconfig.user,
                    password      : dbconfig.password,
                    connectString : dbconfig.connectString
                },
                function (err, con) {
                    if (err) {
                        console.log(err);
                    } else {
                        con.execute("insert into FLITE_CONTRACT(ID_EXPORT,ID_CONTRACT,DATA_DATE,DATA_VALUE_DATE,ID_CURRENCY,ID_CLIENT,ID_PRODUCT,PRINCIPAL_UNDUE,PRINCIPAL_OVERDUE,INTEREST_UNDUE,INTEREST_OVERDUE,INTEREST_PENALTY,OTHER_OUTSTANDING,OFFBALANCE_UNCOND,OFFBALANCE_COND,ORIGINATION_DATE,IS_ACTIVE,IS_DEFAULT,INTEREST_RATE,INTEREST_REVENUE,MATURITY_DATE,ID_DAY_COUNT_CONVENTION,DAYS_PAST_DUE,IFRS_CATEGORY,GCA_ORIG,IS_WRITE_OFF,ACC_WRITE_OFF_AMT,FX_RATE_ORIG,ID_CLIENT_GROUP,IS_RESTRUCTURED,IS_SICR,MANUAL_STAGE) values ('2018101012','0367144029222',TO_DATE('2018/09/30','YYYY-MM-DD'),TO_DATE('2018/09/30','YYYY-MM-DD'),'ZMW','0367144','0670',39118.35,286,27.6,833.97,0,0,0,0,TO_DATE('2018/09/30','YYYY-MM-DD'),'1','0',25.75,10839.84,TO_DATE('2018/09/30','YYYY-MM-DD'),'ACT/365',0,'Amortized Cost',40265.92,'0',0,1,'Retail','0','0','NORM')", (err, result) => {
                            con.commit((err) => {
                                if (err) {
                                    console.log(err);
                                } else  {
                                        console.log('Insertion in Contracts table successful.');
                                        con.execute("select * from FLITE_CONTRACT", (err, result) => {
                                            if (err) {
                                                console.log(err);
                                            } else {
                                                let data = result.rows;
                                                let ws = fs.createWriteStream('/flite_contracts.csv');
                                                csv.write(data).pipe(ws);
                                            }
                                        });
                                    }
                            });
                        });
                    }
                }
            );
        }

        if (Schedules === true) {
            console.log('Schedules True');

            oracledb.getConnection(
                {
                    user          : dbconfig.user,
                    password      : dbconfig.password,
                    connectString : dbconfig.connectString
                },
                function (err, con) {
                    if (err) {
                        console.log(err);
                    } else {
                        con.execute("insert into FLITE_SCHEDULE(ID_EXPORT,ID_CONTRACT,ID_SCHEDULE,DATA_DATE,PAYMENT_TYPE,INTEREST_FLOW,PRINCIPAL_FLOW,FEE_FLOW,PAYMENT_DATE) values ('2018101011','0002005004210','00020050042102458392',TO_DATE('2018/09/30','YYYY-MM-DD'),'PI',452.8,583.2,0,TO_DATE('2018/09/30','YYYY-MM-DD'))", (err, result) => {
                            con.commit((err) => {
                                if (err) {
                                    console.log(err);
                                } else  {
                                        console.log('Insertion in Schedules table successful.');
                                        con.execute("select * from FLITE_SCHEDULE", (err, result) => {
                                            if (err) {
                                                console.log(err);
                                            } else {
                                                let data = result.rows;
                                                let ws = fs.createWriteStream('/flite_schedules.csv');
                                                csv.write(data).pipe(ws);
                                            }
                                        });
                                    }
                            });
                        });
                    }
                }
            );
        }

        if (ExchangeRates === true) {
            console.log('ExchangeRates True');

            oracledb.getConnection(
                {
                    user          : dbconfig.user,
                    password      : dbconfig.password,
                    connectString : dbconfig.connectString
                },
                function (err, con) {
                    if (err) {
                        console.log(err);
                    } else {
                        con.execute("insert into FLITE_FXRATE(ID_EXPORT,ID_CURRENCY,DATA_DATE,FX_RATE) values('2018101004','DKK',TO_DATE('2018/09/30','YYYY-MM-DD'),1.915)", (err, result) => {
                            con.commit((err) => {
                                if (err) {
                                    console.log(err);
                                } else  {
                                        console.log('Insertion in Exchange Rates table successful.');
                                        con.execute("select * from FLITE_FXRATE", (err, result) => {
                                            if (err) {
                                                console.log(err);
                                            } else {
                                                let data = result.rows;
                                                let ws = fs.createWriteStream('/flite_fxrates.csv');
                                                csv.write(data).pipe(ws);
                                            }
                                        });
                                    }
                            });
                        });
                    }
                }
            );
        }

        if (CollateralLinkages === true) {
            console.log('CollateralLinkages True');

            oracledb.getConnection(
                {
                    user          : dbconfig.user,
                    password      : dbconfig.password,
                    connectString : dbconfig.connectString
                },
                function (err, con) {
                    if (err) {
                        console.log(err);
                    } else {
                        con.execute("insert into FLITE_COLLATERAL_LINKAGE(ID_EXPORT,ID_CONTRACT,ID_COLLATERAL,DATA_DATE,ID_COLLATERAL_TYPE,ID_CURRENCY,COLLATERAL_CATEGORY,ALLOC_VALUE) values ('2018101012','018GTYG180570004','57306',TO_DATE('2018/09/30','YYYY-MM-DD'),'LANDED_PROPERTY','ZMW','RESIDENTIAL PROPERTY',1006800)", (err, result) => {
                            con.commit((err) => {
                                if (err) {
                                    console.log(err);
                                } else  {
                                        console.log('Insertion in Collateral Linkages table successful.');
                                        con.execute("select * from FLITE_COLLATERAL_LINKAGE", (err, result) => {
                                            if (err) {
                                                console.log(err);
                                            } else {
                                                let data = result.rows;
                                                let ws = fs.createWriteStream('/flite_collaterallinkages.csv');
                                                csv.write(data).pipe(ws);
                                            }
                                        });
                                    }
                            });
                        });
                    }
                }
            );
        }


    }


res.redirect('back');

});

let port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server is up on port ${port}.`);
});