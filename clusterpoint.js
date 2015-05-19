var conn = null;

module.exports = function(RED) {
    "use strict";
    var cps = require('cps-api');

    function ClusterpointNode(n) {
        RED.nodes.createNode(this,n);

        var node = this;
        // var conn = null;

        node.cspDatabase = n.cspDatabase;
        node.topic = n.topic;
        node.request = n.request;

        node.cspConfig = RED.nodes.getNode(this.cspDatabase);
        node.tcp = node.cspConfig.tcp;
        node.port = node.cspConfig.port;
        node.user = node.cspConfig.user;
        node.password = node.cspConfig.password;
        node.db = node.cspConfig.db;
        node.account = node.cspConfig.account;
        node.document = node.cspConfig.document;
        node.documentId = node.cspConfig.documentId;

        // var msg = {};

        console.log(node.cspConfig);

        if(!conn) conn = new cps.Connection(node.tcp + ":" + node.port, node.db, node.user, node.password, node.document, node.documentId, {account: node.account});


        // respond to inputs....
        this.on('input', function (msg) {
            try{

                var request = null;
                if(node.request == "INS"){
                    request = new cps.InsertRequest(msg.payload);
                }else if(node.request == "UPD"){
                    request = new cps.UpdateRequest(msg.payload);
                }else if(node.request == "REPL"){
                    request = new cps.ReplaceRequest(msg.payload);
                }else if(node.request == "PARTREPL"){
                    request = new cps.PartialReplaceRequest(msg.payload);
                }else if(node.request == "DEL"){
                    request = new cps.DeleteRequest(msg.payload);
                }else if(node.request == "SERC"){
                    var str_search = "{";
                    for(var i in  msg.payload){
                        for(var k in msg.payload[i]){
                            str_search += cps.Term(msg.payload[i][k], k);
                        }
                    }
                    str_search += "}";
                    request = new cps.SearchRequest(str_search);

                    if(msg.ordering){
                        request.setOrdering(eval("cps." + msg.ordering));
                    }
                }else if(node.request == "LKUP"){
                    var ids = (msg.payload.ids ? msg.payload.ids : []);
                    var doc = (msg.payload.doc ? msg.payload.doc : {});

                    request = new cps.LookupRequest(ids, doc);
                }else if(node.request == "RETRV"){
                    request = new cps.RetrieveRequest(msg.payload);
                }else if(node.request == "LSLAST"){
                    request = new cps.ListLastRequest(msg.payload, 0, 2);
                }else if(node.request == "LSPATHS"){
                    request = new cps.ListLastRequest();
                }else if(node.request == "STATUS"){
                    request = new cps.ListLastRequest();
                }else if(node.request == "CLEAR"){
                    request = new cps.ListLastRequest('clear');
                }else if(node.request == "REINDEX"){
                    request = new cps.ListLastRequest('reindex');
                }


                if(request != null){
                    conn.sendRequest(request, function (err, resp) {
                       if (err){
                            msg.payload = err;
                            node.send(msg);
                        }else{
                            msg.payload = resp;
                            node.send(msg);
                        }
                    });
                }
                // conn.close();
            }catch(e){
                console.log('e');
            }
        });

        this.on("close", function() {
            // conn.end();
            console.log("CLOSE CALLBACK");
        });
    }
    RED.nodes.registerType("clusterpoint",ClusterpointNode);



    function ConfigNode(n) {
        RED.nodes.createNode(this,n);
        this.tcp = n.tcp;
        this.port = n.port;
        this.name = n.name;
        this.user = n.user;
        this.password = n.pass;
        this.db = n.db;
        this.account = n.account;
        this.document = n.document;
        this.documentId = n.documentId;
    }
    RED.nodes.registerType("cspDatabase", ConfigNode);
}
