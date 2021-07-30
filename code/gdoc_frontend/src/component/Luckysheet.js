import React from 'react';
import {trylock,unlock,writeunit} from "../service/documentService";
import {message} from "antd";
import ReactWebsocket from "../util/websocket";
import Websocket from 'react-websocket';
import {warnAboutFunctionChild} from "react-scrollbar/src/js/utils";
class Luckysheet extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            filepath:props.filepath,
            data:props.files,
            username: localStorage.getItem("username"),
            lock:false
        };
        //console.log(this.props.files)
    }

    lock =(r,c)=>{
        let user = localStorage.getItem("username");
        let param = {username:user,owner:user,filepath:this.state.filepath,row:r, column:c};
        //return true;
        trylock(param, (data) => {
            if(data.status!==200){
                console.log(data);
                message.warning("有其它用户正在编辑此单元格哦！");
                const luckysheet = window.luckysheet;
                luckysheet.exitEditMode(r,c);
                return false;
            }
            else return true;
            }
        );

    };
    handleData(data) {
        let result = JSON.parse(data);
        console.log(result);
        const luckysheet = window.luckysheet;
        if(result.isImage===false)
            luckysheet.setCellValue(result.row,result.column,result.content);
        else luckysheet.insertImage(result.content,{rowIndex:result.row,colIndex:result.col})
    }

    write =(r,c)=>{
        console.log(r,c);
        const luckysheet = window.luckysheet;
        let data = luckysheet.getAllSheets();
        console.log(data);
        let newValue = luckysheet.getCellValue(r,c);
        if(newValue===null) newValue = "";
        let user = localStorage.getItem("username");
        let param = {username:user,owner:user,filepath:this.state.filepath,row:r, column:c,content:newValue,isImage:false};
        this.refWebSocket.sendMessage(JSON.stringify(param));

        writeunit(param, (data) => {
                if(data.status!==200){
                    message.error(data.msg);
                }
                else {
                    message.success("已自动保存");
                    let param = {username:user,owner:user,filepath:this.state.filepath,row:r, column:c};
                    unlock(param,()=>{});
                }

            }
        )
    };

    insertImage = (r,c,src)=>{
        const luckysheet = window.luckysheet;
        let newValue = luckysheet.getCellValue(r,c);
        if(newValue===null) newValue = "";
        let user = localStorage.getItem("username");
        let param = {username:user,owner:user,filepath:this.state.filepath,row:r, column:c,content:src,isImage:true};
        this.refWebSocket.sendMessage(JSON.stringify(param));
    };

    componentDidMount() {
        const luckysheet = window.luckysheet;
        let that = this;
        var autoSave;
        let apiUrl = "http://localhost:8080/read?username="+this.state.username+"&owner="+this.state.username+"&filepath="+this.state.filepath;
        console.log(apiUrl);
        luckysheet.create({
            container: "luckysheet",
            plugins:['chart'],
            title:this.state.filepath,
            data: this.state.data,
            //allowUpdate: true,
            //loadUrl:"http://localhost:11551/get",
            //updateUrl: "http://localhost:11551?name=ZYW",
            userInfo:'<i style="font-size:30px;color:#6d8b69;padding-left:5px" class="fa fa-user-circle" aria-hidden="true"></i>',
            myFolderUrl:'http://localhost:3000/',
            functionButton:
                '<button id="" class="btn btn-primary" style="padding:3px 6px;font-size: 12px;margin-right: 5px;background-color:#6d8b69;">保存</button> ',
            hook: {
                cellEditBefore: function (range) {
                    let r = range[0].row[0];
                    let c = range[0].column[0];
                    let res = that.lock(r, c);
                    console.log(res);
                    return res;
                },
                cellUpdated:function (r,c,oldValue,newValue,isRefresh) {
                    console.log(oldValue,newValue);
                    if(newValue==null||oldValue==null||newValue.v!==oldValue.v)
                        // if(newValue.v!==oldValue.v)
                        that.write(r,c);
                },
                cellDragStop:function (cell,position,sheet,ctx,event) {
                    console.log(sheet);
                    let keys = Object.keys(sheet.images);
                    if(keys.length!==0)
                        that.insertImage(position.r,position.c,sheet.images[keys[0]].src)
                }
            }
            });
    }
    render() {
        const luckyCss = {
            margin: '0px',
            padding: '0px',
            position: 'absolute',
            width: '100%',
            height: '100%',
            left: '0px',
            top: '0px'
        };
    return (
            <div
            id="luckysheet"
            style={luckyCss}
            >
                <Websocket url={'ws://127.0.0.1:11551?name='+this.state.username}
                           onMessage={this.handleData.bind(this)}
                           ref={Websocket => {
                               this.refWebSocket = Websocket;
                           }}/>
            </div>
        )
    }
}

export default Luckysheet