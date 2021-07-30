import {Table, Tag, Space, Menu, Dropdown, Button, Empty, message, Modal} from 'antd';
import React from "react";
import {ProfileTwoTone,FolderTwoTone,EditOutlined,DeleteOutlined,MoreOutlined,ShareAltOutlined} from "@ant-design/icons";
import {readdir, deletefile, recoverfile, renamefile, sharefile, getloginfo} from "../service/fileService";
import PubSub from "pubsub-js";
import {MsgType} from "../util/constants";
import {history} from "../util/history";
import {CreateForm} from "./CreateForm";
import {ShareForm} from "./ShareForm";
import LogList from "./LogList";

class FileList extends React.Component{
    constructor(props) {
        super(props);
        this.state = {
            files:[],
            renameModalVisible:false,
            shareModalVisible:false,
            logModalVisible:false,
            logs:[],
            curFile:""
        };
        PubSub.subscribe(MsgType.ADD_FILE, (msg, data) => {
            this.updateFile(this.props.filepath);
        });

    }

    componentDidMount(){
        this.updateFile(this.props.filepath);
    }

    componentWillReceiveProps(nextProps){
        console.log(nextProps.filepath);
        this.updateFile(nextProps.filepath);
        this.forceUpdate();
    }

    updateFile = (filepath) =>{
        let user = localStorage.getItem("username");
        let param = {username:user,dirpath:filepath};
        readdir(param, (data) => {
                let files = JSON.parse(data.data);
                if( files != null){
                let arr = files.filter(function (item) {
                    return item.Name.indexOf("__del__") === -1 && item.Name.indexOf("_log") === -1
                });
                    console.log(arr);
                    this.setState({files: arr});
                }
                else this.setState({files: []})

            }
        )
    };

    changeDir = (record) =>{
        if(record.IsDir){
            PubSub.publish(MsgType.CHANGE_DIR, {filepath:this.props.filepath+'/'+record.Name});
        }
        else{
            history.push('/file',{filepath:this.props.filepath+'/'+record.Name})
        }
    };

    deleteFile = (record) =>{
        let user = localStorage.getItem("username");
        let param = {username:user,filepath:this.props.filepath+'/'+record.Name};
        deletefile(param, (data) => {
            if(data.status === 200) {
                console.log(data);
                message.success("删除"+record.Name+"成功");
                this.updateFile(this.props.filepath);
            }
            else{
                message.error(data.msg);
            }
        })
    };

    recoverFile = (record) =>{
        let user = localStorage.getItem("username");
        let param = {username:user,filename:record.Name};
        recoverfile(param, (data) => {
            if(data.status === 200) {
                console.log(data);
                message.success("恢复"+record.Name+"成功");
                this.updateFile(this.props.filepath);
            }
            else{
                message.error(data.msg);
            }
        })
    };

    renameFile = (data) =>{
        let user = localStorage.getItem("username");
        let param = {username:user,oldpath:this.props.filepath+'/'+this.state.curFile,newpath:"/"+user+"/"+data.filepath};
        console.log(param);
        renamefile(param, (data) => {
            if(data.status === 200) {
                console.log(data);
                message.success("重命名成功");
                this.updateFile(this.props.filepath);
            }
            else{
                message.error(data.msg);
            }
        })
    };
    renderRenameModal = () => {
        if(this.state.renameModalVisible === true)
            return (<CreateForm filepath = {this.props.filepath+'/'+this.state.curFile}
                                move={this.moveOutRenameModal}
                                submit={this.renameFile}
                                buttonText="确认重命名"
            />);
        else return null;
    };

    moveOutRenameModal = () => {
        this.setState({renameModalVisible:false})
    };

    editFile = (record)=>{
        history.push('/file',{filepath:this.props.filepath+'/'+record.Name})
    };

    shareFile = (data) =>{
        let user = localStorage.getItem("username");
        let param = {username:user,shareusername:data.username,filepath:this.props.filepath+'/'+this.state.curFile};
        sharefile(param, (data) => {
            if(data.status === 200) {
                console.log(data);
                message.success("分享"+this.state.curFile+"成功");
                this.updateFile(this.props.filepath);
            }
            else{
                message.error(data.msg);
            }
        })
    };

    renderShareModal = () => {
        if(this.state.shareModalVisible === true)
            return (<ShareForm filepath = {this.props.filepath+'/'+this.state.curFile}
                                move={this.moveOutShareModal}
                                submit={this.shareFile}
                                buttonText="确认分享"
            />);
        else return null;
    };

    moveOutShareModal = () => {
        this.setState({shareModalVisible:false})
    };

    isShareAble = (record)=>{
        if(record.IsDir){
            message.warning("不能分享文件夹哦！")
        }
        else this.setState({shareModalVisible:true,curFile:record.Name})
    };

    getLogInfo = (record) =>{
        let user = localStorage.getItem("username");
        let param = {username:user,owner:user,filepath:this.props.filepath+'/'+record.Name};
        getloginfo(param, (data) => {
            if(data.status === 200) {
                console.log(data);
                this.setState({logs:data.data,logModalVisible:true,curFile:record.Name});
                message.success("获取"+this.state.curFile+"日志信息成功");
            }
            else{
                message.error(data.msg);
            }
        })
    };

    renderLogModal = () => {
        if(this.state.logModalVisible === true)
            return (
                <Modal title={this.state.curFile+"的日志信息"} visible={this.state.logModalVisible} onOk={this.moveOutLogModal} onCancel={this.moveOutLogModal}>
                    <LogList logs={this.state.logs}/>
                </Modal>);
        else return null;
    };

    moveOutLogModal = () => {
        this.setState({logModalVisible:false})
    };

   render(){
        const {filepath}=this.props;
        const menu1 = (record)=>(
            <Menu>
                <Menu.Item key={'1'} onClick={()=>{this.setState({renameModalVisible:true,curFile:record.Name});}}>
                    <a>重命名</a>
                </Menu.Item>
                <Menu.Item key={'2'} onClick={()=>{this.getLogInfo(record)}}>
                    <a>日志信息</a>
                </Menu.Item>
            </Menu>
        );
        const menu2 = (record)=> (
            <Menu>
                <Menu.Item key={'1'} onClick={()=>{this.recoverFile(record)}}>
                    <a>放回原处</a>
                </Menu.Item>
                <Menu.Item key={'2'} onClick={()=>{this.getLogInfo(record)}}>
                    <a>日志信息</a>
                </Menu.Item>
            </Menu>
        );
        const columns = [
            {
                title: '',
                key: 'IsDir',
                dataIndex: 'IsDir',
                render: text =>text===false?
                    <ProfileTwoTone twoToneColor="#52c41a" style={{fontSize:'20px'}}/>
                    :<FolderTwoTone style={{fontSize:'20px'}}/>
            },
            {
                title: '文件名称',
                dataIndex: 'Name',
                key: 'Name',
                render: (text,record) => <Button type="link" onClick={()=>{this.changeDir(record)}}>{text}</Button>,
            },
            {
                title: '文件大小',
                dataIndex: 'Length',
                key: 'Length',
                render: text => <div>{text} B</div>,
            },
            {
                title: '操作',
                key: 'action',
                render: (text, record) => (
                    <Space size="middle">
                        <Button type="primary" shape="circle" icon={<EditOutlined/>}
                                onClick={()=>{this.editFile(record)}}/>
                        <Button type="primary" shape="circle" icon={<DeleteOutlined/>}
                                onClick={()=>{this.deleteFile(record)}}/>
                        <Button type="primary" shape="circle" icon={<ShareAltOutlined/>}
                                onClick={()=>{this.isShareAble(record)}}/>
                        <Dropdown overlay={filepath.indexOf("delete")===-1?menu1.bind(this,record):menu2.bind(this,record)}>
                            <div>
                            <Button type="primary" shape="circle" icon={<MoreOutlined />}/>
                            </div>
                        </Dropdown>
                    </Space>
                ),
            },
        ];
        if(this.state.files.length===0)
            return(<Empty/>);
        else
            return(
                <div>
                    <Table columns={columns} dataSource={this.state.files} />
                    {this.renderRenameModal()}
                    {this.renderShareModal()}
                    {this.renderLogModal()}
                </div>
            )
    }
}
export default FileList;