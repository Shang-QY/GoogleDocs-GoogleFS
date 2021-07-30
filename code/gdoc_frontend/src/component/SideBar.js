import React from 'react';
import {Row, Col, Button, Menu, Divider, Dropdown, message} from 'antd';
import {
    FileTextOutlined,
    PlusCircleOutlined,
    DeleteOutlined, ProfileTwoTone, FolderTwoTone
} from '@ant-design/icons';
import '../css/home.css';
import {create,mkdir} from "../service/fileService";
import {CreateForm} from "./CreateForm";
import {history} from "../util/history";
import {MsgType} from "../util/constants";
import PubSub from "pubsub-js";


class SideBar extends React.Component{

    constructor(props) {
        super(props);
        this.state = {
            fileModalVisible:false,
            dirModalVisible:false
        };
    }

    componentDidMount(){

    }

    renderFileModal = () => {
        if(this.state.fileModalVisible === true)
            return (<CreateForm filepath = {this.props.filepath}
                                move={this.moveOutFileModal}
                                submit={this.createFile}
                                buttonText="新建文件"
            />);
        else return null;
    };

    moveOutFileModal = () => {
        this.setState({fileModalVisible:false})
    };


    createFile = (res) => {
        let user = localStorage.getItem("username");
        let param = {username:user,filepath:this.props.filepath+'/'+res.filepath};
        const callback = (data) => {
            if(data.status === 200) {
                message.success(res.filepath+"文件创建成功");
                PubSub.publish(MsgType.ADD_FILE, data.data);
            }
            else{
                message.error(data.msg);
            }
        };
        create(param,callback);
    };
    renderDirModal = () => {
        if(this.state.dirModalVisible === true)
            return (<CreateForm
                filepath = {this.props.filepath}
                move={this.moveOutDirModal}
                submit={this.createDir}
                buttonText="新建文件夹"
            />);
        else return null;
    };

    moveOutDirModal = () => {
        this.setState({dirModalVisible:false})
    };

    createDir = (res) => {
        let user = localStorage.getItem("username")
        let param = {username:user,dirpath:this.props.filepath+'/'+res.filepath};
        const callback = (data) => {
            if(data.status === 200) {
                message.success(res.filepath+"文件夹创建成功");
                PubSub.publish(MsgType.ADD_FILE, data.data);
            }
            else{
                message.error(data.msg);
            }
        };
        mkdir(param,callback);
    };

    openMyFiles = () =>{
        PubSub.publish(MsgType.CHANGE_DIR, {filepath:'/'+localStorage.getItem('username')});
    };

    openTrash = () => {
        PubSub.publish(MsgType.CHANGE_DIR, {filepath: '/' + 'delete/' + localStorage.getItem('username')});
    };
    render() {
        const menu = (
            <Menu>
                <Menu.Item icon={<ProfileTwoTone twoToneColor="#52c41a" style={{fontSize:'20px'}}/>}
                 onClick={() => {this.setState({fileModalVisible:true})}} key='1'>
                    <a>新建文件</a>
                </Menu.Item>
                <Menu.Item icon={<FolderTwoTone style={{fontSize:'20px'}}/>}
                    onClick = {() => {this.setState({dirModalVisible:true})}} key='2'>
                    <a>新建文件夹</a>
                </Menu.Item>
            </Menu>
        );

        return(
            <React.Fragment>
                <Row gutter={[16, 12]}>
                    <Col span={24}>
                        <div className="logo"/>
                    </Col>
                    <Col span={2}/>
                    <Col span={20}>
                        <Dropdown overlay={menu}>
                            <Button type="primary" htmlType="submit" className="create-button" icon ={<PlusCircleOutlined />} block>
                                新建
                            </Button>
                        </Dropdown>
                    </Col>
                    <Col span={24}/>
                    <Col span={24}>
                        <Divider/>
                        <Menu theme="light" mode="inline" defaultSelectedKeys={['4']}>
                            <Menu.Item key="1" icon={<FileTextOutlined/>} onClick={this.openMyFiles}>
                                我的文档
                            </Menu.Item>
                            <Menu.Item key="2" icon={<DeleteOutlined/>} onClick={this.openTrash}>
                                回收站
                            </Menu.Item>
                        </Menu>
                    </Col>
                </Row>
                {this.renderFileModal()}
                {this.renderDirModal()}
            </React.Fragment>
        )
    }
}
export default SideBar;