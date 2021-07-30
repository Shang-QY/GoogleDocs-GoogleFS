import React from 'react';
import { Layout, Menu,Breadcrumb,Avatar,Row,Col,Dropdown } from 'antd';
import '../css/home.css';
import {
    UserOutlined,
    PoweroffOutlined, ProfileTwoTone,
} from '@ant-design/icons';
import {withRouter} from "react-router-dom";
import SideBar from "../component/SideBar";
import FileList from "../component/FileList";
import {MsgType} from "../util/constants";
import PubSub from "pubsub-js";
import {logout} from "../service/userService"

const { Header, Content, Footer, Sider } = Layout;


class HomeView extends React.Component{

    constructor(props) {
        super(props);
        this.state = {
            filepath:'/'+localStorage.getItem('username'),
        };
        PubSub.subscribe(MsgType.CHANGE_DIR, (msg, data) => {
            this.setState({filepath:data.filepath});
            console.log(this.state.filepath);
            this.forceUpdate();
        });
    }

    render() {
        const menu = (
            <Menu>
                <Menu.Item icon={<PoweroffOutlined/>} onClick={()=>{logout();;}}>
                    <a>退出登陆</a>
                </Menu.Item>
            </Menu>
        );
        return(
        <Layout>
            <Sider
            style={{
                overflow: 'auto',
                height: '100vh',
                position: 'fixed',
                left: 0,
            }}
            theme = "light"
            >
                <SideBar filepath={this.state.filepath}/>
            </Sider>
            <Layout className="site-layout" style={{marginLeft: 200}}>
                <Header className="site-layout-background" style={{padding: 1,paddingRight:5}}>
                    <Row>
                        <Col xs={21}></Col>
                        <Col xs={2}> Hi! {localStorage.getItem("username")}</Col>
                        <Col xs={1}>
                            <Dropdown overlay={menu}>
                            <Avatar size={40} style={{ backgroundColor: '#6d8b69' }} icon={<UserOutlined />} />
                            </Dropdown>
                        </Col>
                    </Row>
                </Header>
                <Content style={{margin: '0px 16px 0', overflow: 'initial',minHeight:'900px'}}>
                    <div style={{margin:'20px 2px 20px',color:'#6d8b69'}}>
                        {this.state.filepath}
                    </div>
                    <div className="site-layout-background" style={{padding: 24, textAlign: 'center',minHeight:'900px'}}>
                        <FileList filepath={this.state.filepath}/>
                    </div>
                </Content>
                <Footer style={{textAlign: 'center'}}>All Rights are Reserved</Footer>
            </Layout>
        </Layout>)
    }
}
export default withRouter(HomeView);