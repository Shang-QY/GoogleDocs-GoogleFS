import { List, message, Avatar, Spin, Pagination } from 'antd';
import React from "react";
import {UserOutlined} from "@ant-design/icons";
import InfiniteScroll from 'react-infinite-scroller';
import ScrollArea from 'react-scrollbar';
export default class LogList extends React.Component{

    constructor(props) {
        super(props);
        this.state={
            logs:[]
        }
    }


    render() {
        return(
            <List
                dataSource={this.props.logs}
                pagination={{pageSize:5}}
                size="small"
                renderItem={item => (
                    <List.Item key={item.id}>
                        <List.Item.Meta
                            avatar={
                                <Avatar style={{ backgroundColor: '#6d8b69' }} icon={<UserOutlined />} />
                            }
                            title={<a >{item}</a>}
                            //description={item.email}
                        />
                    </List.Item>
                )}
            />
          );
    }
}