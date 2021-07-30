import React from "react";
import {Button, Col, Input, Modal, Row} from "antd";
import Text from "antd/es/typography/Text";

const { TextArea } = Input;
export class ShareForm extends React.Component{

    constructor(props) {
        super(props);
        this.state = {
            filepath:this.props.filepath,
        };
    }

    changeValue = (e) => {
        console.log(e.target.id);
        console.log(e.target.value);
        this.setState({username:e.target.value});
        console.log(this.state);
    };

    submit = () => {
        this.props.submit(this.state);
        this.props.move();
    };

    render() {
        return (
            <Modal
                title={"文件分享 "+this.props.filepath}
                centered
                visible={true}
                footer={null}
                onCancel={this.props.move}
            >
                <Row>
                    <Col span={9} offset={2}>
                        <Text className="modalLabel" style={{fontSize:17}}>输入分享对象用户名：</Text>
                    </Col>
                    <Col span={11}>
                        <Input defaultValue={""} onChange={this.changeValue} id={"1"}/>
                    </Col>
                </Row>
                <br/>
                <Button type="primary" size="large" style={{marginLeft:"180px",marginTop:"20px"}} onClick={this.submit}>
                    {this.props.buttonText}
                </Button>
            </Modal>
        )
    }
}
