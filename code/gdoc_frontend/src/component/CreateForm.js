import React from "react";
import {Button, Col, Input, Modal, Row} from "antd";
import Text from "antd/es/typography/Text";

const { TextArea } = Input;
export class CreateForm extends React.Component{

    constructor(props) {
        super(props);
        let pos = this.props.filepath.indexOf("/");
        let str1 = this.props.filepath.substring(pos+1,this.props.filepath.length);
        console.log(str1);
        let pos2 = str1.indexOf("/");
        let str2 = str1.substring(pos2+1,str1.length);
        console.log(str2);
        this.state = {
            filepath:str2,
        };
    }

    changeValue = (e) => {
        console.log(e.target.id);
        console.log(e.target.value);
        this.setState({filepath:e.target.value});
        console.log(this.state);
    };

    submit = () => {
        this.props.submit(this.state);
        this.props.move();
    };

    render() {
        return (
            <Modal
                title={this.props.buttonText.substr(0,2)}
                centered
                visible={true}
                footer={null}
                onCancel={this.props.move}
            >
                <Row>
                    <Col span={4} offset={2}>
                        <Text className="modalLabel" style={{fontSize:17}}>文件名：</Text>
                    </Col>
                    <Col span={15}>
                        <Input
                            defaultValue={this.props.buttonText==="确认重命名"?this.state.filepath:""}
                            onChange={this.changeValue} id={"1"}/>
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
