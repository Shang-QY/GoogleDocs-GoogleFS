import React from 'react';
import {withRouter} from "react-router-dom";
import Luckysheet from "../component/Luckysheet";
import {read, readdir} from "../service/fileService";
import {Empty, message} from 'antd';
class FileView extends React.Component{
    constructor(props) {
        super(props);
        this.state = {
            filepath: this.props.location.state.filepath,
            file: []

        }
    }

    componentDidMount(){
        let user = localStorage.getItem("username");
        let param = {username:user,owner:user,filepath:this.state.filepath};
        read(param, (data) => {
            console.log(data);
            if(data.status===200){
                console.log(data);
                this.setState({file: data.data})
            }

        })
    }
    render() {
        console.log(this.state.file);
        if(this.state.file.length===0) return <Empty/>;
        return( <Luckysheet filepath={this.state.filepath} files={this.state.file}/>)
    }
}
export default withRouter(FileView);