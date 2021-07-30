import React from 'react';
import {Route, Redirect} from 'react-router-dom'
import * as userService from "./service/userService"
import {message} from "antd";

export default class PrivateRoute extends React.Component{
    constructor(props) {
        super(props);
        this.state = {
            isAuthed: false,
            hasAuthed: false,
        };
    }

    checkAuth = (data) => {
        console.log(data);
        if (data.status >= 0) {
            this.setState({isAuthed: true, hasAuthed: true});
        } else {
            message.error(data.msg);
            localStorage.removeItem('user');
            this.setState({isAuthed: false, hasAuthed: true});
        }
    };


    componentDidMount() {
        userService.checkSession(this.checkAuth);
    }


    render() {

        const {component: Component, path="/",exact=false,strict=false} = this.props;

        console.log(this.state.isAuthed);

        if (!this.state.hasAuthed) {
            return null;
        }

        return <Route path={path} exact={exact} strict={strict} render={props => (
            this.state.isAuthed ? (

                <Component {...props}/>
            ) : (
                <Redirect to={{
                    pathname: '/login',
                    state: {from: props.location}
                }}/>
            )
        )}/>
    }
}

