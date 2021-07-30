import React from 'react';
import LoginForm from '../component/LoginForm';
import {withRouter} from "react-router-dom";


class LoginView extends React.Component{


    render(){
        return(
            <div className="login-page">
                <div className="login-container">
                    <div className="login-box">
                        <h1 className="page-title">Welcome to gDocs</h1>
                        <div className="login-content">
                            <LoginForm />
                        </div>
                    </div>
                </div>
            </div>
        );

    }
}

export default withRouter(LoginView);