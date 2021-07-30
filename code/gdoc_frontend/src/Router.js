import React from 'react';
import { Router, Route, Switch, Redirect} from 'react-router-dom';
import PrivateRoute from './PrivateRoute'
import LoginView from './view/LoginView';
import HomeView from "./view/HomeView";
import FileView from "./view/FileView";
import {history} from "./util/history";
import RegisterView from "./view/RegisterView";
class BasicRoute extends React.Component{

    constructor(props) {
        super(props);

        history.listen((location, action) => {
            // clear alert on location change
            console.log(location,action);
        });
    }

    render(){
        return(
            <Router history={history}>
                <Switch>
                    <Route exact path="/" component={HomeView} />
                    <Route exact path="/login" component={LoginView} />
                    <Route exact path="/register" component={RegisterView}/>
                    <Route exact path="/file" component={FileView}/>
                    <Redirect from="/*" to="/" />
                </Switch>

            </Router>
        )
    }
}

export default BasicRoute;
