import {apiUrl} from "../util/constants";
import {postRequest, postRequest_v2, postRequest_query} from "../util/ajax";
import {history} from '../util/history';
import {message} from 'antd';

export const login = (data) => {
    const url = `${apiUrl}/login`;
    const callback = (data) => {
        if(data.status === 200) {
            console.log(data);
            localStorage.clear();
            localStorage.setItem("username", data.data.username);
            let user = localStorage.getItem("username");
            console.log(user);
            history.push("/");
            message.success(data.msg);
        }
        else{
            message.error(data.msg);
        }
    };
    postRequest_query(url, data, callback);
};

export const logout = () => {
    localStorage.removeItem("username");
    history.push("/login");
    message.success("登出成功");
};

export const checkSession = (callback) => {
    const url = `${apiUrl}/checkSession`;
    postRequest(url, {}, callback);
};

export const getUser= (id, callback) => {
    console.log(id);
    const data = {id: id};
    const url = `${apiUrl}/getUser`;
    postRequest_v2(url, data, callback);
    console.log(data.userId);
};

export const register = (data) => {
    const url = `${apiUrl}/register`;
    const callback = (data) => {
        if(data.status >= 0) {
            history.push("/login");
            message.success(data.msg);
        }
        else{
            message.error(data.msg);
        }
    };

    postRequest(url, data, callback);
};