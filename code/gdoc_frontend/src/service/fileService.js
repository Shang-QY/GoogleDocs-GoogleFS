import {apiUrl} from "../util/constants";
import {postRequest_query} from "../util/ajax";

export const create = (data,callback) => {
    const url = `${apiUrl}/create`;
    postRequest_query(url, data, callback);
};

export const mkdir = (data,callback) => {
    const url = `${apiUrl}/mkdir`;
    postRequest_query(url, data, callback);
};

export const readdir = (data,callback) => {
    const url = `${apiUrl}/dirinfo`;
    postRequest_query(url, data, callback);
};

export const deletefile = (data,callback) => {
    const url = `${apiUrl}/delete`;
    postRequest_query(url, data, callback);
};

export const recoverfile = (data,callback) => {
    const url = `${apiUrl}/recover`;
    postRequest_query(url, data, callback);
};

export const renamefile = (data,callback) => {
    const url = `${apiUrl}/rename`;
    postRequest_query(url, data, callback);
};

export const sharefile = (data,callback) => {
    const url = `${apiUrl}/share`;
    postRequest_query(url, data, callback);
};

export const read = (data,callback) => {
    const url = `${apiUrl}/read`;
    postRequest_query(url, data, callback);
};

export const getfileinfo = (data,callback) => {
    const url = `${apiUrl}/fileinfo`;
    postRequest_query(url, data, callback);
};

export const getloginfo = (data,callback) => {
    const url = `${apiUrl}/loginfo`;
    postRequest_query(url, data, callback);
};
