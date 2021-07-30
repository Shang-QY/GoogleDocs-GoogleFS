import {apiUrl} from "../util/constants";
import {postRequest_query} from "../util/ajax";

export const trylock = (data,callback) => {
    const url = `${apiUrl}/trylock`;
    //callback()
    postRequest_query(url, data, callback);
};

export const unlock = (data,callback) => {
    const url = `${apiUrl}/unlock`;
    postRequest_query(url, data, callback);
};

export const writeunit = (data,callback) => {
    const url = `${apiUrl}/write`;
    postRequest_query(url, data, callback);
};



