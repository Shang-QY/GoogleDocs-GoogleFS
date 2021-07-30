import {message} from 'antd';
import axios from 'axios';
let postRequest_v2 = (url, data, callback) => {
    let formData = new FormData();

    for (let p in data){
        if(data.hasOwnProperty(p))
            formData.append(p, data[p]);
    }

    let opts = {
        method: "POST",
        body: formData,
        credentials: "include"
    };

    fetch(url,opts)
        .then((response) => {
            return response.json()
        })
        .then((data) => {
            console.log(data);
            callback(data);
        })
        .catch((error) => {
           console.log(error);
        });
};

async function fetchAsync (url,opts) {
    let response = await fetch(url,opts);
    let data = await response.json();
    return data;
}

function parseQuery(url, query) {
    if (query) {
        let paramsArray = [];
        Object.keys(query).forEach(key => paramsArray.push(key + '=' + query[key]))
        if (url.search(/\?/) === -1) {
            url += '?' + paramsArray.join('&')
        } else {
            url += '&' + paramsArray.join('&')
        }
        return url;
    } else return url;
};
let postRequest = (url, json, callback) => {

    let opts = {
        method: "POST",
        body: JSON.stringify(json),
        headers: {
            'Content-Type': 'application/json'
        },
        credentials: "include",
        //async:false,
    };

    fetch(url,opts)
        // .then(res => res.text())
        // .then(text => console.log(text))
        .then((response) => {
            return response.json()
        })
        .then((data) => {
            //console.log(data);
            callback(data);
        })
        .catch((error) => {
            console.log(error);
        });

};

let postRequest_query= async (url,query,callback)=>{
     url = parseQuery(url,query);
    postRequest(url,null,callback);

};
export {postRequest,postRequest_v2,postRequest_query};