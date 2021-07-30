import React from 'react';
import Websocket from 'react-websocket';

class ReactWebsocket extends React.Component {

    handleData(data) {
        this.props.onMessage(data)
    }

    handleOpen(value){
        this.props.onOpen(value);
        // this.refWebSocket.sendMessage(value);
    }

    handleClose() {
        this.props.onClose()
    }

    sendMessage(message){
        this.refWebSocket.sendMessage(message);
    }

    render() {
        const {url,reconnect,debug} =this.props;
        return (
            <Websocket
                url={url}
                onMessage={this.handleData.bind(this)}
                onOpen={this.handleOpen.bind(this)}
                onClose={this.handleClose.bind(this)}
                reconnect={reconnect}
                debug={debug}
                ref={Websocket => {
                    this.refWebSocket = Websocket;
                }}/>
        );
    }
}

export default ReactWebsocket;