package gdoc.websocket;

import gdoc.utils.HttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MyWebsocketServer extends org.java_websocket.server.WebSocketServer {

    Map<WebSocket,String> connMap = new ConcurrentHashMap<>();

    public MyWebsocketServer(int port){
        super(new InetSocketAddress(port));
    }

    int n = 0;
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String resource = conn.getResourceDescriptor();
        String name = HttpUtils.getParameter(resource, "name");
        if(name == null) name = "" + n++;
        connMap.put(conn,name);
        System.out.println(name+" 连接 Websocket, 总连接数 = "+connMap.size());

    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String name = connMap.remove(conn);
        System.out.println(name+" 断开 websocket");
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("send: "+message);
        connMap.forEach((socket,n) -> {
                    if(conn == socket) return;
                    socket.send(message);
                });
    }

    @Override
    public void onError(WebSocket webSocket, Exception e) {

    }

    @Override
    public void onStart() {
        System.out.println("ws start http://127.0.0.1:"+getAddress().getPort());
    }
}
