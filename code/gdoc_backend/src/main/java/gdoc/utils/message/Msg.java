package gdoc.utils.message;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class Msg<T> {

    private int status;

    private String msg;

    private T data;

    public Msg(MsgCode msg, T data){
        this.status = msg.getStatus();
        this.msg = msg.getMsg();
        this.data = data;
    }

    public Msg(MsgCode msg, String extra, T data){
        this.status = msg.getStatus();
        this.msg = extra;
        this.data = data;
    }

    public Msg(MsgCode msg){
        this.status = msg.getStatus();
        this.msg = msg.getMsg();
        this.data = null;
    }

    public Msg(MsgCode msg, String extra){
        this.status = msg.getStatus();
        this.msg = extra;
        this.data = null;
    }

    public Msg(int status, String extra, T data){
        this.status = status;
        this.msg = extra;
        this.data = data;
    }

    public Msg(int status, String extra){
        this.status = status;
        this.msg = extra;
        this.data = null;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
