package gdoc.utils.message;

public enum MsgCode {
    SUCCESS(MsgUtil.SUCCESS, MsgUtil.SUCCESS_MSG),
    ERROR(MsgUtil.ERROR,MsgUtil.ERROR_MSG),
    LOGIN_USER_ERROR(MsgUtil.LOGIN_USER_ERROR,MsgUtil.LOGIN_USER_ERROR_MSG),
    NOT_LOGGED_IN_ERROR(MsgUtil.NOT_LOGGED_IN_ERROR,MsgUtil.NOT_LOGGED_IN_ERROR_MSG),
    USER_BAN(MsgUtil.USER_BAN,MsgUtil.USER_BAN_MSG),
    USER_NOT_EXIST(MsgUtil.USER_NOT_EXIST,MsgUtil.USER_NOT_EXIST_MSG),
    IS_ADMIN(MsgUtil.IS_ADMIN,MsgUtil.IS_ADMIN_MSG),
    IS_ADMIN2(MsgUtil.IS_ADMIN,MsgUtil.IS_ADMIN2_MSG),
    USER_EXIST(MsgUtil.ERROR,MsgUtil.USER_EXIST_MSG);

    private int status;
    private String msgContent;

    public int getStatus() {
        return status;
    }

    public String getMsg() {
        return msgContent;
    }

    private MsgCode(int status, String msg) {
        this.status = status;
        this.msgContent = msg;
    }
}
