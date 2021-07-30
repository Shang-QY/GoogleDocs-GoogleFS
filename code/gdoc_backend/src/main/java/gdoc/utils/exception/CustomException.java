package gdoc.utils.exception;

/**
 * 自定义异常(CustomException)
 */
public class CustomException extends RuntimeException {

    public CustomException(String msg){
        super(msg);
    }

    public CustomException() {
        super();
    }
}
