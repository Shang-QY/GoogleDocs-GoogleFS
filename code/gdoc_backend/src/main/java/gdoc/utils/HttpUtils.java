package gdoc.utils;

public class HttpUtils {

    /**
     * 获取url参数
     * @param path
     * @param key
     * @return
     */
    public static String getParameter(String path, String key) {
        char[] data = path.toCharArray();
        char[] keyArr = key.toCharArray();
        for (int i = 0; i < data.length; i++) {
            if (data[i] == '?') {
                for (int j = i + 1; j < data.length; j++) {
                    if(data[j] == keyArr[0]){

                        boolean flag = true;
                        for (int k = 1; k < keyArr.length; k++) {
                            if (keyArr[k] != data[j+k]) {
                                flag = false;
                                break;
                            }
                        }

                        if(flag){
                            int start = j + keyArr.length + 1;
                            int end = start;
                            while (end < data.length) {
                                if (data[end] == '&'){
                                    break;
                                }

                                end++;
                            }

                            return new String(data,start,end-start);
                        }
                    }
                }
            }
        }

        return null;
    }
}
