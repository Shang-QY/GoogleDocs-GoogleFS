package gdoc.utils;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

public class FileUtils {
    public String post(String url,String body) {
        System.out.println(url);
        System.out.println(body);
        try {
            HttpResponse<String> httpResponse = Unirest.post(url)
                    .header("Content-Type", "text/plain")
                    .body(body)
                    .asString();
            return httpResponse.getBody();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return "false";
    }

   public boolean methodlock(String methodname,String username){
        String url = "http://localhost:8081/tryredislock?username="+username+"&key="+methodname+"&expireTime=5000";
        try {
            HttpResponse<String> httpResponse = Unirest.post(url)
                    .asString();
            String res = httpResponse.getBody();
            if(res.compareTo("true") == 0)
                return true;
            else
                return false;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }

    public boolean methodunlock(String methodname,String username){
        String url = "http://localhost:8081/releaseredislock?username="+username+"&key="+methodname;
        try {
            HttpResponse<String> httpResponse = Unirest.post(url)
                    .asString();
            String res = httpResponse.getBody();
            if(res.compareTo("true") == 0)
                return true;
            else
                return false;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }

    public boolean ifexist(String filepath){
        String url = "http://localhost:1314/read?path="+filepath+"&offset=0&size=0";
        String res = post(url,"");
        if(res.compareTo("path "+filepath+" not found") == 0)
            return false;
        else
            return true;
    }


}
