package gdoc.serviceimpl;

import gdoc.entity.DeleteInfo;
import gdoc.repository.DeleteRepository;
import gdoc.service.FileService;
import gdoc.utils.FileUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

//import org.apache.zookeeper.KeeperException;
//import org.apache.zookeeper.WatchedEvent;
//import org.apache.zookeeper.ZooKeeper;
//import org.apache.zookeeper.Watcher;

@Service
public class FileServiceImpl implements FileService {

    String baseurl = "http://localhost:1314/";

    FileUtils fileUtils = new FileUtils();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

//    ZooKeeper zk = new ZooKeeper(
//            "139.224.113.116:2181",
//            20000,
//            watchedEvent -> {
//                // 发生变更的节点路径
//                String path = watchedEvent.getPath();
//                System.out.println("path:" + path);
//
//                // 通知状态
//                Watcher.Event.KeeperState state = watchedEvent.getState();
//                System.out.println("KeeperState:" + state);
//
//                // 事件类型
//                Watcher.Event.EventType type = watchedEvent.getType();
//                System.out.println("EventType:" + type);
//                List<String> children = zookeeper.getChildren(path, false);
//                baseurl = "http://localhost:"+children.get(0)+"/"
//            }
//    );

    @Autowired
    DeleteRepository deleteRepository;

    public FileServiceImpl() throws IOException {
    }


    @Override
    public String create(String username, String filepath){
//        while(!methodlock("create",username));
        String url = baseurl+"create";
        String res = fileUtils.post(url,filepath);
        fileUtils.post(url,filepath+"_log");
        String time = df.format(new Date());// new Date()为获取当前系统时间
        fileUtils.post(baseurl+"append?path="+filepath+"_log","Time:"+time+" User:"+username+" create it"+'\n');
//        methodunlock("create",username);
        return res;
    }


    @Override
    public String share(String username,String shareusername, String filepath){

        if(!fileUtils.ifexist(filepath))
            return "file "+filepath+" don't exist";

        String url = baseurl+"create";
        String[] names = filepath.split("/");
        String basefilename = names[names.length-1];
        String filename = basefilename;
        if(fileUtils.ifexist("/"+shareusername+"/"+"share_"+filename))
        {
            for (int i=1;;i++)
            {
                filename = basefilename+"("+i+")";
                if(!fileUtils.ifexist("/"+shareusername+"/"+"share_"+filename))
                {
                    break;
                }
            }
        }
        String newpath = "/"+shareusername+"/"+"share_"+filename;
        String res = fileUtils.post(url,newpath);
        fileUtils.post(baseurl+"append?path="+newpath,filepath);
        return res;
    }

    @Override
    public String mkdir(String username,String dirpath){
//        while (!methodlock("mkdir",username));
        String url = baseurl+"mkdir";
        String res = fileUtils.post(url,dirpath);
//        methodunlock("mkdir",username);
        return res;
    }

    @Override
    public String dirinfo(String username,String dirpath){
        String url = baseurl+"list";
        String res = fileUtils.post(url,dirpath);
        return res;
    }

    @Override
    public String fileinfo(String username, String filepath){

        if(filepath.contains("share_"))
        {
            String url = baseurl + "read?path="+filepath+"&offset=0&size="+9999;
            String res = fileUtils.post(url,"");
            return fileinfo(username,res);
        }

        String url = baseurl+"getFileInfo";
        String res = fileUtils.post(url,filepath);
        return res;
    }

    @Override
    public String[] loginfo(String username, String owner, String filepath){

        if(filepath.contains("share_"))
        {
            String url = baseurl + "read?path="+filepath+"&offset=0&size="+9999;
            String res = fileUtils.post(url,"");
            return loginfo(username,owner,res);
        }

        return read(username,owner,filepath+"_log");
    }

    @Override
    public String rename(String username,String oldpath,String newpath){
        if (!fileUtils.ifexist(oldpath))
        {
            return "file "+oldpath+" don't exist";
        }
        if (fileUtils.ifexist(newpath))
        {
            return "file "+newpath+" already exist";
        }
        String url = baseurl + "rename";
        String body = oldpath+":"+newpath;
        fileUtils.post(url,body);
        body = oldpath+"_log"+":"+newpath+"_log";
        return fileUtils.post(url,body);
    }

    @Override
    public String delete(String username,String filepath){
        String url = baseurl + "rename";
        if(!fileUtils.ifexist("/delete"))
            mkdir(username,"/delete");
        if(!fileUtils.ifexist("/delete/" + username))
            mkdir(username,"/delete/" + username);
        if(!fileUtils.ifexist(filepath))
            return "path "+filepath+" don't exist";
        String[] names = filepath.split("/");
        String basefilename = names[names.length-1];
        String filename = basefilename;
        if(fileUtils.ifexist("/delete/"+username+"/"+filename))
        {
            for (int i=1;;i++)
            {
                filename = basefilename+"("+i+")";
                if(!fileUtils.ifexist("/delete/"+username+"/"+filename))
                {
                    break;
                }
            }
        }
        String body = filepath+":"+"/delete/"+username+"/"+filename;
        fileUtils.post(url,body);
        body = filepath+"_log"+":"+"/delete/"+username+"/"+filename+"_log";
        fileUtils.post(url,body);
        DeleteInfo deleteInfo;
        if(deleteRepository.findbyname(username+"-"+filename).isEmpty())
            deleteInfo = new DeleteInfo();
        else
            deleteInfo = deleteRepository.findbyname(username+"-"+filename).get(0);
        deleteInfo.setFilename(username+"-"+filename);
        deleteInfo.setPath(filepath);
        deleteRepository.save(deleteInfo);
        return "success";
    }


    @Override
    public String recover(String username, String filename){

        if(!fileUtils.ifexist("/delete/"+username+"/"+filename))
            return "file "+filename+" don't exist";
        String url = baseurl + "rename";
        DeleteInfo deleteInfo = deleteRepository.findbyname(username+"-"+filename).get(0);
        String basepath = deleteInfo.getPath();
        String path = basepath;
        int i = 0;
        if(fileUtils.ifexist(path))
        {
            for(i = 1 ;;i++)
            {
                path = basepath+"("+i+")";
                if(!fileUtils.ifexist(path))
                    break;
            }
        }

        String body = "/delete/"+username+"/"+filename+":"+ path;
        String res = fileUtils.post(url,body);
        body = "/delete/"+username+"/"+filename+"_log"+":"+ path+"_log";
        fileUtils.post(url,body);
        if(i != 0)
            return "success: rename to "+ path;
        return res;
    }


    @Override
    public String[] read(String username, String owner, String filepath){

        if(filepath.contains("share_"))
        {
            String url = baseurl + "read?path="+filepath+"&offset=0&size="+9999;
            String res = fileUtils.post(url,"");
            return read(username,owner,res);
        }


        if(!fileUtils.ifexist(filepath))
            return new String[]{"file "+filepath+" don't exist"};

        String url = baseurl + "getFileInfo";
        String res = fileUtils.post(url,filepath);
        JSONObject jsonObject = new JSONObject(res);
        int length = jsonObject.getInt("Length");
        url = baseurl + "read?path="+filepath+"&offset=0&size="+length;
        res = fileUtils.post(url,"");
        String[] ret = res.split("\n");
        return ret;
    }


    @Override
    public String write(String username,String owner,String filepath, int row,int column,String content) {

        if(!fileUtils.ifexist(filepath))
            return "file "+filepath+" don't exist";

        if(filepath.contains("share_"))
        {
            String url = baseurl + "read?path="+filepath+"&offset=0&size="+9999;
            String res = fileUtils.post(url,"");
            return write(username,owner,res,row,column,content);
        }

        String[] buffer = read(username,owner,filepath);
        boolean flag = false;
        int index = 0;
        for(;index<buffer.length;index++)
        {
            if(buffer[index].contains(row+","+column))
            {
                if(content.compareTo("")==0)
                      buffer[index]="";
                else
                    buffer[index] = row+","+column+":"+content;
                flag = true;
                break;
            }
        }
        String res;
        int length = 0;
        if(flag)
        {
            String body = "";
            for(int i=0;i<buffer.length;i++)
            {
                if(buffer[i].compareTo("")==0)
                    continue;
                body = body + buffer[i] + '\n';
            }
            res = fileUtils.post(baseurl+"writeAndCut?path="+filepath+"&offset=0",body);
        }

        else {
            res = fileUtils.post(baseurl+"append?path="+filepath,row+","+column+":"+content+'\n');
        }
        String time = df.format(new Date());// new Date()为获取当前系统时间
        fileUtils.post(baseurl+"append?path="+filepath+"_log","Time:"+time+" User:"+username+" Location:("+row+","+column+") Content:"+content+'\n');
        return res;
    }

    @Override
    public String trylock(String username, String owner,String filepath,int row,int column)
    {

        if(filepath.contains("share_"))
        {
            String url = baseurl + "read?path="+filepath+"&offset=0&size="+9999;
            String res = fileUtils.post(url,"");
            return trylock(username,owner,res,row,column);
        }

        if(!fileUtils.ifexist("/lock"))
            mkdir(username,"/lock");

        String filename = filepath.replaceAll("/","-").replaceFirst("-","");
        filepath = "/lock/"+filename;
        if(!fileUtils.ifexist(filepath))
            create(username,filepath);

        String[] buffer = read(username,owner,filepath);
        int index = 0;
        for(;index<buffer.length;index++)
        {
            if(buffer[index].contains(row+","+column+":"))
            {
                if(!buffer[index].contains(username))
                    return "fail";
                else
                    return "success";
            }
        }
        return fileUtils.post(baseurl+"append?path="+filepath,row+","+column+":"+username+'\n');
    }

    @Override
    public String unlock(String username, String owner,String filepath,int row,int column){

        if(filepath.contains("share_"))
        {
            String url = baseurl + "read?path="+filepath+"&offset=0&size="+9999;
            String res = fileUtils.post(url,"");
            return unlock(username,owner,res,row,column);
        }

        String filename = filepath.replaceAll("/","-").replaceFirst("-","");
        filepath = "/lock/"+filename;
        if(!fileUtils.ifexist(filepath))
            return "file "+filepath+" don't exist";
        String[] buffer = read(username,owner,filepath);
        String body = "";
        boolean flag = false;
        for(int i=0;i<buffer.length;i++)
        {
            if(!buffer[i].contains(row+","+column+":"+username))
                {
                    body = body + buffer[i] + '\n';
                }
            else flag = true;
        }
        if (!flag)
            return "no lock";
        String res = fileUtils.post(baseurl+"writeAndCut?path="+filepath+"&offset=0",body);
        return res;
    }

    @Override
    public String[] getlockinfo(String username, String owner, String filepath) {

        if(filepath.contains("share_"))
        {
            String url = baseurl + "read?path="+filepath+"&offset=0&size="+9999;
            String res = fileUtils.post(url,"");
            return getlockinfo(username,owner,res);
        }

        String filename = filepath.replaceAll("/","-").replaceFirst("-","");
        filepath = "/lock/"+filename;
        if (!fileUtils.ifexist(filepath))
            return new String[]{""};
        return read(username, owner, filepath);
    }
}
