package gdoc.service;


import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.List;

public interface FileService {

    public String create(String username, String filepath);
    public String share(String username,String shareusername, String filepath);
    public String fileinfo(String username, String filepath);
    public String[] loginfo(String username,String owner, String filepath);
    public String mkdir(String username,String dirpath);
    public String dirinfo(String username,String dirpath);
    public String rename(String username,String oldpath,String newpath);
    public String delete(String username, String filename);
    public String recover(String username, String filename);
    public String[] read(String username, String owner, String filepath);
    public String write(String username, String owner,String filepath,int row,int column,String content);
    public String trylock(String username, String owner,String filepath,int row,int column);
    public String unlock(String username, String owner,String filepath,int row,int column);
    public String[] getlockinfo(String username, String owner, String filepath);
}
