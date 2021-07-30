package gdoc.controller;

import gdoc.entity.Sheet;
import gdoc.entity.User;
import gdoc.service.FileService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import gdoc.utils.message.*;
import gdoc.utils.exception.*;

import java.util.List;

@RestController
public class FileController {

    @Autowired
    FileService fileService;

    //文件创建接口
    //传入username，filename
    //返回：
    //      成功："success"
    //      失败："path xxx not found"
    //           "file xxx already exists in dir"
    @PostMapping("/create")
    public Msg create(@RequestParam("username") String username,@RequestParam("filepath") String filepath)
    {
        String res = fileService.create(username,filepath);
        if(res.compareTo("success") == 0)
            return new Msg(HttpStatus.OK.value(),"success");
        else
            throw new CustomUnauthorizedException(res);
    }

    //文件创建接口
    //传入username，filename
    //返回：
    //      成功："success"
    //      失败："path xxx not found"
    //           "file xxx already exists in dir"
    @PostMapping("/share")
    public Msg share(@RequestParam("username") String username,@RequestParam("shareusername") String shareusername,@RequestParam("filepath") String filepath)
    {
        String res = fileService.share(username,shareusername,filepath);
        if(res.compareTo("success") == 0)
            return new Msg(HttpStatus.OK.value(),"success");
        else
            throw new CustomUnauthorizedException(res);
    }

    //目录创建接口
    //传入username，filename
    //返回：
    //      成功："success"
    //      失败："Dir xxx already exists"
    @PostMapping("/mkdir")
    public Msg mkdir(@RequestParam("username") String username,@RequestParam("dirpath") String dirpath)
    {
        String res = fileService.mkdir(username,dirpath);
        if(res.compareTo("success") == 0)
            return new Msg(HttpStatus.OK.value(),"success");
        else
            throw new CustomUnauthorizedException(res);
    }

    //目录信息接口
    //传入username，filename
    //返回：
    //      成功："success",json 对象数组字符串：
    //      "[{\"Name\":\"test\",\"IsDir\":false,\"Length\":0,\"Chunks\":0},{\"Name\":\"lock\",\"IsDir\":true,\"Length\":0,\"Chunks\":0}]"
    //      失败："path xxx not found"
    @PostMapping("/dirinfo")
    public Msg<String> dirinfo(@RequestParam("username") String username, @RequestParam("dirpath") String dirpath)
    {
        String res = fileService.dirinfo(username,dirpath);
        if(res.contains("not found"))
            throw new CustomUnauthorizedException(res);
        else
            return new Msg<String>(HttpStatus.OK.value(),"success",res);
    }

    //文件信息接口(目录或文件都可)
    //传入username，filename
    //返回：
    //      成功："success",json 对象数组字符串：
    //      "{\"IsDir\":false,\"Length\":0,\"Chunks\":0}"
    //      失败："path xxx not found"
    @PostMapping("/fileinfo")
    public Msg<String> fileinfo(@RequestParam("username") String username, @RequestParam("filepath") String filepath)
    {
        String res = fileService.fileinfo(username,filepath);
        if(res.contains("not found"))
            throw new CustomUnauthorizedException(res);
        else
            return new Msg<String>(HttpStatus.OK.value(),"success",res);
    }

    //重命名接口(目录或文件都可)
    //传入username，filename
    //返回：
    //      成功："success"
    //      失败："path xxx not found"
    @PostMapping("/rename")
    public Msg rename(@RequestParam("username") String username, @RequestParam("oldpath") String oldpath,@RequestParam("newpath") String newpath)
    {
        String res = fileService.rename(username,oldpath,newpath);
        if(res.contains("exist"))
            throw new CustomUnauthorizedException(res);
        else
            return new Msg<String>(HttpStatus.OK.value(),res);
    }

    //日志信息接口(文件)
    //传入username，filepath
    //返回：
    //      成功："success",日志信息：String[]
    //      失败："path xxx not found"
    @PostMapping("/loginfo")
    public Msg<String[]> loginfo(@RequestParam("username") String username,@RequestParam("owner") String owner, @RequestParam("filepath") String filepath)
    {
        String[] res = fileService.loginfo(username,owner,filepath);
        if(res[0].contains("not found"))
            throw new CustomUnauthorizedException(res[0]);
        else
            return new Msg<String[]>(HttpStatus.OK.value(),"success",res);
    }

    //待gfs完成测试
    //文件删除接口，删除后文件从normal文件夹移动到delete文件夹，可以多次删除同名文件，在delete中会维护file，file(1),file(2)...
    //如果重启的gfs，那也请刷新数据库delete。
    //传入username filename
    //返回：
    //      成功："success"
    //      失败："path xxx don't exist"
    @PostMapping("/delete")
    public Msg delete(@RequestParam("username") String username,@RequestParam("filepath") String filepath)
    {
        String res = fileService.delete(username,filepath);
        if(res.compareTo("success") == 0)
            return new Msg(HttpStatus.OK.value(),"success");
        else
            throw new CustomUnauthorizedException(res);

    }

    //待gfs完成测试4
    //文件恢复接口，从delete文件夹恢复到normal文件夹
    //传入username filename
    //返回：
    //      成功："success"
    //           "success: rename to xxx"
    //      失败："file xxx don't exist"
    //            "fail"
    @PostMapping("/recover")
    public Msg recover(@RequestParam("username") String username,@RequestParam("filename") String filename)
    {
        String res = fileService.recover(username,filename);
        if(res.contains("success"))
            return new Msg(HttpStatus.OK.value(),res);
        else
            throw new CustomUnauthorizedException(res);
    }

    //文件读取接口
    //传入 username，owner，filename
    //返回：
    //      成功：success, 文件内容：String[]；
    //            null,文件内容：String[]={""}
    //      失败："file xxx don't exist"
    @PostMapping("/read")
    public Msg<Sheet[]> read(@RequestParam("username") String username, @RequestParam("owner") String owner, @RequestParam("filepath") String filepath)
    {
        String[] res = fileService.read(username,owner,filepath);
        String extra = "success";
        Sheet[] sheet = new Sheet[1];
        sheet[0] = new Sheet();
        sheet[0].name = "sheet1";
        sheet[0].celldata = null;
        if(res[0].compareTo("")==0)
            return new Msg<Sheet[]>(HttpStatus.OK.value(),"null",sheet);
        if(res[0].contains("don't exist"))
            throw new CustomUnauthorizedException(res[0]);
        else {
            sheet[0].initcelldata(res);
            return new Msg<Sheet[]>(HttpStatus.OK.value(),extra,sheet);
        }
    }

    //文件写入接口
    //传入 username，owner，filename，content
    //返回：
    //      成功："success"
    //      失败："file xxx don't exist"
    //            "fail"
    @PostMapping("/write")
    public Msg write(@RequestParam("username") String username,@RequestParam("owner") String owner,@RequestParam("filepath") String filepath,
                        @RequestParam("row") int row,@RequestParam("column") int column,@RequestParam("content") String content)
    {
        String res = fileService.write(username,owner,filepath,row,column,content);
        if(res.contains("don't exist"))
            throw new CustomUnauthorizedException(res);
        else
            return new Msg(HttpStatus.OK.value(),res);
    }

    //单元格锁定接口,同一用户可锁定多个单元格，可以重复锁定一个单元格，其他用户不可锁定已被他人锁定的单元格
    //传入 username，owner，filename，content
    //返回：
    //      成功："success"
    //      失败："file xxx don't exist"
    //            "fail"
    @PostMapping("/trylock")
    public Msg trylock(@RequestParam("username") String username,@RequestParam("owner") String owner,@RequestParam("filepath") String filepath,
                        @RequestParam("row") int row,@RequestParam("column") int column)
    {
        String res = fileService.trylock(username,owner,filepath,row,column);
        if(res.contains("don't exist"))
            throw new CustomUnauthorizedException(res);
        else
            if(res.compareTo("fail") == 0)
                throw new CustomUnauthorizedException(res);
            else
                return new Msg(HttpStatus.OK.value(),res);
    }

    //单元格取消锁定接口
    //传入 username，owner，filename，content
    //返回：
    //      成功："success"
    //      失败："file xxx don't exist"
    //            "no lock"
    @PostMapping("/unlock")
    public Msg unlock(@RequestParam("username") String username,@RequestParam("owner") String owner,@RequestParam("filepath") String filepath,
                          @RequestParam("row") int row,@RequestParam("column") int column)
    {
        String res = fileService.unlock(username,owner,filepath,row,column);
        if(res.contains("don't exist"))
            throw new CustomUnauthorizedException(res);
        else
            if(res.compareTo("no lock")==0)
                throw new CustomUnauthorizedException(res);
            else
                return new Msg(HttpStatus.OK.value(),res);
    }

    //单元格锁定信息获取接口
    //传入 username，owner，filename
    //返回：
    //      成功：success, 锁定信息内容：String[]；
    //            null,锁定信息内容：String[]={""}
    //      失败："file xxx don't exist"
    @PostMapping("/getlockinfo")
    public Msg<String[]> getlockinfo(@RequestParam("username") String username,@RequestParam("owner") String owner,@RequestParam("filepath") String filepath)
    {
        String[] res = fileService.getlockinfo(username,owner,filepath);
        String extra = "success";
        if(res[0].compareTo("")==0)
            extra = "null";
        if(res[0].contains("don't exist"))
            throw new CustomUnauthorizedException(res[0]);
        else
            return new Msg<String[]>(HttpStatus.OK.value(),extra,res);
    }
}
