package gdoc.controller;

import gdoc.entity.User;
import gdoc.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import gdoc.utils.message.*;
import gdoc.utils.exception.*;
import org.springframework.http.HttpStatus;

@RestController
public class UserController {

    @Autowired
    UserService userService;

    //注册接口，
    //参数：传入json类型的user结构体 ：{"username":"user4","password":"user4","phone":"123456789"}
    //                    可以缺省 ：{"username":"user4","password":"user4"}
    //返回值：
    //      成功：返回 user 的 id
    //      失败："该用户已存在"
    @PostMapping("/register")
    public Msg<User> register(@RequestBody User user)
    {
        User res_user = userService.register(user);
        if(res_user != null){
            return new Msg<User>(HttpStatus.OK.value(), "注册成功(Register Success.)",res_user);
        }
        else {
            throw new CustomUnauthorizedException("该帐号已存在(Account exist.)");
        }
    }

    //登录接口
    //参数：传入(String)username和(String)password
    //返回值：
    //      成功："登录成功"
    //      失败：
    //          "登录失败，用户不存在"
    //          "登录失败，密码错误"
    @PostMapping("/login")
    public Msg<User> login(@RequestParam("username") String username,@RequestParam("password") String password)
    {
        User res_user = userService.login(username,password);
        if(res_user != null){
            return new Msg<User>(HttpStatus.OK.value(), "登陆成功(Login Success.)",res_user);
        }
        else {
            throw new CustomUnauthorizedException("帐号或密码错误(Account or Password Error.)");
        }
    }

}
