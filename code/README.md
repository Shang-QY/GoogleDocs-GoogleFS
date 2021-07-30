## Naive gDocs

#### awesomeGFS：使用go语言编写的，通过云服务器部署的zookeeper搭建的DFS。

#### gdoc_frontend：使用react编写的gdoc前端，提供登录注册，文件目录浏览，回收站浏览，表格文件展示，实时协同编辑等功能。

#### gdoc_backend：使用java spring boot 编写的gdoc后端，实现有文件操作和用户相关操作，具体参数依据后端controller层注释说明，包含websocket server

      1. 文件创建接口
      2. 文件删除接口
      3. 文件恢复接口
      4. 文件读取接口
      5. 文件写入接口
      6. 单元格锁定接口
      7. 单元格取消锁定接口
      8. 单元格锁定信息获取接口
      9. 用户登录接口
      10. 用户注册接口
      11. 日志信息获取接口
      12. 重命名接口
      13. 文件信息获取接口
      14. 目录信息获取接口
      15. 目录创建接口

#### gdoc_lockend：使用java spring boot 编写的基于redis的锁服务器，用于分布式后端使用，以解决不可重入的函数竞争问题。运行在 localhost:8081端口，具有两个外部功能：


1. 获取分布式redis锁：http://localhost:8081/tryredislock?username=user1&key=write&expireTime=5000

```
//获取分布式redis锁
//传入参数：lockKey 锁
//         username 请求人
//         expireTime 超期时间,单位为 ms
//返回值：是否成功
```

2. 释放分布式redis锁：http://localhost:8081/releaseredislock?username=user2&key=write

```
//释放分布式redis锁
//传入参数：lockKey 锁
//         username 请求人
//         expireTime 超期时间,单位为 ms
//返回值：是否成功
```
