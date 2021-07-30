package Lock.controller;


import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.util.Collections;

@RestController
public class Lock {

    private static final Jedis jedis = new Jedis("localhost", 6379);

    //获取分布式redis锁
    //传入参数：lockKey 锁
    //         username 请求人
    //         expireTime 超期时间,单位为 ms
    //返回值：是否成功
    @PostMapping("/tryredislock")
    public static boolean tryredislock(@RequestParam("key") String lockKey, @RequestParam("username") String username, @RequestParam("expireTime") int expireTime) {

        String result = jedis.set(lockKey, username, "NX", "PX", expireTime);

        if (result.equals("OK")) {
            return true;
        }

        return false;

    }

    //释放分布式redis锁
    //传入参数：lockKey 锁
    //         username 请求人
    //         expireTime 超期时间,单位为 ms
    //返回值：是否成功
    @PostMapping("/releaseredislock")
    public static boolean releaseredislock(@RequestParam("key") String lockKey,@RequestParam("username") String requestId) {

        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));

        if (result.equals(1L)) {
            return true;
        }

        return false;
    }

}
