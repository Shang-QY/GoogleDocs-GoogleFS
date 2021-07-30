package gdoc.service;


import gdoc.entity.User;

public interface UserService {

    public User register(User user);

    public User login(String username,String password);
}
