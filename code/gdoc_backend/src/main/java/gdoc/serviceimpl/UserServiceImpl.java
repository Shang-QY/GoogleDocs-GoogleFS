package gdoc.serviceimpl;

import gdoc.entity.User;
import gdoc.repository.UserRepository;
import gdoc.service.FileService;
import gdoc.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class UserServiceImpl implements UserService {

    @Autowired
    UserRepository userRepository;

    @Autowired
    FileService fileService;

    @Override
    public User register(User user)
    {

        if(userRepository.findbyname(user.getUsername()).isEmpty())
        {
            userRepository.save(user);
            fileService.mkdir(user.getUsername(),"/"+user.getUsername());
            return user;
        }
        else
            return null;
    }

    @Override
    public User login(String username,String password){
        List<User> userlist = userRepository.findbyname(username);
        if(userlist.isEmpty())
        {
            return null;
        }
        else
        {
            User user = userlist.get(0);
            if(user.getPassword().compareTo(password)==0)
                return user;
            else
                return null;
        }

    }
}
