package gdoc.repository;

import gdoc.entity.DeleteInfo;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;


public interface DeleteRepository extends MongoRepository<DeleteInfo, String> {
    @Query("{filename: ?0}")
    public List<DeleteInfo> findbyname(String filename);
}
