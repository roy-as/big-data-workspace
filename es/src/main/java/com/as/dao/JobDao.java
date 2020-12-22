package com.as.dao;

import com.as.Job;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobDao extends ElasticsearchRepository<Job, String> {

    List<Job> findAllByArea(String area);

    List<Job> findAllByAreaAndTitle(String area, String title);

}
