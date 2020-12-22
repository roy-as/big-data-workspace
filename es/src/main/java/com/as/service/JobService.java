package com.as.service;

import com.as.Job;
import com.as.entity.PageQuery;
import org.springframework.data.domain.Page;

import java.util.List;

public interface JobService {

    void save(Job job);

    void saveBatch(List<Job> jobs);

    void update(Job job);

    void updateBatch(List<Job> jobs);

    void delete(String id);

    void deleteBatch(List<Job> jobs);

    Job findById(String id);

    Page<Job> pageQuery(Job job, PageQuery pageQuery);
}
