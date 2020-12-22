package com.as.service.impl;

import com.as.Job;
import com.as.dao.JobDao;
import com.as.entity.PageQuery;
import com.as.service.JobService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class JobServiceImpl implements JobService {

    @Autowired
    private ElasticsearchRestTemplate restTemplate;

    @Autowired
    private JobDao jobDao;

    @PostConstruct
    public void init() {
        boolean exists = restTemplate.indexExists(Job.class);
        if (!exists) {
            this.restTemplate.createIndex(Job.class);
            this.restTemplate.putMapping(Job.class);
        }
    }

    @Override
    @Transactional
    public void save(Job job) {
        jobDao.save(job);
        throw new RuntimeException("测试一波");
    }

    @Override
    public void saveBatch(List<Job> jobs) {
        jobDao.saveAll(jobs);
    }

    @Override
    public void update(Job job) {
        if (StringUtils.isNotBlank(job.getId())) {
            jobDao.save(job);
        }
    }

    @Override
    public void updateBatch(List<Job> jobs) {
        jobs = jobs.stream().filter(job -> StringUtils.isNotBlank(job.getId())).collect(Collectors.toList());
        jobDao.saveAll(jobs);
    }

    @Override
    public void delete(String id) {
        jobDao.deleteById(id);
    }

    @Override
    public void deleteBatch(List<Job> jobs) {
        jobs = jobs.stream().filter(job -> StringUtils.isNotBlank(job.getId())).collect(Collectors.toList());
        jobDao.deleteAll(jobs);
    }

    @Override
    public Job findById(String id) {
        return jobDao.findById(id).orElse(null);
    }

    @Override
    public Page<Job> pageQuery(Job job, PageQuery pageQuery) {
        Criteria criteria = new Criteria();
        CriteriaQuery query = new CriteriaQuery(criteria);
        if (StringUtils.isNotBlank(job.getArea())) {
            criteria.and(new Criteria("area").is(job.getArea()));
        }

        if (StringUtils.isNotBlank(job.getCompany())) {
            criteria.and(new Criteria("cmp").is(job.getCompany()));
        }

        if (StringUtils.isNotBlank(job.getEdu())) {
            criteria.and(new Criteria("edu").is(job.getEdu()));
        }

        if (StringUtils.isNotBlank(job.getJd())) {
            criteria.and(new Criteria("area").is(job.getArea()));
        }

        if (StringUtils.isNotBlank(job.getJd())) {
            criteria.and(new Criteria("jd").is(job.getJd()));
        }

        if (StringUtils.isNotBlank(job.getJobType())) {
            criteria.and(new Criteria("job_type").is(job.getJobType()));
        }
        query.setPageable(PageRequest.of(pageQuery.getPage() -1 , pageQuery.getPageSize()));

         return restTemplate.queryForPage(query, Job.class);
    }

}
