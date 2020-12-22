package com.as;

import com.as.dao.JobDao;
import com.as.entity.PageQuery;
import com.as.service.JobService;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class ElasticSearchTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobDao jobDao;

    @Test
    public void save(){
        Job job = new Job();
        job.setId("4");
        job.setArea("四川-广安");
        job.setCompany("华为有限公司");
        job.setEdu("本科以上");
        job.setExp("3年以上");
        job.setSalary("12k-25k");
        job.setJobType("全职");
        job.setPv("10000人浏览");
        job.setTitle("java开发工程师");
        jobService.save(job);
    }

    @Test
    public void saveBatch(){
        List<Job> jobs = new ArrayList<>();
        Job job1 = new Job();
        job1.setId("2");
        job1.setArea("四川-成都");
        job1.setCompany("小米科技");
        job1.setEdu("本科以上");
        job1.setExp("1年以上");
        job1.setSalary("12k-20k");
        job1.setJobType("全职");
        job1.setPv("500人浏览");
        job1.setTitle("后台开发");
        job1.setJd("岗位职责：1、负责设计、开发、应用B/S或C/S基础架构软件，以及云计算平台架构。2、负责应用架构、关键代码的程序编写。3、在项目中能够承担软件团队的技术负责人，评估分析和设计方案，对系统的重用、扩展、安全、性能、伸缩性、简洁等做系统级的把握，确保系统设计的质量；4、理解系统的业务需求，创建合理、完善的应用系统体系架构，特别是在项目的整体系统架构方面");
        jobs.add(job1);

        Job job2 = new Job();
        job2.setId("3");
        job2.setArea("重庆-渝北区");
        job2.setCompany("腾讯");
        job2.setEdu("专科以上");
        job2.setExp("2年以上");
        job2.setSalary("15k-30k");
        job2.setJobType("全职");
        job2.setPv("5000人浏览");
        job2.setTitle("后台开发");
        job2.setJd("岗位职责：1.负责产品研发和维护； 2.熟悉Java主流框架的应用，并用其进行项目的开发； 3.熟悉面向对象设计方法，掌握MVC和常用设计模式；任职要求：统招985/211本科及以上学历，计算机或相关专业，5年以上工作经验；具备扎实的JAVA基础，具有良好的编码习惯；理解JAVAWEB技术体系架构，熟练掌握Servlet/JSP技术；熟练掌握Struts2、Spring、Hibernate三大框架，深刻理解Struts2的核心流程；熟练使用ssm框架");
        jobs.add(job2);
        jobService.saveBatch(jobs);
    }

    @Test
    public void update(){
        Job job = new Job();
        job.setId("1");
        job.setArea("四川-广安");
        job.setCompany("华为有限公司");
        job.setEdu("本科以上");
        job.setExp("3年以上");
        job.setSalary("12k-25k");
        job.setJobType("全职");
        job.setPv("10000人浏览");
        job.setTitle("java开发工程师");
        job.setJd("工作描述：1. 接口设计与开发，能够使用缓存与队列，解决接口访问并发问题2. 应用系统设计与开发，查询报表制作。3. PHP程序设计与开发4. 功能模块开发，代码编写5. 部分需求分析与开发文档撰写6. 与代码质量保证与测试7. 与测试部门紧密配合，修改BUG任职要求：1. 统招本科以上学历，三年以上工作经验，熟悉php+mysql开发或java+db，能够独立分析设计系统，了解至少一种js语言库，例如jquery。2. 熟悉一种队列处理机制，能够用队列做应用。3. 能够分析并优化慢查询sql，数据库简单管理");
        jobService.save(job);
    }

    @Test
    public void updateBatch(){
        List<Job> jobs = new ArrayList<>();
        Job job1 = new Job();
        job1.setId("2");
        job1.setArea("北京-昌平区");
        jobs.add(job1);
        jobService.updateBatch(jobs);
    }

    @Test
    public void delete(){
        jobService.delete("2");
    }

    @Test
    public void deleteBatch(){
        List<Job> jobs = new ArrayList<>();
        Job job1 = new Job();
        job1.setId("1");
        jobs.add(job1);
        Job job2 = new Job();
        job2.setId("3");
        jobs.add(job2);
        jobService.deleteBatch(jobs);
    }

    @Test
    public void findById() {
        Job job = jobService.findById("2");
        System.out.println(job);
    }

    @Test
    public void findAllByArea(){
        List<Job> jobs = jobDao.findAllByArea("四川");
        System.out.println(jobs);
        List<Job> job = jobDao.findAllByAreaAndTitle("四川", "java");
        System.out.println(job);
    }

    @Test
    public void query() {
        MatchQueryBuilder builder = QueryBuilders.matchQuery("area", "四川");
        Iterable<Job> jobs = jobDao.search(builder);
        System.out.println(jobs);

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("开发", "title", "jd");
        Iterable<Job> multiJobs = jobDao.search(multiMatchQueryBuilder);
        System.out.println(multiJobs);
    }

    @Test
    public void pageQuery() {
        Job job = new Job();
        job.setArea("四川");
        job.setJd("开发");

        Page<Job> jobs = jobService.pageQuery(job, new PageQuery(1, 10));
        System.out.println(jobs);
    }

}
