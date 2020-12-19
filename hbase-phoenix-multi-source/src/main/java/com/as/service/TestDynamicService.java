package com.as.service;

import com.as.dao.StudentDao;
import com.as.dao.TeacherDao;
import com.as.datasource.DatasourceNames;
import com.as.datasource.annotation.Datasource;
import com.as.entity.Student;
import com.as.entity.Teacher;
import org.junit.Test;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

@Service
public class TestDynamicService {

    @Resource
    private StudentDao studentDao;

    @Resource
    private TeacherDao teacherDao;

    @Resource
    private TestDynamicService testDynamicService;


    @Datasource
    @Transactional
    public void insertStudent1() {
        Student student = new Student("rk002", "wayne", 33);
        studentDao.insert(student);
    }

    @Datasource(DatasourceNames.FIRST)
    @Transactional
    public void insertStudent2() {
        Student student = new Student("rk003", "wayne", 33);
        studentDao.insert(student);
    }

    @Datasource(DatasourceNames.FIRST)
    @Transactional
    public void insertStudent3() {
        Student student = new Student("rk004", "wayne", 33);
        studentDao.insert(student);
        throw new RuntimeException("测试事务");
    }

    @Datasource(DatasourceNames.SECOND)
    @Transactional
    public void insertTeacher1() {
        Teacher teacher = new Teacher("rk003", "wayne", 33);
        teacherDao.insert(teacher);
    }

    @Datasource(DatasourceNames.SECOND)
    @Transactional
    public void insertTeacher2() {
        Teacher teacher = new Teacher("rk004", "roy", 33);
        teacherDao.insert(teacher);
        throw new RuntimeException("测试事务");
    }

    public void pagination() {
        testDynamicService.insertTeacher3();
        testDynamicService.insertStudent4();
    }

    @Datasource(DatasourceNames.SECOND)
    @Transactional
    public void insertTeacher3() {
        Teacher teacher = new Teacher("rk005", "roy1", 33);
        teacherDao.insert(teacher);
    }

    @Datasource(DatasourceNames.FIRST)
    @Transactional
    public void insertStudent4() {
        Student student = new Student("rk005", "wayne1", 33);
        studentDao.insert(student);
        throw new RuntimeException("测试事务传播");
    }
}
