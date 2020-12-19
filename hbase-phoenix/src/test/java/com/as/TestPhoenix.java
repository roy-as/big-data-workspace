package com.as;

import com.as.dao.StudentDao;
import com.as.entity.Student;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestPhoenix {

    @Resource
    private StudentDao studentDao;

    @Test
    public void test3() {
        List<Student> tests = studentDao.queryAll();
        System.out.println(tests);
    }

    @Test
    @Transactional(rollbackFor = Exception.class)
    @Rollback(false)
    public void test1() {
        Student student1 = new Student("12", "harper",28);
        Student student2 = new Student("13", "cloud",20);
        Student student3 = new Student("14", "mac",30);
        Student student4 = new Student("15", "rafe",25);
        Student student5 = new Student("16", "jerry",35);
        studentDao.insert(student1);
        studentDao.insert(student2);
        studentDao.insert(student3);
        studentDao.insert(student4);
        studentDao.insert(student5);
       // throw new RuntimeException("测试事务");

    }

    @Test
    public void test4() {
        Student student = new Student();
        student.setAge(20);
        //student.setName("roy");
        student.setPage(1);
        student.setPageSize(2);
        List<Student> students = studentDao.queryPage(student);
        System.out.println(students);
    }
}
