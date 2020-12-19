package com.as;

import com.as.service.TestDynamicService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestDynamic {

    @Autowired
    private TestDynamicService service;

    @Test
    public void test1() {
        service.insertStudent1();
    }

    @Test
    public void test2() {
        service.insertStudent2();
    }

    @Test
    public void test3() {
        service.insertTeacher1();
    }

    @Test
    public void test4() {
        service.insertStudent3();
    }

    @Test
    public void test5() {
        service.insertTeacher2();
    }

    @Test
    public void test6() {
        service.pagination();
    }




}
