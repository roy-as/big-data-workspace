package com.as.dao;

import com.as.entity.Student;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface StudentDao {

    List<Student> queryAll();

    void insert(Student student);

    List<Student> queryPage(Student student);
}
