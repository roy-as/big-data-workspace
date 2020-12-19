package com.as.dao;

import com.as.entity.Student;
import com.as.entity.Teacher;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface TeacherDao {

    List<Teacher> queryAll();

    void insert(Teacher student);

    List<Student> queryPage(Teacher student);
}
