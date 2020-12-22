package com.as.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "job_index")
@Data
public class Job {

    @Id
    private String id;

    @Field(name = "area", type = FieldType.Text, store = true, analyzer = "ik_max_word")
    private String area;

    @Field(name = "exp", type = FieldType.Text, store = true, analyzer = "ik_max_word")
    private String exp;

    @Field(name = "edu", type = FieldType.Keyword, store = true)
    private String edu;

    @Field(name = "salary", type = FieldType.Keyword, store = true)
    private String salary;

    @Field(name = "job_type", type = FieldType.Keyword, store = true)
    private String jobType;

    @Field(name = "cmp", type = FieldType.Text, store = true, analyzer = "ik_max_word")
    private String company;

    @Field(name = "pv", type = FieldType.Keyword, store = true)
    private String pv;

    @Field(name = "title", type = FieldType.Text, store = true, analyzer = "ik_max_word")
    private String title;

    @Field(name = "jd", type = FieldType.Text, store = true, analyzer = "ik_max_word")
    private String jd;


}
