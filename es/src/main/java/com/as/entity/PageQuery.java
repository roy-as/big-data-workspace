package com.as.entity;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class PageQuery {

    private int page = 1;

    private int pageSize = 10;

    public PageQuery(int page, int pageSize) {
        if(page > 0) {
            this.page = page;
        }
        if(pageSize > 0) {
            this.pageSize = pageSize;
        }
    }
}
