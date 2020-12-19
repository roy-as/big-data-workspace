package com.as.entity;

public class Page {

    private Integer page = 1;
    private Integer pageSize = 10;
    private Integer pageStart;


    public int getPage() {
        return page;
    }

    public void setPage(Integer page) {
        if(null != page && page > 0) {
            this.page = page;
        }
        this.pageStart = (this.page - 1) * this.pageSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        if(null != pageSize && pageSize > 0) {
            this.pageSize = pageSize;
        }
        this.pageStart = (this.page - 1) * this.pageSize;
    }

    public Integer getPageStart() {
        return pageStart;
    }

    public void setPageStart(Integer pageStart) {
        this.pageStart = pageStart;
    }
}
