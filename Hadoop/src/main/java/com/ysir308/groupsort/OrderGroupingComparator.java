package com.ysir308.groupsort;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    public OrderGroupingComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        // 只要id相同，就认为是相同的key，使其进入同一个reduce
        int result;

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        if (aBean.getOrderId() == bBean.getOrderId()){
            result = 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }

        // 只要result为0，就会返回true，会将相同id的对象传给同一个reduce处理

        return result;
    }
}