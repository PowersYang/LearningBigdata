package com.ysir308.groupsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;
    private double price;


    public OrderBean() {
        super();
    }

    public OrderBean(int orderId, double price) {
        super();
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean bean) {

        // 先根据orderId升序，再按照price降序
        int result;
        if (orderId > bean.getOrderId()) {
            result = 1;
        } else if (orderId < bean.getOrderId()) {
            result = -1;

        } else {
            if (price > bean.getPrice()) {
                result = -1;
            } else if (price < bean.getPrice()) {
                result = 1;
            } else {
                result = 0;
            }
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "\t" + orderId + "\t" + price;
    }
}
