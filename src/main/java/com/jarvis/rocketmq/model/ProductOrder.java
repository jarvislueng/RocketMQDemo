package com.jarvis.rocketmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class ProductOrder implements Serializable {
    private static final long serialVersionUID = 2371367659765896173L;

    /**
    * 订单id
    * */
    private long orderid;

    /**
     * 订单类型
     * */
    private String type;

    public static List<ProductOrder> getOrderList(){
        List<ProductOrder> list= new ArrayList<>();
        list.add(new ProductOrder(111L, "创建订单"));
        list.add(new ProductOrder(222L, "创建订单"));
        list.add(new ProductOrder(111L, "支付订单"));
        list.add(new ProductOrder(222L, "支付订单"));
        list.add(new ProductOrder(111L, "完成订单"));
        list.add(new ProductOrder(222L, "完成订单"));
        return list;
    }

}
