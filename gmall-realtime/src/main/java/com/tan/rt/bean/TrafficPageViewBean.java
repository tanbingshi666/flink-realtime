package com.tan.rt.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficPageViewBean {

    private String stt;
    private String edt;
    private String vc;
    private String ch;
    private String ar;
    private String isNew;
    private Long uvCt;
    private Long svCt;
    private Long pvCt;
    private Long durSum;
    private Long ujCt;
    private Long ts;
}