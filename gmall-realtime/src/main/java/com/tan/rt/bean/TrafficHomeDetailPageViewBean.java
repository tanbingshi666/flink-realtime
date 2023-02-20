package com.tan.rt.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    String stt;
    String edt;
    Long homeUvCt;
    Long goodDetailUvCt;
    Long ts;
}