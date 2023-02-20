package com.tan.rt.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {

    private String stt;
    private String edt;
    private String source;
    private String keyword;
    private Long keyword_count;
    private Long ts;
}
